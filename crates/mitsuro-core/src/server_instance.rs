//! Server instance detection and management
//!
//! Tracks running Mitsuro server instances via a PID file at ~/.mitsuro/server.pid.
//! Enables the desktop app and CLI to detect and reuse an existing server.

use anyhow::{bail, Context, Result};
use serde::Deserialize;
use std::fs::{File, OpenOptions};
use std::io::Write as _;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::paths;

pub const HEALTH_IDENTITY: &str = "mitsuro-server";

static PID_FILE_NONCE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerInstance {
    pub pid: u32,
    pub port: u16,
}

/// Process-lifetime authority for publishing the canonical server instance.
///
/// The advisory lock prevents two modern Mitsuro processes from both deciding
/// that they own the canonical PID record. Older releases do not hold this
/// lock, so callers must still probe [`detect_running_server`] before
/// acquiring it.
#[derive(Debug)]
pub struct ServerInstanceGuard {
    instance: ServerInstance,
    pid_path: PathBuf,
    lock_file: File,
}

impl ServerInstanceGuard {
    /// Acquire the canonical server singleton lock and atomically publish this
    /// process as its owner.
    pub fn acquire(port: u16) -> Result<Self> {
        Self::acquire_at(pid_file_path(), port)
    }

    fn acquire_at(pid_path: PathBuf, port: u16) -> Result<Self> {
        ensure_private_parent(&pid_path)?;
        let lock_path = pid_path.with_extension("pid.lock");
        let lock_file = open_private_lock_file(&lock_path)?;
        fs2::FileExt::try_lock_exclusive(&lock_file).with_context(|| {
            format!(
                "Another Mitsuro server process owns the singleton lock {}",
                lock_path.display()
            )
        })?;

        match read_pid_file_from(&pid_path, true) {
            Some(existing) if existing.pid != std::process::id() || existing.port != port => {
                let _ = fs2::FileExt::unlock(&lock_file);
                bail!(
                    "Mitsuro server PID record is still owned by process {} on port {}",
                    existing.pid,
                    existing.port
                );
            }
            _ => {}
        }

        let instance = ServerInstance {
            pid: std::process::id(),
            port,
        };
        write_pid_file_at(&pid_path, &instance)?;

        Ok(Self {
            instance,
            pid_path,
            lock_file,
        })
    }

    pub fn instance(&self) -> &ServerInstance {
        &self.instance
    }
}

impl Drop for ServerInstanceGuard {
    fn drop(&mut self) {
        let _ = remove_pid_file_if_matches_at(&self.pid_path, &self.instance);
        let _ = fs2::FileExt::unlock(&self.lock_file);
    }
}

#[derive(Debug, Deserialize)]
struct HealthIdentity {
    status: String,
    identity: String,
    pid: u32,
}

fn pid_file_path() -> PathBuf {
    paths::config_dir().join("server.pid")
}

/// Write PID file when server starts.
pub fn write_pid_file(port: u16) -> Result<()> {
    let path = pid_file_path();
    ensure_private_parent(&path)?;
    write_pid_file_at(
        &path,
        &ServerInstance {
            pid: std::process::id(),
            port,
        },
    )
}

fn ensure_private_parent(path: &std::path::Path) -> Result<()> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    std::fs::create_dir_all(parent)
        .with_context(|| format!("Failed to create {}", parent.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700))
            .with_context(|| format!("Failed to secure {}", parent.display()))?;
    }
    Ok(())
}

fn open_private_lock_file(path: &std::path::Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.create(true).read(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options
            .mode(0o600)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
    }
    let file = options
        .open(path)
        .with_context(|| format!("Failed to open server lock {}", path.display()))?;
    if !file.metadata()?.is_file() {
        bail!("Server lock is not a regular file: {}", path.display());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))
            .with_context(|| format!("Failed to secure server lock {}", path.display()))?;
    }
    Ok(file)
}

fn singleton_lock_is_held_at(pid_path: &std::path::Path) -> bool {
    let lock_path = pid_path.with_extension("pid.lock");
    let mut options = OpenOptions::new();
    options.read(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
    }
    let Ok(lock_file) = options.open(lock_path) else {
        return false;
    };
    match fs2::FileExt::try_lock_exclusive(&lock_file) {
        Ok(()) => {
            let _ = fs2::FileExt::unlock(&lock_file);
            false
        }
        Err(error) => error.kind() == std::io::ErrorKind::WouldBlock,
    }
}

fn write_pid_file_at(path: &std::path::Path, instance: &ServerInstance) -> Result<()> {
    let nonce = PID_FILE_NONCE.fetch_add(1, Ordering::Relaxed);
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("server.pid");
    let temporary =
        path.with_file_name(format!(".{file_name}.{}.{}.tmp", std::process::id(), nonce));

    let mut options = OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options
            .mode(0o600)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
    }

    let publish = (|| -> Result<()> {
        let mut file = options
            .open(&temporary)
            .with_context(|| format!("Failed to stage server PID file {}", temporary.display()))?;
        write!(file, "{}:{}", instance.pid, instance.port)
            .context("Failed to write staged server PID file")?;
        file.sync_all()
            .context("Failed to sync staged server PID file")?;
        std::fs::rename(&temporary, path).with_context(|| {
            format!(
                "Failed to atomically publish server PID file {}",
                path.display()
            )
        })?;
        Ok(())
    })();

    if publish.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    publish
}

/// Remove a PID file on shutdown only when it still belongs to this process.
/// New code that knows the port should prefer [`remove_pid_file_if_matches`].
pub fn remove_pid_file() {
    let path = pid_file_path();
    let Some(instance) = read_pid_file_from(&path, false) else {
        return;
    };
    if instance.pid == std::process::id() {
        let _ = remove_pid_file_if_matches_at(&path, &instance);
    }
}

/// Remove the PID file only if it still contains the exact expected PID and
/// port. This prevents a stale health probe from unlinking a replacement
/// server's authority record.
pub fn remove_pid_file_if_matches(expected_pid: u32, expected_port: u16) -> bool {
    remove_pid_file_if_matches_at(
        &pid_file_path(),
        &ServerInstance {
            pid: expected_pid,
            port: expected_port,
        },
    )
}

/// Read PID file and check if the process is still alive.
pub fn read_pid_file() -> Option<ServerInstance> {
    let path = pid_file_path();
    read_pid_file_from(&path, true)
}

fn read_pid_file_from(path: &std::path::Path, clean_stale: bool) -> Option<ServerInstance> {
    let content = std::fs::read_to_string(path).ok()?;
    let instance = parse_pid_file(&content)?;

    // Check if process is alive (Unix: kill -0)
    if !is_process_alive(instance.pid) {
        if clean_stale {
            let _ = remove_pid_file_if_matches_at(path, &instance);
        }
        return None;
    }

    Some(instance)
}

fn parse_pid_file(content: &str) -> Option<ServerInstance> {
    let (pid, port) = content.trim().split_once(':')?;
    if port.contains(':') {
        return None;
    }
    Some(ServerInstance {
        pid: pid.parse().ok()?,
        port: port.parse().ok()?,
    })
}

fn remove_pid_file_if_matches_at(path: &std::path::Path, expected: &ServerInstance) -> bool {
    let Ok(content) = std::fs::read_to_string(path) else {
        return false;
    };
    if parse_pid_file(&content).as_ref() != Some(expected) {
        return false;
    }
    std::fs::remove_file(path).is_ok()
}

/// Check whether this process hosts the canonical Mitsuro server on `port`.
/// This is used while an embedded server is starting.
pub async fn probe_health(port: u16) -> bool {
    probe_health_for_instance(&ServerInstance {
        pid: std::process::id(),
        port,
    })
    .await
}

async fn probe_health_for_instance(instance: &ServerInstance) -> bool {
    let url = format!("http://127.0.0.1:{}/health", instance.port);
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(2))
        .build();

    let client = match client {
        Ok(c) => c,
        Err(_) => return false,
    };

    let Ok(response) = client.get(&url).send().await else {
        return false;
    };
    if !response.status().is_success() {
        return false;
    }
    response
        .json::<HealthIdentity>()
        .await
        .is_ok_and(|health| health_identity_matches(instance, &health))
}

fn health_identity_matches(instance: &ServerInstance, health: &HealthIdentity) -> bool {
    health.status == "ok" && health.identity == HEALTH_IDENTITY && health.pid == instance.pid
}

/// Detect a running Mitsuro server instance.
/// Returns the instance info if a healthy server is found.
pub async fn detect_running_server() -> Option<ServerInstance> {
    let pid_path = pid_file_path();
    let instance = read_pid_file()?;

    if !process_can_host_canonical_server(instance.pid) {
        let _ = remove_pid_file_if_matches(instance.pid, instance.port);
        return None;
    }

    if probe_health_for_instance(&instance).await {
        Some(instance)
    } else {
        // A modern server publishes its PID before expensive application-state
        // initialization so supervisors know which process is starting. Keep
        // that record while its process-lifetime singleton lock is held; a
        // later poll can validate health once the listener begins serving.
        // Legacy/unlocked records still get cleaned when their health proof no
        // longer matches.
        if !singleton_lock_is_held_at(&pid_path) {
            let _ = remove_pid_file_if_matches(instance.pid, instance.port);
        }
        None
    }
}

/// Whether the live process is an executable allowed to own the canonical
/// Mitsuro server record.
///
/// Linux callers use this before trusting an otherwise well-formed PID file,
/// so PID reuse by an unrelated process cannot turn a stale record into server
/// authority. Other platforms still require the exact PID and canonical
/// identity from `/health`, but cannot prove the executable through `std`.
#[cfg(target_os = "linux")]
pub fn process_can_host_canonical_server(pid: u32) -> bool {
    let path = PathBuf::from(format!("/proc/{pid}/exe"));
    std::fs::read_link(path)
        .map(strip_deleted_suffix)
        .is_ok_and(|path| is_canonical_server_host_executable(&path))
}

#[cfg(not(target_os = "linux"))]
pub fn process_can_host_canonical_server(_pid: u32) -> bool {
    // The health response still has to prove canonical identity and the exact
    // PID. Linux additionally provides a trustworthy executable path via
    // procfs; other platforms do not expose an equivalent through std.
    true
}

#[cfg(target_os = "linux")]
fn strip_deleted_suffix(path: PathBuf) -> PathBuf {
    path.to_string_lossy()
        .strip_suffix(" (deleted)")
        .map_or(path.clone(), PathBuf::from)
}

fn is_canonical_server_host_executable(path: &std::path::Path) -> bool {
    if path
        .components()
        .any(|component| component.as_os_str() == std::ffi::OsStr::new(".mitsuro-releases"))
    {
        return true;
    }
    matches!(
        path.file_name().and_then(|name| name.to_str()),
        Some(
            "mitsuro"
                | "mitsuro.exe"
                | "mitsuro-server"
                | "mitsuro-server.exe"
                | "mitsuro-desktop"
                | "mitsuro-desktop.exe"
                | "Mitsuro"
                | "Mitsuro.exe"
        )
    )
}

#[cfg(unix)]
fn is_process_alive(pid: u32) -> bool {
    if pid > i32::MAX as u32 {
        return false;
    }
    // SAFETY: kill(pid, 0) with signal 0 only checks process existence
    // without sending a signal. The pid is guarded to fit in i32.
    unsafe { libc::kill(pid as i32, 0) == 0 }
}

#[cfg(not(unix))]
fn is_process_alive(_pid: u32) -> bool {
    // On non-Unix, assume alive and let health check determine
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pid_file_parser_requires_exact_pid_and_port_shape() {
        assert_eq!(
            parse_pid_file("123:4567"),
            Some(ServerInstance {
                pid: 123,
                port: 4567
            })
        );
        for invalid in ["", "123", "123:4567:extra", "pid:4567", "123:70000"] {
            assert_eq!(parse_pid_file(invalid), None, "{invalid}");
        }
    }

    #[test]
    fn conditional_cleanup_never_removes_a_replacement_pid_file() {
        let temp = tempfile::tempdir().expect("temp dir");
        let path = temp.path().join("server.pid");
        std::fs::write(&path, "200:4000").expect("replacement PID file");
        assert!(!remove_pid_file_if_matches_at(
            &path,
            &ServerInstance {
                pid: 100,
                port: 3000,
            }
        ));
        assert_eq!(
            std::fs::read_to_string(&path).expect("replacement remains"),
            "200:4000"
        );
        assert!(remove_pid_file_if_matches_at(
            &path,
            &ServerInstance {
                pid: 200,
                port: 4000,
            }
        ));
        assert!(!path.exists());
    }

    #[cfg(unix)]
    #[test]
    fn stale_pid_record_is_removed_before_a_new_server_is_considered() {
        let temp = tempfile::tempdir().expect("temp dir");
        let path = temp.path().join("server.pid");
        std::fs::write(&path, format!("{}:3000", u32::MAX)).expect("stale PID file");

        assert_eq!(read_pid_file_from(&path, true), None);
        assert!(!path.exists(), "the exact stale authority must be removed");
    }

    #[test]
    fn singleton_guard_blocks_a_second_owner_and_cleans_its_record() {
        let temp = tempfile::tempdir().expect("temp dir");
        let path = temp.path().join("server.pid");
        let first = ServerInstanceGuard::acquire_at(path.clone(), 3000)
            .expect("first process acquires singleton authority");
        assert!(singleton_lock_is_held_at(&path));

        let error = ServerInstanceGuard::acquire_at(path.clone(), 3000)
            .expect_err("second owner must be refused");
        assert!(error.to_string().contains("singleton lock"));
        assert_eq!(
            std::fs::read_to_string(&path).expect("published PID record"),
            format!("{}:3000", std::process::id())
        );

        drop(first);
        assert!(!singleton_lock_is_held_at(&path));
        assert!(!path.exists());
    }

    #[cfg(unix)]
    #[test]
    fn pid_and_lock_files_are_private() {
        use std::os::unix::fs::PermissionsExt as _;

        let temp = tempfile::tempdir().expect("temp dir");
        let path = temp.path().join("private").join("server.pid");
        let guard =
            ServerInstanceGuard::acquire_at(path.clone(), 3000).expect("singleton authority");

        assert_eq!(
            std::fs::metadata(&path)
                .expect("PID metadata")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        assert_eq!(
            std::fs::metadata(path.with_extension("pid.lock"))
                .expect("lock metadata")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        assert_eq!(
            std::fs::metadata(path.parent().expect("parent"))
                .expect("parent metadata")
                .permissions()
                .mode()
                & 0o777,
            0o700
        );

        drop(guard);
    }

    #[test]
    fn health_identity_requires_canonical_role_and_exact_pid() {
        let expected = ServerInstance {
            pid: 123,
            port: 3000,
        };
        assert!(health_identity_matches(
            &expected,
            &HealthIdentity {
                status: "ok".to_string(),
                identity: HEALTH_IDENTITY.to_string(),
                pid: 123,
            }
        ));
        assert!(!health_identity_matches(
            &expected,
            &HealthIdentity {
                status: "ok".to_string(),
                identity: "krusty-server".to_string(),
                pid: 123,
            }
        ));
        assert!(!health_identity_matches(
            &expected,
            &HealthIdentity {
                status: "ok".to_string(),
                identity: HEALTH_IDENTITY.to_string(),
                pid: 999,
            }
        ));
        assert!(!health_identity_matches(
            &expected,
            &HealthIdentity {
                status: "starting".to_string(),
                identity: HEALTH_IDENTITY.to_string(),
                pid: 123,
            }
        ));
    }

    #[test]
    fn executable_check_accepts_only_canonical_server_hosts() {
        for accepted in [
            "/opt/mitsuro",
            "/opt/mitsuro-server",
            "/opt/mitsuro-desktop",
            "/srv/.mitsuro-releases/0.9.20/arbitrary-name",
        ] {
            assert!(is_canonical_server_host_executable(std::path::Path::new(
                accepted
            )));
        }
        for rejected in [
            "/opt/krusty",
            "/opt/krusty-mako",
            "/opt/mitsuro-helper",
            "/srv/not-.mitsuro-releases/arbitrary-name",
        ] {
            assert!(!is_canonical_server_host_executable(std::path::Path::new(
                rejected
            )));
        }
    }
}

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpStream};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};

use anyhow::{bail, Context as _, Result};
use mitsuro_client::{HealthResponse, MitsuroClient};
use mitsuro_core::server_instance::{
    detect_running_server, process_can_host_canonical_server, read_pid_file,
    remove_pid_file_if_matches, ServerInstance, HEALTH_IDENTITY,
};
use tokio::process::{Child, Command};
use tokio::time::sleep;

const STARTUP_TIMEOUT: Duration = Duration::from_secs(20);
const HEALTH_INTERVAL: Duration = Duration::from_millis(250);
const SYSTEMD_UNIT: &str = "mitsuro-serve.service";
const SUPPORTED_API_PROTOCOL: u16 = 1;
const CANONICAL_SERVER_PORT: u16 = 3000;

pub const REQUIRED_CAPABILITIES: [(&str, u16); 4] = [
    ("desktop_canvases", 1),
    ("terminal_sessions", 2),
    ("browser_sessions", 1),
    ("agent_streams", 1),
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BootstrapOutcome {
    Reused,
    StartedSystemService,
    SpawnedCompanion,
}

#[derive(Clone, Debug)]
pub struct ServerConnection {
    pub client: MitsuroClient,
    pub health: HealthResponse,
    pub pid: u32,
    pub port: u16,
    pub outcome: BootstrapOutcome,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BootstrapFailureKind {
    Incompatible,
    Occupied,
    ServiceFailed,
    CompanionMissing,
    CompanionFailed,
}

#[derive(Debug)]
pub struct BootstrapFailure {
    pub kind: BootstrapFailureKind,
    pub message: String,
    pub diagnostics: Option<String>,
}

impl std::fmt::Display for BootstrapFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for BootstrapFailure {}

pub struct ServerSupervisor;

impl ServerSupervisor {
    pub async fn connect() -> std::result::Result<ServerConnection, BootstrapFailure> {
        // Prefer the already-running canonical Mitsuro. A PID record is the
        // fast path, but a healthy `/health` identity on the canonical port is
        // enough to reuse Honey, systemd, or another desktop-managed server
        // even when the record is missing or stale. Only start a server when
        // nothing is listening.
        if let Some(instance) = trusted_recorded_instance() {
            return wait_for_server(BootstrapOutcome::Reused, None, Some(instance)).await;
        }
        match discover_healthy_listener(CANONICAL_SERVER_PORT).await {
            Ok(Some(instance)) => {
                return wait_for_server(BootstrapOutcome::Reused, None, Some(instance)).await;
            }
            Ok(None) => {}
            Err(error) => {
                return Err(service_failure(format!(
                "Could not inspect the running Mitsuro on port {CANONICAL_SERVER_PORT}: {error:#}"
            )))
            }
        }

        // A listener that cannot prove Mitsuro identity is not safe to reuse
        // or replace. Leave it untouched and do not start systemd or a
        // companion on the occupied canonical port.
        match canonical_port_has_listener().await {
            Ok(true) => {
                return Err(BootstrapFailure {
                    kind: BootstrapFailureKind::Occupied,
                    message: format!(
                        "Port {CANONICAL_SERVER_PORT} is already in use by a process that did not prove it is Mitsuro. The listener was left untouched."
                    ),
                    diagnostics: Some(
                        "Stop the unknown listener if you want Native Canvas to start Mitsuro, or run Mitsuro on this port first."
                            .to_owned(),
                    ),
                })
            }
            Ok(false) => {}
            Err(error) => {
                return Err(service_failure(format!(
                    "Could not safely inspect port {CANONICAL_SERVER_PORT}: {error:#}"
                )))
            }
        }

        match systemd_unit_state().await {
            Ok(Some(_state)) => {
                let output = Command::new("systemctl")
                    .args(["--user", "start", SYSTEMD_UNIT])
                    .stdin(Stdio::null())
                    .output()
                    .await
                    .map_err(|error| {
                        service_failure(format!(
                            "Could not start the loaded {SYSTEMD_UNIT}: {error}"
                        ))
                    })?;
                if !output.status.success() {
                    return Err(BootstrapFailure {
                        kind: BootstrapFailureKind::ServiceFailed,
                        message: format!(
                            "The loaded {SYSTEMD_UNIT} failed to start. Mitsuro Canvas will not replace or restart it."
                        ),
                        diagnostics: Some(command_output(&output)),
                    });
                }
                wait_for_server(BootstrapOutcome::StartedSystemService, None, None).await
            }
            Ok(None) => {
                let path = resolve_companion().map_err(|error| BootstrapFailure {
                    kind: BootstrapFailureKind::CompanionMissing,
                    message: error.to_string(),
                    diagnostics: None,
                })?;
                let mut child = spawn_companion(&path)
                    .await
                    .map_err(|error| BootstrapFailure {
                        kind: BootstrapFailureKind::CompanionFailed,
                        message: format!("Could not launch {}: {error:#}", path.display()),
                        diagnostics: None,
                    })?;
                match wait_for_server(BootstrapOutcome::SpawnedCompanion, Some(&mut child), None)
                    .await
                {
                    Ok(connection) => Ok(connection),
                    Err(error) => {
                        terminate_unhealthy_child(&mut child).await;
                        Err(error)
                    }
                }
            }
            Err(error) => Err(service_failure(format!(
                "Could not inspect {SYSTEMD_UNIT}: {error:#}"
            ))),
        }
    }
}

async fn validate_instance(
    instance: ServerInstance,
    outcome: BootstrapOutcome,
) -> std::result::Result<ServerConnection, BootstrapFailure> {
    let client =
        MitsuroClient::new(format!("http://127.0.0.1:{}", instance.port)).map_err(|error| {
            BootstrapFailure {
                kind: BootstrapFailureKind::CompanionFailed,
                message: format!("Could not construct the local Mitsuro client: {error:#}"),
                diagnostics: None,
            }
        })?;
    let health = client.health().await.map_err(|error| BootstrapFailure {
        kind: BootstrapFailureKind::CompanionFailed,
        message: format!("The recorded Mitsuro server stopped answering: {error:#}"),
        diagnostics: None,
    })?;

    if health.status != "ok" || health.identity != HEALTH_IDENTITY || health.pid != instance.pid {
        return Err(BootstrapFailure {
            kind: BootstrapFailureKind::Incompatible,
            message: "The running process did not prove the canonical Mitsuro server identity. It was left untouched."
                .to_owned(),
            diagnostics: Some(format!(
                "recorded pid={} health identity={:?} health pid={} status={:?}",
                instance.pid, health.identity, health.pid, health.status
            )),
        });
    }

    let missing = missing_capabilities(&health);
    if health.api_protocol != SUPPORTED_API_PROTOCOL || !missing.is_empty() {
        let protocol = (health.api_protocol != SUPPORTED_API_PROTOCOL).then(|| {
            format!(
                "api_protocol:{} (client requires {})",
                health.api_protocol, SUPPORTED_API_PROTOCOL
            )
        });
        let requirements = protocol.into_iter().chain(missing).collect::<Vec<_>>();
        return Err(BootstrapFailure {
            kind: BootstrapFailureKind::Incompatible,
            message: format!(
                "The running Mitsuro server is healthy but cannot host Native Canvas: {}. It was left running.",
                requirements.join(", ")
            ),
            diagnostics: Some(format!(
                "server version={} api_protocol={} pid={}",
                health.version, health.api_protocol, health.pid
            )),
        });
    }

    Ok(ServerConnection {
        client,
        pid: instance.pid,
        port: instance.port,
        health,
        outcome,
    })
}

fn missing_capabilities(health: &HealthResponse) -> Vec<String> {
    REQUIRED_CAPABILITIES
        .iter()
        .filter_map(|(name, required)| {
            let available = health.capability_version(name);
            (available < *required).then(|| format!("{name}:{required} (server has {available})"))
        })
        .collect()
}

async fn wait_for_server(
    outcome: BootstrapOutcome,
    mut child: Option<&mut Child>,
    recorded_instance: Option<ServerInstance>,
) -> std::result::Result<ServerConnection, BootstrapFailure> {
    let deadline = Instant::now() + STARTUP_TIMEOUT;
    let mut stable: Option<StableHealthSample> = None;
    let mut stable_count = 0_u8;
    let mut last_error = String::new();
    let mut companion_exited = false;

    while Instant::now() < deadline {
        let mut exited_status = None;
        if let Some(process) = child.as_deref_mut() {
            if let Ok(Some(status)) = process.try_wait() {
                exited_status = Some(status);
            }
        }
        if let Some(status) = exited_status {
            // A second Canvas can lose the process-level singleton race after
            // another compatible companion has already won. Stop polling the
            // exited child, but keep probing the canonical PID authority so
            // both clients converge on the one server rather than reporting a
            // false startup failure or launching a replacement.
            child = None;
            companion_exited = true;
            last_error = format!("The spawned companion exited during startup ({status}).");
        }

        let candidate = match recorded_instance.as_ref() {
            Some(instance) => Some(instance.clone()),
            None => match detect_running_server().await {
                Some(instance) => Some(instance),
                None => discover_healthy_listener(CANONICAL_SERVER_PORT)
                    .await
                    .ok()
                    .flatten(),
            },
        };
        if let Some(instance) = candidate {
            let candidate_outcome = resolved_outcome(outcome, companion_exited);
            match validate_instance(instance.clone(), candidate_outcome).await {
                Ok(connection) => {
                    let sample = StableHealthSample {
                        port: connection.port,
                        health: connection.health.clone(),
                    };
                    if stable.as_ref() == Some(&sample) {
                        stable_count += 1;
                    } else {
                        stable = Some(sample);
                        stable_count = 1;
                    }
                    if stable_count >= 2 {
                        return Ok(connection);
                    }
                }
                Err(error) if error.kind == BootstrapFailureKind::Incompatible => {
                    return Err(error)
                }
                Err(error) => last_error = error.to_string(),
            }
        }
        sleep(HEALTH_INTERVAL).await;
    }

    let diagnostics = if outcome == BootstrapOutcome::StartedSystemService {
        systemd_diagnostics().await.ok()
    } else if last_error.is_empty() {
        None
    } else {
        Some(last_error)
    };
    Err(BootstrapFailure {
        kind: match outcome {
            BootstrapOutcome::StartedSystemService => BootstrapFailureKind::ServiceFailed,
            BootstrapOutcome::Reused | BootstrapOutcome::SpawnedCompanion => {
                BootstrapFailureKind::CompanionFailed
            }
        },
        message: "Mitsuro did not publish two stable matching health responses within 20 seconds. No replacement server was started."
            .to_owned(),
        diagnostics,
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct StableHealthSample {
    port: u16,
    health: HealthResponse,
}

fn trusted_recorded_instance() -> Option<ServerInstance> {
    let instance = read_pid_file()?;
    if process_can_host_canonical_server(instance.pid) {
        return Some(instance);
    }
    let _ = remove_pid_file_if_matches(instance.pid, instance.port);
    None
}

async fn discover_healthy_listener(port: u16) -> Result<Option<ServerInstance>> {
    let client = MitsuroClient::new(format!("http://127.0.0.1:{port}"))
        .context("constructing the local Mitsuro health client")?;
    let health = match client.health().await {
        Ok(health) => health,
        Err(_) => return Ok(None),
    };
    if health.status != "ok" || health.identity != HEALTH_IDENTITY || health.pid == 0 {
        return Ok(None);
    }
    if !process_can_host_canonical_server(health.pid) {
        return Ok(None);
    }
    Ok(Some(ServerInstance {
        pid: health.pid,
        port,
    }))
}

fn resolved_outcome(requested: BootstrapOutcome, companion_exited: bool) -> BootstrapOutcome {
    if companion_exited && requested == BootstrapOutcome::SpawnedCompanion {
        BootstrapOutcome::Reused
    } else {
        requested
    }
}

async fn systemd_unit_state() -> Result<Option<String>> {
    #[cfg(target_os = "linux")]
    {
        let output = Command::new("systemctl")
            .args([
                "--user",
                "show",
                SYSTEMD_UNIT,
                "--property=LoadState",
                "--value",
            ])
            .stdin(Stdio::null())
            .output()
            .await
            .context("running systemctl show")?;
        if !output.status.success() {
            bail!("{}", command_output(&output));
        }
        let state = String::from_utf8_lossy(&output.stdout).trim().to_owned();
        if state.is_empty() || state == "not-found" {
            Ok(None)
        } else {
            Ok(Some(state))
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        Ok(None)
    }
}

async fn canonical_port_has_listener() -> Result<bool> {
    tokio::task::spawn_blocking(|| {
        let address = SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::LOCALHOST,
            CANONICAL_SERVER_PORT,
        ));
        match TcpStream::connect_timeout(&address, Duration::from_millis(250)) {
            Ok(stream) => {
                drop(stream);
                Ok(true)
            }
            Err(error) if error.kind() == std::io::ErrorKind::ConnectionRefused => Ok(false),
            Err(error) => Err(error).context("probing the canonical loopback listener"),
        }
    })
    .await
    .context("joining the canonical listener probe")?
}

async fn systemd_diagnostics() -> Result<String> {
    let output = Command::new("systemctl")
        .args([
            "--user",
            "show",
            SYSTEMD_UNIT,
            "--property=LoadState,ActiveState,SubState,Result,ExecMainStatus",
        ])
        .stdin(Stdio::null())
        .output()
        .await
        .context("collecting system service diagnostics")?;
    Ok(command_output(&output))
}

fn resolve_companion() -> Result<PathBuf> {
    if let Some(configured) = std::env::var_os("MITSURO_SERVER_BIN") {
        return canonical_executable(Path::new(&configured))
            .context("validating MITSURO_SERVER_BIN");
    }

    let executable = std::env::current_exe().context("locating Mitsuro Canvas executable")?;
    let bin_dir = executable
        .parent()
        .context("Mitsuro Canvas executable has no parent directory")?;
    let mut candidates = vec![
        bin_dir.join("../libexec/mitsuro/mitsuro"),
        bin_dir.join("../lib/mitsuro/mitsuro"),
    ];
    if let Some(prefix) = option_env!("MITSURO_CANVAS_LIBEXEC_DIR") {
        candidates.insert(0, PathBuf::from(prefix).join("mitsuro"));
    }

    for candidate in candidates {
        if let Ok(path) = canonical_executable(&candidate) {
            return Ok(path);
        }
    }
    bail!(
        "No packaged Mitsuro companion was found. Development builds must set MITSURO_SERVER_BIN to the exact absolute matching `mitsuro` binary."
    )
}

fn canonical_executable(path: &Path) -> Result<PathBuf> {
    if !path.is_absolute() {
        bail!("server binary path must be absolute: {}", path.display());
    }
    let canonical = path
        .canonicalize()
        .with_context(|| format!("canonicalizing {}", path.display()))?;
    if !canonical.is_file() {
        bail!(
            "server binary is not a regular file: {}",
            canonical.display()
        );
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        if canonical.metadata()?.permissions().mode() & 0o111 == 0 {
            bail!("server binary is not executable: {}", canonical.display());
        }
    }
    Ok(canonical)
}

async fn spawn_companion(path: &Path) -> Result<Child> {
    let mut command = Command::new(path);
    command
        .args(["serve", "--desktop-managed", "--port"])
        .arg(CANONICAL_SERVER_PORT.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(false);
    #[cfg(unix)]
    command.process_group(0);
    command.spawn().context("spawning Mitsuro companion")
}

async fn terminate_unhealthy_child(child: &mut Child) {
    let _ = child.start_kill();
    let _ = child.wait().await;
}

fn command_output(output: &std::process::Output) -> String {
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    match (stdout.is_empty(), stderr.is_empty()) {
        (false, false) => format!("{stdout}\n{stderr}"),
        (false, true) => stdout,
        (true, false) => stderr,
        (true, true) => format!("process exited with {}", output.status),
    }
}

fn service_failure(message: String) -> BootstrapFailure {
    BootstrapFailure {
        kind: BootstrapFailureKind::ServiceFailed,
        message,
        diagnostics: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[cfg(target_os = "linux")]
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    fn health(capabilities: &[(&str, u16)]) -> HealthResponse {
        HealthResponse {
            status: "ok".to_owned(),
            identity: HEALTH_IDENTITY.to_owned(),
            pid: 7,
            version: "test".to_owned(),
            api_protocol: 1,
            capabilities: capabilities
                .iter()
                .map(|(name, version)| ((*name).to_owned(), *version))
                .collect::<HashMap<_, _>>(),
            features: HashMap::new(),
        }
    }

    #[test]
    fn reports_every_missing_contract() {
        let missing = missing_capabilities(&health(&[("desktop_canvases", 1)]));
        assert_eq!(missing.len(), 3);
        assert!(missing
            .iter()
            .any(|item| item.starts_with("terminal_sessions:2")));
    }

    #[test]
    fn accepts_the_exact_contract_floor() {
        assert!(missing_capabilities(&health(&REQUIRED_CAPABILITIES)).is_empty());
    }

    #[test]
    fn development_override_must_be_absolute() {
        let error = canonical_executable(Path::new("target/debug/mitsuro"))
            .expect_err("relative companion must be rejected");
        assert!(error.to_string().contains("absolute"));
    }

    #[test]
    fn stable_health_sample_detects_pid_and_capability_changes() {
        let first = StableHealthSample {
            port: 3000,
            health: health(&REQUIRED_CAPABILITIES),
        };
        let mut changed_pid = first.clone();
        changed_pid.health.pid += 1;
        assert_ne!(first, changed_pid);

        let mut changed_capability = first.clone();
        changed_capability
            .health
            .capabilities
            .insert("terminal_sessions".to_owned(), 3);
        assert_ne!(first, changed_capability);
    }

    #[test]
    fn a_losing_companion_is_classified_as_reusing_the_singleton_winner() {
        assert_eq!(
            resolved_outcome(BootstrapOutcome::SpawnedCompanion, false),
            BootstrapOutcome::SpawnedCompanion
        );
        assert_eq!(
            resolved_outcome(BootstrapOutcome::SpawnedCompanion, true),
            BootstrapOutcome::Reused
        );
        assert_eq!(
            resolved_outcome(BootstrapOutcome::StartedSystemService, true),
            BootstrapOutcome::StartedSystemService
        );
    }

    #[cfg(target_os = "linux")]
    fn spawn_harmless_winner(directory: &Path) -> Child {
        use std::os::unix::fs::PermissionsExt as _;

        let winner_path = directory.join("mitsuro");
        let mut source = std::fs::File::open("/bin/sleep").expect("open harmless process");
        let mut executable = std::fs::File::create(&winner_path).expect("create winner executable");
        fs2::FileExt::lock_exclusive(&executable).expect("lock winner writer");
        std::io::copy(&mut source, &mut executable).expect("copy harmless winner process");
        executable
            .set_permissions(std::fs::Permissions::from_mode(0o700))
            .expect("make winner executable");

        // std::fs opens CLOEXEC files, but a parallel fork can inherit the writer until exec,
        // causing ETXTBSY even after our copy closes. The inherited flock lasts
        // until that last writer closes; reacquire it through a read-only file.
        drop(executable);
        let executable = std::fs::File::open(&winner_path).expect("reopen winner read-only");
        fs2::FileExt::lock_shared(&executable).expect("wait for inherited winner writers");
        drop(executable);

        Command::new(&winner_path)
            // uutils/BusyBox dispatch by argv[0], while /proc/PID/exe must
            // retain the canonical `mitsuro` basename for the identity check.
            .arg0("sleep")
            .arg("60")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .expect("spawn singleton winner")
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn losing_companion_process_reuses_one_compatible_singleton_winner() {
        let temp = tempfile::tempdir().expect("temp dir");
        let mut winner = spawn_harmless_winner(temp.path());
        let winner_pid = winner.id().expect("winner pid");

        let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("bind isolated health server");
        let port = listener.local_addr().expect("listener address").port();
        let body = serde_json::json!({
            "status": "ok",
            "identity": HEALTH_IDENTITY,
            "pid": winner_pid,
            "version": "singleton-test",
            "api_protocol": SUPPORTED_API_PROTOCOL,
            "capabilities": REQUIRED_CAPABILITIES.into_iter().collect::<HashMap<_, _>>(),
            "features": {},
        })
        .to_string();
        let health_server = tokio::spawn(async move {
            let mut starting = true;
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                let mut request = [0_u8; 2048];
                let _ = stream.read(&mut request).await;
                let status = if std::mem::replace(&mut starting, false) {
                    "503 Service Unavailable"
                } else {
                    "200 OK"
                };
                let response = format!(
                    "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                    body.len(), body
                );
                let _ = stream.write_all(response.as_bytes()).await;
                let _ = stream.shutdown().await;
            }
        });

        let pid_path = mitsuro_core::paths::config_dir().join("server.pid");
        std::fs::create_dir_all(pid_path.parent().expect("PID parent"))
            .expect("create isolated PID parent");
        // Modern companions hold this lock while starting. Without it, a failed
        // health probe correctly removes the legacy record and loses this port.
        let singleton_lock = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(pid_path.with_extension("pid.lock"))
            .expect("open isolated singleton lock");
        fs2::FileExt::try_lock_exclusive(&singleton_lock).expect("claim singleton authority");
        std::fs::write(&pid_path, format!("{winner_pid}:{port}"))
            .expect("publish isolated winner record");

        assert!(detect_running_server().await.is_none());
        assert_eq!(
            read_pid_file(),
            Some(ServerInstance {
                pid: winner_pid,
                port
            })
        );
        let discovered = detect_running_server()
            .await
            .expect("locked winner must remain discoverable after its startup health failure");

        let mut loser = Command::new("/bin/false")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn losing companion");
        // Keep health polling on the discovered fixture, never a real service
        // that could be listening on the canonical port during a local run.
        let result = wait_for_server(
            BootstrapOutcome::SpawnedCompanion,
            Some(&mut loser),
            Some(discovered),
        )
        .await;

        health_server.abort();
        let _ = winner.start_kill();
        let _ = winner.wait().await;
        let _ = remove_pid_file_if_matches(winner_pid, port);
        drop(singleton_lock);

        let connection = result.expect("loser must attach to the compatible winner");
        assert_eq!(connection.outcome, BootstrapOutcome::Reused);
        assert_eq!(connection.pid, winner_pid);
        assert_eq!(connection.port, port);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reuses_a_health_proven_mitsuro_without_a_pid_record() {
        let temp = tempfile::tempdir().expect("temp dir");
        let mut winner = spawn_harmless_winner(temp.path());
        let winner_pid = winner.id().expect("winner pid");

        let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("bind isolated health server");
        let port = listener.local_addr().expect("listener address").port();
        let body = serde_json::json!({
            "status": "ok",
            "identity": HEALTH_IDENTITY,
            "pid": winner_pid,
            "version": "reuse-without-pid",
            "api_protocol": SUPPORTED_API_PROTOCOL,
            "capabilities": REQUIRED_CAPABILITIES.into_iter().collect::<HashMap<_, _>>(),
            "features": {},
        })
        .to_string();
        let health_server = tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                let mut request = [0_u8; 2048];
                let _ = stream.read(&mut request).await;
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = stream.write_all(response.as_bytes()).await;
                let _ = stream.shutdown().await;
            }
        });

        let discovered = discover_healthy_listener(port)
            .await
            .expect("health discovery should not fail");
        health_server.abort();
        let _ = winner.start_kill();
        let _ = winner.wait().await;

        let instance = discovered.expect("health-proven Mitsuro must be reused without a PID file");
        assert_eq!(instance.pid, winner_pid);
        assert_eq!(instance.port, port);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn ignores_a_listener_that_does_not_prove_mitsuro_identity() {
        let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("bind foreign listener");
        let port = listener.local_addr().expect("listener address").port();
        let foreign = tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                let mut request = [0_u8; 2048];
                let _ = stream.read(&mut request).await;
                let body = r#"{"status":"ok","identity":"not-mitsuro","pid":1,"version":"x"}"#;
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = stream.write_all(response.as_bytes()).await;
                let _ = stream.shutdown().await;
            }
        });

        let discovered = discover_healthy_listener(port)
            .await
            .expect("foreign health should be a non-Mitsuro miss, not a transport failure");
        foreign.abort();
        assert_eq!(discovered, None);
    }
}

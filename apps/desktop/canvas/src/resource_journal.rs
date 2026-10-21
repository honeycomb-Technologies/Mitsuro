use std::fs;
use std::io::{Read as _, Write as _};
use std::path::{Path, PathBuf};

use anyhow::{Context as _, Result};
use fs2::FileExt as _;
use mitsuro_client::CreateCanvasResourceRequest;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

const JOURNAL_VERSION: u16 = 1;
const MAX_JOURNAL_BYTES: u64 = 2 * 1024 * 1024;
const MAX_ENTRY_BYTES: u64 = 128 * 1024;
const MAX_JOURNAL_ENTRIES: usize = 64;
const MAX_INITIAL_PROMPT_BYTES: usize = 32 * 1024;
const MAX_ID_BYTES: usize = 512;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct ResourceCreateJournalEntry {
    pub server_origin: String,
    pub canvas_id: String,
    pub request: CreateCanvasResourceRequest,
    #[serde(default)]
    pub cleanup_requested: bool,
    /// Expected durable server detach generation. Presence makes Detach the
    /// monotonic accepted action and implies cleanup_requested.
    #[serde(default)]
    pub detach_generation: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResourceJournalRemovalMode {
    /// Ordinary server ownership or prompt acknowledgement must never erase
    /// a cleanup marker written by this or another app process.
    Normal,
    /// External cleanup and the authoritative Canvas update both completed.
    ConfirmedCleanup,
    /// The server atomically proved the original external binding absent
    /// while deliberately retaining its uncertain durable runtime intent.
    ConfirmedDetach,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResourceJournalRemoveOutcome {
    Removed,
    Missing,
    RetainedCleanupRequested,
}

#[derive(Debug, Deserialize, Serialize)]
struct ResourceCreateJournalFile {
    schema_version: u16,
    entry: ResourceCreateJournalEntry,
}

fn validate_entry(entry: &ResourceCreateJournalEntry) -> Result<()> {
    for (label, value) in [
        ("server origin", entry.server_origin.as_str()),
        ("canvas id", entry.canvas_id.as_str()),
        ("create operation id", entry.request.create_op_id.as_str()),
        ("client id", entry.request.client_id.as_str()),
        ("node id", entry.request.node.id.as_str()),
    ] {
        if value.is_empty() || value.len() > MAX_ID_BYTES || value.contains('\0') {
            anyhow::bail!("Canvas resource journal {label} is invalid");
        }
    }
    if entry.request.retry_ambiguous {
        anyhow::bail!("Canvas resource journal cannot persist an implicit ambiguous retry");
    }
    if entry.detach_generation.is_some() && !entry.cleanup_requested {
        anyhow::bail!("Canvas resource detach journal entry must also request cleanup");
    }
    if entry
        .request
        .initial_prompt
        .as_ref()
        .is_some_and(|prompt| prompt.len() > MAX_INITIAL_PROMPT_BYTES)
    {
        anyhow::bail!("Canvas resource journal initial prompt exceeds 32 KiB");
    }
    Ok(())
}

fn entry_identity(server_origin: &str, canvas_id: &str, create_op_id: &str) -> String {
    format!(
        "{}\u{1f}{canvas_id}\u{1f}{create_op_id}",
        server_origin.trim_end_matches('/')
    )
}

fn entry_filename(server_origin: &str, canvas_id: &str, create_op_id: &str) -> String {
    let digest = Sha256::digest(entry_identity(server_origin, canvas_id, create_op_id));
    format!("{digest:x}.json")
}

fn load_from_dir(directory: &Path) -> Result<Vec<ResourceCreateJournalEntry>> {
    if !directory.exists() {
        return Ok(Vec::new());
    }
    with_journal_lock(directory, true, || {
        prune_stale_temporary_files_unlocked(directory)?;
        load_from_dir_unlocked(directory)
    })
}

fn load_from_dir_unlocked(directory: &Path) -> Result<Vec<ResourceCreateJournalEntry>> {
    let entries = match fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error).context("opening Canvas resource create journal"),
    };
    let mut result = Vec::new();
    let mut total_bytes = 0_u64;
    for file in entries {
        let file = file.context("reading Canvas resource journal directory")?;
        let file_type = file
            .file_type()
            .context("reading Canvas resource journal entry type")?;
        if !file_type.is_file()
            || file
                .path()
                .extension()
                .and_then(|extension| extension.to_str())
                != Some("json")
        {
            continue;
        }
        if result.len() >= MAX_JOURNAL_ENTRIES {
            anyhow::bail!("Canvas resource create journal has too many entries");
        }
        let metadata = match file.metadata() {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => {
                return Err(error).context("reading Canvas resource journal entry metadata")
            }
        };
        if metadata.len() > MAX_ENTRY_BYTES {
            anyhow::bail!("Canvas resource create journal entry exceeds 128 KiB");
        }
        total_bytes = total_bytes.saturating_add(metadata.len());
        if total_bytes > MAX_JOURNAL_BYTES {
            anyhow::bail!("Canvas resource create journal exceeds 2 MiB");
        }
        let mut bytes = Vec::with_capacity(metadata.len() as usize);
        let opened = match fs::File::open(file.path()) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error).context("opening Canvas resource journal entry"),
        };
        opened
            .take(MAX_ENTRY_BYTES + 1)
            .read_to_end(&mut bytes)
            .context("reading Canvas resource journal entry")?;
        if bytes.len() as u64 > MAX_ENTRY_BYTES {
            anyhow::bail!("Canvas resource create journal entry exceeds 128 KiB");
        }
        let stored: ResourceCreateJournalFile =
            serde_json::from_slice(&bytes).context("parsing Canvas resource journal entry")?;
        if stored.schema_version != JOURNAL_VERSION {
            anyhow::bail!(
                "unsupported Canvas resource create journal version {}",
                stored.schema_version
            );
        }
        validate_entry(&stored.entry)?;
        let expected = entry_filename(
            &stored.entry.server_origin,
            &stored.entry.canvas_id,
            &stored.entry.request.create_op_id,
        );
        if file.file_name().to_string_lossy() != expected {
            anyhow::bail!("Canvas resource journal filename does not match its entry");
        }
        result.push(stored.entry);
    }
    result.sort_by(|left, right| {
        entry_identity(
            &left.server_origin,
            &left.canvas_id,
            &left.request.create_op_id,
        )
        .cmp(&entry_identity(
            &right.server_origin,
            &right.canvas_id,
            &right.request.create_op_id,
        ))
    });
    Ok(result)
}

fn persist_to_dir(
    directory: &Path,
    entry: ResourceCreateJournalEntry,
) -> Result<ResourceCreateJournalEntry> {
    validate_entry(&entry)?;
    ensure_journal_directory(directory)?;
    with_journal_lock(directory, true, || {
        prune_stale_temporary_files_unlocked(directory)?;
        persist_to_dir_unlocked(directory, entry)
    })
}

fn persist_to_dir_unlocked(
    directory: &Path,
    mut entry: ResourceCreateJournalEntry,
) -> Result<ResourceCreateJournalEntry> {
    let existing = load_from_dir_unlocked(directory)?;
    let existing_entry = existing.iter().find(|candidate| {
        candidate.server_origin.trim_end_matches('/') == entry.server_origin.trim_end_matches('/')
            && candidate.canvas_id == entry.canvas_id
            && candidate.request.create_op_id == entry.request.create_op_id
    });
    if let Some(existing_entry) = existing_entry {
        anyhow::ensure!(
            existing_entry.request == entry.request,
            "Canvas resource journal operation was reused with different input"
        );
        // Cleanup is a monotonic accepted state. A stale normal persist from
        // another client process cannot downgrade it.
        entry.cleanup_requested |= existing_entry.cleanup_requested;
        entry.detach_generation = existing_entry.detach_generation.or(entry.detach_generation);
        entry.cleanup_requested |= entry.detach_generation.is_some();
    }
    let is_new = existing_entry.is_none();
    if is_new && existing.len() >= MAX_JOURNAL_ENTRIES {
        anyhow::bail!("Canvas resource create journal is full");
    }
    let stored = ResourceCreateJournalFile {
        schema_version: JOURNAL_VERSION,
        entry: entry.clone(),
    };
    let bytes = serde_json::to_vec_pretty(&stored)
        .context("encoding Canvas resource create journal entry")?;
    if bytes.len() as u64 > MAX_ENTRY_BYTES {
        anyhow::bail!("Canvas resource create journal entry exceeds 128 KiB");
    }
    let filename = entry_filename(
        &entry.server_origin,
        &entry.canvas_id,
        &entry.request.create_op_id,
    );
    let destination = directory.join(filename);
    let existing_destination_bytes = destination.metadata().map_or(0, |metadata| metadata.len());
    let existing_total = journal_entry_bytes_unlocked(directory)?;
    let projected_total = existing_total
        .saturating_sub(existing_destination_bytes)
        .saturating_add(bytes.len() as u64);
    if projected_total > MAX_JOURNAL_BYTES {
        anyhow::bail!("Canvas resource create journal exceeds 2 MiB");
    }
    let temporary = directory.join(format!(
        ".{}.{}.{}.tmp",
        destination
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("resource-create"),
        std::process::id(),
        Uuid::new_v4()
    ));
    let mut options = fs::OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let publish = (|| -> Result<()> {
        let mut file = options
            .open(&temporary)
            .context("creating temporary Canvas resource journal entry")?;
        file.write_all(&bytes)
            .context("writing Canvas resource journal entry")?;
        file.sync_all()
            .context("syncing Canvas resource journal entry")?;
        fs::rename(&temporary, &destination).context("publishing Canvas resource journal entry")?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            fs::set_permissions(&destination, fs::Permissions::from_mode(0o600))
                .context("securing Canvas resource journal entry")?;
        }
        fs::File::open(directory)
            .context("opening Canvas resource journal directory")?
            .sync_all()
            .context("syncing Canvas resource journal directory")?;
        Ok(())
    })();
    if publish.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    publish?;
    Ok(entry)
}

fn remove_from_dir(
    directory: &Path,
    expected: &ResourceCreateJournalEntry,
    mode: ResourceJournalRemovalMode,
) -> Result<ResourceJournalRemoveOutcome> {
    if !directory.exists() {
        return Ok(ResourceJournalRemoveOutcome::Missing);
    }
    with_journal_lock(directory, true, || {
        prune_stale_temporary_files_unlocked(directory)?;
        remove_from_dir_unlocked(directory, expected, mode)
    })
}

fn remove_from_dir_unlocked(
    directory: &Path,
    expected: &ResourceCreateJournalEntry,
    mode: ResourceJournalRemovalMode,
) -> Result<ResourceJournalRemoveOutcome> {
    validate_entry(expected)?;
    let path = directory.join(entry_filename(
        &expected.server_origin,
        &expected.canvas_id,
        &expected.request.create_op_id,
    ));
    let mut bytes = Vec::new();
    let opened = match fs::File::open(&path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(ResourceJournalRemoveOutcome::Missing)
        }
        Err(error) => return Err(error).context("opening Canvas resource journal entry"),
    };
    opened
        .take(MAX_ENTRY_BYTES + 1)
        .read_to_end(&mut bytes)
        .context("reading Canvas resource journal entry")?;
    let stored: ResourceCreateJournalFile =
        serde_json::from_slice(&bytes).context("parsing Canvas resource journal entry")?;
    anyhow::ensure!(
        stored.schema_version == JOURNAL_VERSION,
        "unsupported Canvas resource create journal version {}",
        stored.schema_version
    );
    validate_entry(&stored.entry)?;
    anyhow::ensure!(
        stored.entry.server_origin.trim_end_matches('/')
            == expected.server_origin.trim_end_matches('/')
            && stored.entry.canvas_id == expected.canvas_id
            && stored.entry.request == expected.request,
        "Canvas resource journal removal compare-and-swap did not match"
    );
    if stored.entry.cleanup_requested && mode == ResourceJournalRemovalMode::Normal {
        return Ok(ResourceJournalRemoveOutcome::RetainedCleanupRequested);
    }
    if mode == ResourceJournalRemovalMode::ConfirmedDetach {
        anyhow::ensure!(
            stored.entry.detach_generation.is_some()
                && stored.entry.detach_generation == expected.detach_generation,
            "Canvas resource detach journal removal compare-and-swap did not match"
        );
    }
    match fs::remove_file(path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(ResourceJournalRemoveOutcome::Missing)
        }
        Err(error) => return Err(error).context("removing Canvas resource journal entry"),
    }
    fs::File::open(directory)
        .context("opening Canvas resource journal directory")?
        .sync_all()
        .context("syncing Canvas resource journal directory")?;
    Ok(ResourceJournalRemoveOutcome::Removed)
}

fn ensure_journal_directory(directory: &Path) -> Result<()> {
    fs::create_dir_all(directory).context("creating Canvas resource journal directory")?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        fs::set_permissions(directory, fs::Permissions::from_mode(0o700))
            .context("securing Canvas resource journal directory")?;
    }
    Ok(())
}

fn with_journal_lock<T>(
    directory: &Path,
    exclusive: bool,
    operation: impl FnOnce() -> Result<T>,
) -> Result<T> {
    ensure_journal_directory(directory)?;
    let lock_path = directory.join(".journal.lock");
    let mut options = fs::OpenOptions::new();
    options.create(true).read(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let lock = options
        .open(lock_path)
        .context("opening Canvas resource journal lock")?;
    if exclusive {
        lock.lock_exclusive()
            .context("locking Canvas resource journal")?;
    } else {
        lock.lock_shared()
            .context("locking Canvas resource journal")?;
    }
    let result = operation();
    fs2::FileExt::unlock(&lock).context("unlocking Canvas resource journal")?;
    result
}

fn prune_stale_temporary_files_unlocked(directory: &Path) -> Result<()> {
    let entries = match fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error).context("reading Canvas resource journal directory"),
    };
    let mut removed = false;
    for entry in entries {
        let entry = entry.context("reading Canvas resource journal directory")?;
        let file_type = match entry.file_type() {
            Ok(file_type) => file_type,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => {
                return Err(error).context("reading Canvas resource journal temporary type")
            }
        };
        if !file_type.is_file() || !valid_temporary_filename(&entry.file_name()) {
            continue;
        }
        match fs::remove_file(entry.path()) {
            Ok(()) => removed = true,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error).context("removing stale Canvas resource journal temporary")
            }
        }
    }
    if removed {
        fs::File::open(directory)
            .context("opening Canvas resource journal directory")?
            .sync_all()
            .context("syncing stale Canvas resource journal cleanup")?;
    }
    Ok(())
}

fn valid_temporary_filename(name: &std::ffi::OsStr) -> bool {
    let Some(name) = name.to_str() else {
        return false;
    };
    let parts = name.split('.').collect::<Vec<_>>();
    parts.len() == 6
        && parts[0].is_empty()
        && parts[1].len() == 64
        && parts[1].bytes().all(|byte| byte.is_ascii_hexdigit())
        && parts[2] == "json"
        && parts[3].parse::<u32>().is_ok()
        && Uuid::parse_str(parts[4]).is_ok()
        && parts[5] == "tmp"
}

fn journal_entry_bytes_unlocked(directory: &Path) -> Result<u64> {
    let mut total = 0_u64;
    for entry in fs::read_dir(directory).context("reading Canvas resource journal directory")? {
        let entry = entry.context("reading Canvas resource journal directory")?;
        if entry
            .path()
            .extension()
            .and_then(|extension| extension.to_str())
            != Some("json")
        {
            continue;
        }
        let metadata = match entry.metadata() {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error).context("reading Canvas resource journal metadata"),
        };
        total = total.saturating_add(metadata.len());
    }
    Ok(total)
}

pub fn load_default_for_origin(server_origin: &str) -> Result<Vec<ResourceCreateJournalEntry>> {
    let origin = server_origin.trim_end_matches('/');
    Ok(load_from_dir(&default_dir())?
        .into_iter()
        .filter(|entry| entry.server_origin.trim_end_matches('/') == origin)
        .collect())
}

pub fn persist_default(entry: ResourceCreateJournalEntry) -> Result<ResourceCreateJournalEntry> {
    persist_to_dir(&default_dir(), entry)
}

pub fn remove_default(
    expected: &ResourceCreateJournalEntry,
    mode: ResourceJournalRemovalMode,
) -> Result<ResourceJournalRemoveOutcome> {
    remove_from_dir(&default_dir(), expected, mode)
}

fn default_dir() -> PathBuf {
    mitsuro_core::paths::config_dir()
        .join("desktop-native")
        .join("resource-creates")
}

#[cfg(test)]
mod tests {
    use super::*;
    use mitsuro_client::{CanvasResourceNodeRequest, CanvasResourceSpec, TerminalSize, WorldRect};

    fn request() -> CreateCanvasResourceRequest {
        CreateCanvasResourceRequest {
            create_op_id: Uuid::new_v4().to_string(),
            client_id: Uuid::new_v4().to_string(),
            node: CanvasResourceNodeRequest {
                id: Uuid::new_v4().to_string(),
                title: "Terminal".to_owned(),
                rect: WorldRect {
                    x: -20.0,
                    y: 10.0,
                    width: 720.0,
                    height: 420.0,
                },
                z_index: 4,
            },
            resource: CanvasResourceSpec::Terminal {
                size: TerminalSize::default(),
            },
            initial_prompt: None,
            retry_ambiguous: false,
        }
    }

    #[test]
    fn crash_before_post_round_trips_the_exact_stable_request() {
        let temp = tempfile::tempdir().expect("temp");
        let entry = ResourceCreateJournalEntry {
            server_origin: "http://127.0.0.1:3000".to_owned(),
            canvas_id: "canvas-a".to_owned(),
            request: request(),
            cleanup_requested: false,
            detach_generation: None,
        };
        persist_to_dir(temp.path(), entry.clone()).expect("persist before POST");
        let loaded = load_from_dir(temp.path()).expect("restart load");
        assert_eq!(loaded, vec![entry]);
    }

    #[test]
    fn per_entry_files_preserve_simultaneous_client_requests() {
        let temp = tempfile::tempdir().expect("temp");
        let first = ResourceCreateJournalEntry {
            server_origin: "http://127.0.0.1:3000".to_owned(),
            canvas_id: "canvas-a".to_owned(),
            request: request(),
            cleanup_requested: false,
            detach_generation: None,
        };
        let mut second = first.clone();
        second.canvas_id = "canvas-b".to_owned();
        second.request.create_op_id = Uuid::new_v4().to_string();
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(3));
        let first_thread = {
            let directory = temp.path().to_owned();
            let barrier = barrier.clone();
            let first = first.clone();
            std::thread::spawn(move || {
                barrier.wait();
                persist_to_dir(&directory, first)
            })
        };
        let second_thread = {
            let directory = temp.path().to_owned();
            let barrier = barrier.clone();
            let second = second.clone();
            std::thread::spawn(move || {
                barrier.wait();
                persist_to_dir(&directory, second)
            })
        };
        barrier.wait();
        first_thread
            .join()
            .expect("first thread")
            .expect("first client");
        second_thread
            .join()
            .expect("second thread")
            .expect("second client");
        assert_eq!(load_from_dir(temp.path()).expect("load").len(), 2);
        remove_from_dir(temp.path(), &first, ResourceJournalRemovalMode::Normal)
            .expect("remove first");
        assert_eq!(load_from_dir(temp.path()).expect("load"), vec![second]);
    }

    #[test]
    fn simultaneous_persist_remove_and_load_remain_consistent() {
        let temp = tempfile::tempdir().expect("temp");
        let first = ResourceCreateJournalEntry {
            server_origin: "http://127.0.0.1:3000".to_owned(),
            canvas_id: "canvas-a".to_owned(),
            request: request(),
            cleanup_requested: false,
            detach_generation: None,
        };
        let mut second = first.clone();
        second.canvas_id = "canvas-b".to_owned();
        second.request.create_op_id = Uuid::new_v4().to_string();
        persist_to_dir(temp.path(), first.clone()).expect("seed first");

        let barrier = std::sync::Arc::new(std::sync::Barrier::new(4));
        let persist = {
            let directory = temp.path().to_owned();
            let barrier = barrier.clone();
            let second = second.clone();
            std::thread::spawn(move || {
                barrier.wait();
                persist_to_dir(&directory, second)
            })
        };
        let remove = {
            let directory = temp.path().to_owned();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                remove_from_dir(&directory, &first, ResourceJournalRemovalMode::Normal)
            })
        };
        let load = {
            let directory = temp.path().to_owned();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                load_from_dir(&directory)
            })
        };
        barrier.wait();
        persist.join().expect("persist thread").expect("persist");
        remove.join().expect("remove thread").expect("remove");
        assert!(load.join().expect("load thread").is_ok());
        assert_eq!(
            load_from_dir(temp.path()).expect("final load"),
            vec![second]
        );
    }

    #[cfg(unix)]
    #[test]
    fn journal_entries_are_mode_private() {
        use std::os::unix::fs::PermissionsExt as _;
        let temp = tempfile::tempdir().expect("temp");
        let entry = ResourceCreateJournalEntry {
            server_origin: "http://127.0.0.1:3000".to_owned(),
            canvas_id: "canvas-a".to_owned(),
            request: request(),
            cleanup_requested: false,
            detach_generation: None,
        };
        persist_to_dir(temp.path(), entry).expect("save");
        let file = fs::read_dir(temp.path())
            .expect("entries")
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.path())
            .find(|path| path.extension().and_then(|extension| extension.to_str()) == Some("json"))
            .expect("journal entry");
        assert_eq!(
            fs::metadata(file).expect("metadata").permissions().mode() & 0o777,
            0o600
        );
        assert_eq!(
            fs::metadata(temp.path())
                .expect("metadata")
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
    }

    #[test]
    fn journal_rejects_retry_authority_and_bounded_overflow() {
        let mut retry = request();
        retry.retry_ambiguous = true;
        let entry = ResourceCreateJournalEntry {
            server_origin: "http://127.0.0.1:3000".to_owned(),
            canvas_id: "canvas-a".to_owned(),
            request: retry,
            cleanup_requested: false,
            detach_generation: None,
        };
        assert!(validate_entry(&entry).is_err());

        let temp = tempfile::tempdir().expect("temp");
        for index in 0..MAX_JOURNAL_ENTRIES {
            let mut entry = entry.clone();
            entry.request.retry_ambiguous = false;
            entry.request.create_op_id = format!("op-{index}");
            persist_to_dir(temp.path(), entry).expect("bounded entry");
        }
        let mut overflow = entry;
        overflow.request.retry_ambiguous = false;
        overflow.request.create_op_id = "overflow".to_owned();
        assert!(persist_to_dir(temp.path(), overflow).is_err());
    }

    #[test]
    fn stale_normal_remove_cannot_erase_a_concurrent_cleanup_marker() {
        let temp = tempfile::tempdir().expect("temp");
        let pending = ResourceCreateJournalEntry {
            server_origin: "http://127.0.0.1:3000".to_owned(),
            canvas_id: "canvas-a".to_owned(),
            request: request(),
            cleanup_requested: false,
            detach_generation: None,
        };
        persist_to_dir(temp.path(), pending.clone()).expect("publish pending");
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(3));
        let cancellation = {
            let directory = temp.path().to_owned();
            let barrier = barrier.clone();
            let mut cancelled = pending.clone();
            cancelled.cleanup_requested = true;
            std::thread::spawn(move || {
                barrier.wait();
                persist_to_dir(&directory, cancelled)
            })
        };
        let stale_clear = {
            let directory = temp.path().to_owned();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                // Retry until cancellation has either won first or this
                // stale clear wins first. If the clear wins, cancellation
                // recreates the monotonic marker; if cancellation wins, CAS
                // must retain it.
                remove_from_dir(&directory, &pending, ResourceJournalRemovalMode::Normal)
            })
        };
        barrier.wait();
        cancellation
            .join()
            .expect("cancel thread")
            .expect("persist cleanup marker");
        let _ = stale_clear
            .join()
            .expect("clear thread")
            .expect("stale normal clear");
        let loaded = load_from_dir(temp.path()).expect("load final marker");
        assert_eq!(loaded.len(), 1);
        assert!(loaded[0].cleanup_requested);
    }

    #[test]
    fn normal_persist_adopts_cleanup_written_by_another_process() {
        let temp = tempfile::tempdir().expect("temp");
        let pending = ResourceCreateJournalEntry {
            server_origin: "http://127.0.0.1:3000".to_owned(),
            canvas_id: "canvas-a".to_owned(),
            request: request(),
            cleanup_requested: false,
            detach_generation: None,
        };
        let mut cancelled = pending.clone();
        cancelled.cleanup_requested = true;
        persist_to_dir(temp.path(), cancelled).expect("persist cancellation");
        let adopted = persist_to_dir(temp.path(), pending).expect("stale normal persist");
        assert!(adopted.cleanup_requested);
    }

    #[test]
    fn stale_valid_temp_files_are_pruned_but_unrelated_files_are_preserved() {
        let temp = tempfile::tempdir().expect("temp");
        let raw_prompt = b"private prompt";
        let valid = temp.path().join(format!(
            ".{}.json.{}.{}.tmp",
            "a".repeat(64),
            std::process::id(),
            Uuid::new_v4()
        ));
        fs::write(&valid, raw_prompt).expect("write stale journal temp");
        let unrelated = temp.path().join("notes.tmp");
        fs::write(&unrelated, raw_prompt).expect("write unrelated temp");
        assert!(load_from_dir(temp.path())
            .expect("load and prune")
            .is_empty());
        assert!(!valid.exists());
        assert!(unrelated.exists());
    }
}

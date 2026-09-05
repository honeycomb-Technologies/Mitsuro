use std::io::Read as _;
use std::path::{Component, Path, PathBuf};

use axum::{
    extract::{Path as AxumPath, Query, State},
    routing::get,
    Json, Router,
};
use cap_fs_ext::{ambient_authority, FollowSymlinks, OpenOptionsFollowExt as _};
use cap_std::fs::{Dir, OpenOptions};

use mitsuro_core::storage::{Database, SpatialCanvasStore};

use crate::auth::CurrentUser;
use crate::error::AppError;
use crate::types::{FileQuery, FileResponse, TreeEntry, TreeQuery, TreeResponse};
use crate::AppState;

const MAX_TREE_ENTRIES: usize = 10_000;
const MAX_CANVAS_TEXT_BYTES: usize = 4 * 1024 * 1024;

pub(super) fn router() -> Router<AppState> {
    Router::new()
        .route("/:id/files", get(read_canvas_file))
        .route("/:id/files/tree", get(canvas_file_tree))
}

async fn read_canvas_file(
    State(state): State<AppState>,
    CurrentUser(user): CurrentUser,
    AxumPath(canvas_id): AxumPath<String>,
    Query(query): Query<FileQuery>,
) -> Result<Json<FileResponse>, AppError> {
    let project_dir =
        owned_canvas_project_dir(state.db_path.as_ref(), &canvas_id, user.user_id.as_deref())?;
    let requested_path = query.path;
    let response = tokio::task::spawn_blocking(move || {
        read_project_file(&project_dir, requested_path.as_str())
    })
    .await
    .map_err(|error| AppError::Internal(format!("Canvas file read task failed: {error}")))??;
    Ok(Json(response))
}

async fn canvas_file_tree(
    State(state): State<AppState>,
    CurrentUser(user): CurrentUser,
    AxumPath(canvas_id): AxumPath<String>,
    Query(query): Query<TreeQuery>,
) -> Result<Json<TreeResponse>, AppError> {
    if query.root.is_some() {
        return Err(AppError::BadRequest(
            "Canvas file tree root is server-owned".to_string(),
        ));
    }
    let project_dir =
        owned_canvas_project_dir(state.db_path.as_ref(), &canvas_id, user.user_id.as_deref())?;
    let response =
        tokio::task::spawn_blocking(move || project_file_tree(&project_dir, query.depth))
            .await
            .map_err(|error| {
                AppError::Internal(format!("Canvas file tree task failed: {error}"))
            })??;
    Ok(Json(response))
}

pub(crate) fn owned_canvas_project_dir(
    db_path: &Path,
    canvas_id: &str,
    owner_user_id: Option<&str>,
) -> Result<PathBuf, AppError> {
    let store = SpatialCanvasStore::new(Database::new(db_path)?);
    let canvas = store
        .get_for_exact_owner(canvas_id, owner_user_id)
        .map_err(AppError::from)?
        .ok_or_else(|| AppError::NotFound(format!("Canvas {canvas_id} not found")))?;
    Ok(PathBuf::from(canvas.project_dir))
}

fn read_project_file(project_dir: &Path, requested: &str) -> Result<FileResponse, AppError> {
    let relative = validated_relative_path(requested)?;
    let mut directory = open_project_root(project_dir)?;
    let components = relative.components().collect::<Vec<_>>();
    let (file_name, parents) = components
        .split_last()
        .ok_or_else(|| AppError::BadRequest("File path cannot be empty".to_string()))?;

    for component in parents {
        let Component::Normal(name) = component else {
            return Err(invalid_relative_path());
        };
        directory = open_child_directory(&directory, Path::new(name), requested)?;
    }

    let Component::Normal(file_name) = file_name else {
        return Err(invalid_relative_path());
    };
    let file_name = Path::new(file_name);
    let metadata = directory
        .symlink_metadata(file_name)
        .map_err(|error| map_entry_error(error, requested))?;
    let file_type = metadata.file_type();
    if file_type.is_symlink() || !file_type.is_file() {
        return Err(AppError::BadRequest(format!(
            "Canvas file path must name a regular file: {requested}"
        )));
    }

    let mut file = directory
        .open_with(file_name, &nofollow_read_options())
        .map_err(|error| map_entry_error(error, requested))?;
    let opened_metadata = file
        .metadata()
        .map_err(|error| AppError::Internal(format!("Failed to inspect canvas file: {error}")))?;
    if !opened_metadata.is_file() {
        return Err(AppError::BadRequest(format!(
            "Canvas file path must name a regular file: {requested}"
        )));
    }

    // This endpoint feeds an interactive read-only surface, not an upload or
    // bulk export. Keep one request from allocating the server-wide 100 MiB
    // body ceiling and starving Agent/terminal/browser work.
    let limit = MAX_CANVAS_TEXT_BYTES;
    if opened_metadata.len() > limit as u64 {
        return Err(AppError::BadRequest(format!(
            "File exceeds maximum text size of {}MB",
            limit / (1024 * 1024)
        )));
    }
    let mut bytes = Vec::with_capacity((opened_metadata.len() as usize).min(limit));
    file.by_ref()
        .take(limit as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| AppError::Internal(format!("Failed to read canvas file: {error}")))?;
    if bytes.len() > limit {
        return Err(AppError::BadRequest(format!(
            "File exceeds maximum text size of {}MB",
            limit / (1024 * 1024)
        )));
    }
    let size = bytes.len() as u64;
    let content = String::from_utf8(bytes).map_err(|_| {
        AppError::BadRequest(
            "File is not valid UTF-8 text; binary files are not supported by this endpoint"
                .to_string(),
        )
    })?;

    Ok(FileResponse {
        path: relative_path_string(&relative)?,
        content,
        size,
    })
}

fn project_file_tree(project_dir: &Path, depth: usize) -> Result<TreeResponse, AppError> {
    let root = open_project_root(project_dir)?;
    let mut count = 0;
    let entries = build_tree(&root, Path::new(""), depth.min(10), &mut count)?;
    Ok(TreeResponse {
        root: ".".to_string(),
        entries,
    })
}

fn build_tree(
    directory: &Dir,
    relative_dir: &Path,
    depth: usize,
    count: &mut usize,
) -> Result<Vec<TreeEntry>, AppError> {
    if depth == 0 || *count >= MAX_TREE_ENTRIES {
        return Ok(Vec::new());
    }

    let mut entries = Vec::new();
    let read_dir = directory
        .entries()
        .map_err(|error| AppError::Internal(format!("Failed to read canvas directory: {error}")))?;
    for entry in read_dir {
        if *count >= MAX_TREE_ENTRIES {
            break;
        }
        let entry = entry.map_err(|error| {
            AppError::Internal(format!("Failed to read canvas directory entry: {error}"))
        })?;
        let Ok(name) = entry.file_name().into_string() else {
            continue;
        };
        if tree_hides(&name) {
            continue;
        }
        let file_type = match entry.file_type() {
            Ok(file_type) => file_type,
            Err(_) => continue,
        };
        // Canvas Files is deliberately narrower than the generic file API:
        // symlinks and special files are not disclosed or traversed.
        if file_type.is_symlink() || (!file_type.is_dir() && !file_type.is_file()) {
            continue;
        }

        let relative_path = relative_dir.join(&name);
        let relative_path_string = match relative_path.to_str() {
            Some(path) => path.to_string(),
            None => continue,
        };
        let is_dir = file_type.is_dir();
        *count += 1;
        let children = if is_dir && depth > 1 {
            let opened = match entry.open_with(&nofollow_read_options()) {
                Ok(opened) => opened,
                Err(_) => continue,
            };
            match opened.metadata() {
                Ok(metadata) if metadata.is_dir() => {}
                _ => continue,
            }
            let child_dir = Dir::from_std_file(opened.into_std());
            Some(build_tree(&child_dir, &relative_path, depth - 1, count)?)
        } else {
            None
        };

        entries.push(TreeEntry {
            name,
            path: relative_path_string,
            is_dir,
            children,
        });
    }
    entries.sort_by(|left, right| match (left.is_dir, right.is_dir) {
        (true, false) => std::cmp::Ordering::Less,
        (false, true) => std::cmp::Ordering::Greater,
        _ => left.name.cmp(&right.name),
    });
    Ok(entries)
}

fn open_project_root(project_dir: &Path) -> Result<Dir, AppError> {
    if !project_dir.is_absolute() {
        return Err(AppError::BadRequest(
            "Canvas project directory is not an absolute canonical path".to_string(),
        ));
    }
    let mut directory = Dir::open_ambient_dir(Path::new("/"), ambient_authority())
        .map_err(|error| AppError::Internal(format!("Failed to open filesystem root: {error}")))?;
    let mut saw_component = false;
    for component in project_dir.components() {
        match component {
            Component::RootDir => {}
            Component::Normal(name) => {
                saw_component = true;
                directory = open_child_directory(
                    &directory,
                    Path::new(name),
                    &project_dir.display().to_string(),
                )?;
            }
            _ => {
                return Err(AppError::BadRequest(
                    "Canvas project directory is not an absolute canonical path".to_string(),
                ));
            }
        }
    }
    if !saw_component {
        return Err(AppError::BadRequest(
            "Canvas project directory cannot be the filesystem root".to_string(),
        ));
    }
    Ok(directory)
}

fn open_child_directory(parent: &Dir, name: &Path, display_path: &str) -> Result<Dir, AppError> {
    let metadata = parent
        .symlink_metadata(name)
        .map_err(|error| map_entry_error(error, display_path))?;
    let file_type = metadata.file_type();
    if file_type.is_symlink() || !file_type.is_dir() {
        return Err(AppError::BadRequest(format!(
            "Canvas path contains a non-directory or symbolic link: {display_path}"
        )));
    }
    let opened = parent
        .open_with(name, &nofollow_read_options())
        .map_err(|error| map_entry_error(error, display_path))?;
    let opened_metadata = opened.metadata().map_err(|error| {
        AppError::Internal(format!("Failed to inspect canvas directory: {error}"))
    })?;
    if !opened_metadata.is_dir() {
        return Err(AppError::BadRequest(format!(
            "Canvas path contains a non-directory or symbolic link: {display_path}"
        )));
    }
    Ok(Dir::from_std_file(opened.into_std()))
}

fn nofollow_read_options() -> OpenOptions {
    let mut options = OpenOptions::new();
    options.read(true).follow(FollowSymlinks::No);
    #[cfg(unix)]
    {
        use cap_std::fs::OpenOptionsExt as _;
        options.custom_flags(libc::O_NONBLOCK);
    }
    options
}

fn validated_relative_path(requested: &str) -> Result<PathBuf, AppError> {
    if requested.trim().is_empty()
        || requested.len() > 4_096
        || requested.contains('\0')
        || requested
            .split('/')
            .any(|segment| segment.is_empty() || matches!(segment, "." | ".."))
    {
        return Err(invalid_relative_path());
    }
    let path = Path::new(requested);
    if path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(invalid_relative_path());
    }
    Ok(path.to_path_buf())
}

fn relative_path_string(path: &Path) -> Result<String, AppError> {
    path.to_str()
        .map(str::to_owned)
        .ok_or_else(invalid_relative_path)
}

fn invalid_relative_path() -> AppError {
    AppError::BadRequest(
        "Canvas file path must be a non-empty, UTF-8, project-relative path without dot segments"
            .to_string(),
    )
}

fn map_entry_error(error: std::io::Error, requested: &str) -> AppError {
    if error.kind() == std::io::ErrorKind::NotFound {
        AppError::NotFound(format!("Canvas path not found: {requested}"))
    } else {
        AppError::BadRequest(format!("Canvas path is unavailable: {requested}"))
    }
}

fn tree_hides(name: &str) -> bool {
    name.starts_with('.') || matches!(name, "node_modules" | "target")
}

#[cfg(test)]
mod tests {
    use super::*;
    use mitsuro_core::storage::{CreateCanvasInput, SpatialCanvasSurface};

    fn create_owned_canvas(
        db_path: &Path,
        project_dir: &Path,
        owner_user_id: Option<&str>,
    ) -> String {
        let store = SpatialCanvasStore::new(Database::new(db_path).expect("database"));
        store
            .create_for_exact_owner(
                owner_user_id,
                CreateCanvasInput {
                    create_op_id: uuid::Uuid::new_v4().to_string(),
                    project_dir: project_dir.to_string_lossy().into_owned(),
                    surface: SpatialCanvasSurface::Code,
                    name: "Main".to_string(),
                    shared_theme_id: "graphite".to_string(),
                },
            )
            .expect("canvas")
            .canvas
            .id
    }

    fn flatten_paths(entries: &[TreeEntry], paths: &mut Vec<String>) {
        for entry in entries {
            paths.push(entry.path.clone());
            if let Some(children) = entry.children.as_deref() {
                flatten_paths(children, paths);
            }
        }
    }

    #[test]
    fn canvas_project_resolution_requires_the_exact_owner() {
        let temp = tempfile::tempdir().expect("temp");
        let project = temp.path().join("project");
        std::fs::create_dir(&project).expect("project");
        let db_path = temp.path().join("mitsuro.db");
        let canvas_id = create_owned_canvas(&db_path, &project, Some("alice"));

        assert_eq!(
            owned_canvas_project_dir(&db_path, &canvas_id, Some("alice")).expect("owned project"),
            project
        );
        assert!(matches!(
            owned_canvas_project_dir(&db_path, &canvas_id, Some("bob")),
            Err(AppError::NotFound(_))
        ));
        assert!(matches!(
            owned_canvas_project_dir(&db_path, &canvas_id, None),
            Err(AppError::NotFound(_))
        ));
    }

    #[test]
    fn regular_canvas_files_are_relative_and_readable() {
        let temp = tempfile::tempdir().expect("temp");
        let project = temp.path().join("project");
        std::fs::create_dir_all(project.join("src")).expect("src");
        std::fs::write(project.join("src/lib.rs"), "pub fn alpha() {}\n").expect("file");

        let file = read_project_file(&project, "src/lib.rs").expect("read file");
        assert_eq!(file.path, "src/lib.rs");
        assert_eq!(file.content, "pub fn alpha() {}\n");
        let tree = project_file_tree(&project, 4).expect("tree");
        assert_eq!(tree.root, ".");
        let mut paths = Vec::new();
        flatten_paths(&tree.entries, &mut paths);
        assert!(paths.contains(&"src".to_string()));
        assert!(paths.contains(&"src/lib.rs".to_string()));
        assert!(paths.iter().all(|path| !Path::new(path).is_absolute()));
    }

    #[test]
    fn oversized_canvas_text_is_rejected_before_reading() {
        let temp = tempfile::tempdir().expect("temp");
        let project = temp.path().join("project");
        std::fs::create_dir_all(&project).expect("project");
        let path = project.join("oversized.txt");
        let file = std::fs::File::create(&path).expect("file");
        file.set_len(MAX_CANVAS_TEXT_BYTES as u64 + 1)
            .expect("extend sparse file");

        let error = match read_project_file(&project, "oversized.txt") {
            Ok(_) => panic!("oversized interactive file must be rejected"),
            Err(error) => error,
        };
        assert!(matches!(error, AppError::BadRequest(_)));
    }

    #[test]
    fn canvas_file_paths_reject_parent_absolute_and_dot_segments() {
        assert!(matches!(
            validated_relative_path("../secret"),
            Err(AppError::BadRequest(_))
        ));
        assert!(matches!(
            validated_relative_path("/tmp/secret"),
            Err(AppError::BadRequest(_))
        ));
        assert!(matches!(
            validated_relative_path("src/./lib.rs"),
            Err(AppError::BadRequest(_))
        ));
        assert!(validated_relative_path("src/lib.rs").is_ok());
    }

    #[cfg(unix)]
    #[test]
    fn canvas_tree_and_read_do_not_disclose_symlink_targets_or_special_files() {
        use std::os::unix::net::UnixListener;

        let temp = tempfile::tempdir().expect("temp");
        let project = temp.path().join("project");
        let outside = temp.path().join("outside");
        std::fs::create_dir_all(&project).expect("project");
        std::fs::create_dir_all(&outside).expect("outside");
        std::fs::write(outside.join("secret.txt"), "DO-NOT-DISCLOSE").expect("secret");
        std::os::unix::fs::symlink(&outside, project.join("leak-dir")).expect("directory link");
        std::os::unix::fs::symlink(outside.join("secret.txt"), project.join("leak-file"))
            .expect("file link");
        std::os::unix::fs::symlink("missing-target", project.join("broken-link"))
            .expect("broken link");
        let _socket = UnixListener::bind(project.join("special.sock")).expect("socket");

        let tree = project_file_tree(&project, 4).expect("tree");
        let mut paths = Vec::new();
        flatten_paths(&tree.entries, &mut paths);
        for forbidden in ["leak-dir", "leak-file", "broken-link", "special.sock"] {
            assert!(!paths.iter().any(|path| path == forbidden));
        }
        for forbidden in [
            "leak-dir/secret.txt",
            "leak-file",
            "broken-link",
            "special.sock",
        ] {
            let error = match read_project_file(&project, forbidden) {
                Ok(_) => panic!("{forbidden} must fail closed"),
                Err(error) => error,
            };
            assert!(matches!(error, AppError::BadRequest(_)));
        }
    }
}

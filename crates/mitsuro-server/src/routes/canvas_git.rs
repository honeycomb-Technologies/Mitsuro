//! Read-only Git changes derived from an exact-owner Canvas project.

use std::path::{Path, PathBuf};

use axum::{
    extract::{Path as AxumPath, Query, State},
    routing::get,
    Json, Router,
};
use serde::Deserialize;

use crate::auth::CurrentUser;
use crate::error::AppError;
use crate::types::{GitChangedFileResponse, GitChangesResponse, GitFileDiffResponse};
use crate::AppState;

#[derive(Debug, Default, Deserialize)]
struct CanvasGitQuery {
    /// Rejected compatibility field: the Canvas owns the project root.
    path: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CanvasGitDiffQuery {
    /// Rejected compatibility field: the Canvas owns the project root.
    path: Option<String>,
    file: String,
}

pub(super) fn router() -> Router<AppState> {
    Router::new()
        .route("/:id/changes", get(list_canvas_changes))
        .route("/:id/diff", get(get_canvas_file_diff))
}

async fn list_canvas_changes(
    State(state): State<AppState>,
    CurrentUser(user): CurrentUser,
    AxumPath(canvas_id): AxumPath<String>,
    Query(query): Query<CanvasGitQuery>,
) -> Result<Json<GitChangesResponse>, AppError> {
    let project_dir = canvas_git_project_dir(
        state.db_path.as_ref(),
        &canvas_id,
        user.user_id.as_deref(),
        query.path.as_deref(),
    )?;
    let response = tokio::task::spawn_blocking(move || canvas_changes_at(&project_dir))
        .await
        .map_err(|error| AppError::Internal(format!("Canvas changes task failed: {error}")))??;
    Ok(Json(response))
}

async fn get_canvas_file_diff(
    State(state): State<AppState>,
    CurrentUser(user): CurrentUser,
    AxumPath(canvas_id): AxumPath<String>,
    Query(query): Query<CanvasGitDiffQuery>,
) -> Result<Json<GitFileDiffResponse>, AppError> {
    let project_dir = canvas_git_project_dir(
        state.db_path.as_ref(),
        &canvas_id,
        user.user_id.as_deref(),
        query.path.as_deref(),
    )?;
    let file = query.file;
    let response = tokio::task::spawn_blocking(move || canvas_file_diff_at(&project_dir, &file))
        .await
        .map_err(|error| AppError::Internal(format!("Canvas file diff task failed: {error}")))??;
    Ok(Json(response))
}

fn canvas_changes_at(project_dir: &Path) -> Result<GitChangesResponse, AppError> {
    let changes = mitsuro_core::git::project_changes(project_dir).map_err(to_bad_request)?;
    let Some(changes) = changes else {
        return Ok(GitChangesResponse {
            in_repo: false,
            repo_root: None,
            files: Vec::new(),
            truncated: false,
        });
    };

    Ok(GitChangesResponse {
        in_repo: true,
        repo_root: Some(changes.repo_root.display().to_string()),
        files: changes
            .files
            .into_iter()
            .map(|file| GitChangedFileResponse {
                path: file.path,
                status: file.status,
                additions: file.additions,
                deletions: file.deletions,
            })
            .collect(),
        truncated: changes.truncated,
    })
}

fn canvas_file_diff_at(project_dir: &Path, file: &str) -> Result<GitFileDiffResponse, AppError> {
    let diff = mitsuro_core::git::project_file_diff(project_dir, file)
        .map_err(to_bad_request)?
        .ok_or_else(|| AppError::BadRequest("Path is not inside a git repository".to_string()))?;
    Ok(GitFileDiffResponse {
        path: diff.path,
        patch: diff.patch,
        truncated: diff.truncated,
        binary: diff.binary,
    })
}

fn to_bad_request(error: anyhow::Error) -> AppError {
    AppError::BadRequest(error.to_string())
}

fn canvas_git_project_dir(
    db_path: &Path,
    canvas_id: &str,
    owner_user_id: Option<&str>,
    requested_path: Option<&str>,
) -> Result<PathBuf, AppError> {
    let stored = super::canvas_files::owned_canvas_project_dir(db_path, canvas_id, owner_user_id)?;
    if requested_path.is_some() {
        return Err(AppError::BadRequest(
            "Canvas Git project path is server-owned".to_string(),
        ));
    }
    let canonical = std::fs::canonicalize(&stored)
        .map_err(|error| AppError::BadRequest(format!("Canvas project is unavailable: {error}")))?;
    if canonical != stored {
        return Err(AppError::BadRequest(
            "Canvas project no longer resolves to its canonical path".to_string(),
        ));
    }
    Ok(canonical)
}

#[cfg(test)]
mod tests {
    use super::*;
    use mitsuro_core::storage::{
        CreateCanvasInput, Database, SpatialCanvasStore, SpatialCanvasSurface,
    };
    use std::process::Command;

    fn run_git(repo: &Path, args: &[&str]) {
        let status = Command::new("git")
            .args(args)
            .current_dir(repo)
            .status()
            .expect("run git");
        assert!(status.success(), "git {args:?} failed");
    }

    fn create_owned_canvas(
        db_path: &Path,
        project_dir: &Path,
        owner_user_id: Option<&str>,
    ) -> String {
        let database = Database::new(db_path).expect("database");
        if let Some(owner_user_id) = owner_user_id {
            database
                .conn()
                .execute(
                    "INSERT OR IGNORE INTO users (id, email, license_tier) VALUES (?1, ?2, ?3)",
                    (
                        owner_user_id,
                        format!("{owner_user_id}@example.com"),
                        "free",
                    ),
                )
                .expect("canvas owner");
        }
        SpatialCanvasStore::new(database)
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

    #[test]
    fn canvas_git_root_requires_exact_owner_and_rejects_path_forgery() {
        let temp = tempfile::tempdir().expect("temp");
        let project = temp.path().join("project");
        std::fs::create_dir(&project).expect("project");
        let project = std::fs::canonicalize(project).expect("canonical project");
        let db_path = temp.path().join("mitsuro.db");
        let canvas_id = create_owned_canvas(&db_path, &project, Some("alice"));

        assert_eq!(
            canvas_git_project_dir(&db_path, &canvas_id, Some("alice"), None)
                .expect("owner project"),
            project
        );
        assert!(matches!(
            canvas_git_project_dir(&db_path, &canvas_id, Some("bob"), None),
            Err(AppError::NotFound(_))
        ));
        assert!(matches!(
            canvas_git_project_dir(&db_path, &canvas_id, Some("bob"), Some("/tmp/forged")),
            Err(AppError::NotFound(_))
        ));
        assert!(matches!(
            canvas_git_project_dir(&db_path, &canvas_id, None, None),
            Err(AppError::NotFound(_))
        ));
        assert!(matches!(
            canvas_git_project_dir(&db_path, &canvas_id, Some("alice"), Some("/tmp/forged")),
            Err(AppError::BadRequest(_))
        ));
    }

    #[test]
    fn canvas_git_projects_an_enclosing_repo_without_exposing_siblings() {
        let temp = tempfile::tempdir().expect("temp");
        let repo = temp.path().join("repo");
        let project = repo.join("project");
        let sibling = repo.join("sibling");
        std::fs::create_dir_all(&project).expect("project");
        std::fs::create_dir_all(&sibling).expect("sibling");
        run_git(&repo, &["init", "-b", "main"]);
        run_git(&repo, &["config", "user.name", "Mitsuro Test"]);
        run_git(
            &repo,
            &["config", "user.email", "mitsuro-test@example.invalid"],
        );
        std::fs::write(project.join("inside.txt"), "base inside\n").expect("inside base");
        std::fs::write(sibling.join("outside.txt"), "base outside\n").expect("outside base");
        run_git(&repo, &["add", "."]);
        run_git(&repo, &["commit", "-m", "base"]);

        std::fs::write(project.join("inside.txt"), "base inside\ncanvas change\n")
            .expect("inside change");
        std::fs::write(project.join("new.txt"), "canvas only\n").expect("inside untracked");
        std::fs::write(
            sibling.join("outside.txt"),
            "base outside\nsecret sibling\n",
        )
        .expect("outside change");
        std::fs::write(sibling.join("new-outside.txt"), "secret untracked\n")
            .expect("outside untracked");

        let project = std::fs::canonicalize(project).expect("canonical project");
        let db_path = temp.path().join("mitsuro.db");
        let canvas_id = create_owned_canvas(&db_path, &project, Some("alice"));
        let owned_project = canvas_git_project_dir(&db_path, &canvas_id, Some("alice"), None)
            .expect("owner project");

        let changes = canvas_changes_at(&owned_project).expect("scoped changes");
        assert!(changes.in_repo);
        assert_eq!(changes.repo_root.as_deref(), project.to_str());
        assert_eq!(
            changes
                .files
                .iter()
                .map(|file| file.path.as_str())
                .collect::<Vec<_>>(),
            vec!["inside.txt", "new.txt"]
        );

        let tracked = canvas_file_diff_at(&owned_project, "inside.txt").expect("inside diff");
        assert_eq!(tracked.path, "inside.txt");
        assert!(tracked
            .patch
            .contains("diff --git a/inside.txt b/inside.txt"));
        assert!(tracked.patch.contains("+canvas change"));
        assert!(!tracked.patch.contains("project/inside.txt"));
        assert!(!tracked.patch.contains("secret sibling"));

        let untracked = canvas_file_diff_at(&owned_project, "new.txt").expect("new diff");
        assert_eq!(untracked.path, "new.txt");
        assert!(untracked.patch.contains("diff --git a/new.txt b/new.txt"));

        for forged in [
            "../sibling/outside.txt",
            "/sibling/outside.txt",
            "sibling/outside.txt",
        ] {
            assert!(matches!(
                canvas_file_diff_at(&owned_project, forged),
                Err(AppError::BadRequest(_))
            ));
        }
    }

    #[cfg(unix)]
    #[test]
    fn canvas_git_root_rejects_a_replaced_project_symlink() {
        let temp = tempfile::tempdir().expect("temp");
        let project = temp.path().join("project");
        let outside = temp.path().join("outside");
        std::fs::create_dir(&project).expect("project");
        std::fs::create_dir(&outside).expect("outside");
        let project = std::fs::canonicalize(project).expect("canonical project");
        let db_path = temp.path().join("mitsuro.db");
        let canvas_id = create_owned_canvas(&db_path, &project, Some("alice"));

        std::fs::remove_dir(&project).expect("remove original project");
        std::os::unix::fs::symlink(&outside, &project).expect("replace with symlink");

        assert!(matches!(
            canvas_git_project_dir(&db_path, &canvas_id, Some("alice"), None),
            Err(AppError::BadRequest(_))
        ));
    }
}

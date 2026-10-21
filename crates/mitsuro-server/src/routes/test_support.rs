//! Shared test fixtures for route handlers.

use crate::auth::{AuthenticatedUser, CurrentUser};

/// Build an authenticated test user with an explicit identity and home dir.
pub(crate) fn current_user(user_id: &str, home_dir: &std::path::Path) -> CurrentUser {
    CurrentUser(AuthenticatedUser {
        user_id: Some(user_id.to_string()),
        home_dir: Some(home_dir.to_path_buf()),
    })
}

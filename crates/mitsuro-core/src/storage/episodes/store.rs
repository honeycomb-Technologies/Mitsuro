use anyhow::{Context, Result};
use rusqlite::{params, OptionalExtension};
use sha2::{Digest, Sha256};

use crate::ai::types::Content;
use crate::storage::database::Database;

use super::{ConversationEpisode, EpisodeSearch};

const MAX_EPISODE_BYTES: usize = 16 * 1024;
const MAX_SEARCH_LIMIT: usize = 100;

pub struct EpisodeStore<'a> {
    db: &'a Database,
}

impl<'a> EpisodeStore<'a> {
    pub fn new(db: &'a Database) -> Self {
        Self { db }
    }

    /// Index a canonical user/assistant message. Non-text blocks, pending
    /// steering, tools, thinking, and malformed content are ignored.
    pub fn record_message(
        &self,
        session_id: &str,
        source_message_id: i64,
        role: &str,
        content_json: &str,
        occurred_at: &str,
    ) -> Result<Option<i64>> {
        let Some(body) = episode_body(role, content_json) else {
            return Ok(None);
        };
        let content_hash = content_hash(role, &body);

        self.db.conn().execute(
            "INSERT INTO conversation_episodes (
                session_id, source_message_id, role, body, content_hash, occurred_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(session_id, source_message_id) DO UPDATE SET
                role = excluded.role,
                body = excluded.body,
                content_hash = excluded.content_hash,
                occurred_at = excluded.occurred_at",
            params![
                session_id,
                source_message_id,
                role,
                body,
                content_hash,
                occurred_at
            ],
        )?;

        let id = self.db.conn().query_row(
            "SELECT id FROM conversation_episodes
             WHERE session_id = ?1 AND source_message_id = ?2",
            params![session_id, source_message_id],
            |row| row.get(0),
        )?;
        Ok(Some(id))
    }

    /// Backfill legacy canonical messages in bounded batches. Repeated calls
    /// are idempotent because `(session_id, source_message_id)` is unique.
    pub fn backfill(&self, after_message_id: i64, limit: usize) -> Result<(usize, Option<i64>)> {
        let limit = limit.clamp(1, 1_000) as i64;
        let rows = {
            let mut statement = self.db.conn().prepare(
                "SELECT id, session_id, role, content, created_at
                 FROM messages
                 WHERE id > ?1
                   AND role IN ('user', 'assistant')
                 ORDER BY id
                 LIMIT ?2",
            )?;
            let rows = statement
                .query_map(params![after_message_id, limit], |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                    ))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            rows
        };

        let mut indexed = 0;
        let mut high_water = None;
        for (message_id, session_id, role, content, created_at) in rows {
            high_water = Some(message_id);
            if self
                .record_message(&session_id, message_id, &role, &content, &created_at)?
                .is_some()
            {
                indexed += 1;
            }
        }
        Ok((indexed, high_water))
    }

    /// Search only episodes belonging to the exact ownership scope. Local
    /// mode (`None`) intentionally excludes authenticated users' sessions.
    pub fn search(&self, search: &EpisodeSearch<'_>) -> Result<Vec<ConversationEpisode>> {
        let Some(fts_query) = safe_fts_query(search.query) else {
            return Ok(Vec::new());
        };
        let limit = search.limit.clamp(1, MAX_SEARCH_LIMIT) as i64;
        let mut statement = self.db.conn().prepare(
            "SELECT e.id, e.session_id, e.source_message_id, e.role, e.body,
                    e.content_hash, e.occurred_at, s.title, s.project_dir
             FROM conversation_episodes_fts f
             JOIN conversation_episodes e ON e.id = f.rowid
             JOIN sessions s ON s.id = e.session_id
             WHERE conversation_episodes_fts MATCH ?1
               AND ((?2 IS NULL AND s.user_id IS NULL) OR s.user_id = ?2)
               AND (?3 IS NULL OR s.project_dir = ?3)
               AND (?4 IS NULL OR s.session_type = ?4)
               AND (?5 IS NULL OR e.session_id = ?5)
               AND (
                   (
                       ?6 IS NOT NULL
                       AND (
                           EXISTS (
                               SELECT 1 FROM hive_workers worker
                               WHERE worker.id = ?6
                                 AND worker.dm_session_id = e.session_id
                           )
                           OR EXISTS (
                               SELECT 1 FROM hive_group_worker_lanes lane
                               WHERE lane.worker_id = ?6
                                 AND lane.session_id = e.session_id
                           )
                       )
                   )
                   OR (
                       ?6 IS NULL
                       AND (
                           ?7 != 0
                           OR (
                               NOT EXISTS (
                                   SELECT 1 FROM hive_workers worker
                                   WHERE worker.dm_session_id = e.session_id
                               )
                               AND NOT EXISTS (
                                   SELECT 1 FROM hive_group_worker_lanes lane
                                   WHERE lane.session_id = e.session_id
                               )
                           )
                       )
                   )
               )
             ORDER BY bm25(conversation_episodes_fts), e.occurred_at DESC
             LIMIT ?8",
        )?;
        let episodes = statement
            .query_map(
                params![
                    fts_query,
                    search.user_id,
                    search.project_dir,
                    search.session_type,
                    search.session_id,
                    search.worker_id,
                    if search.include_worker_sessions {
                        1_i64
                    } else {
                        0_i64
                    },
                    limit
                ],
                |row| {
                    Ok(ConversationEpisode {
                        id: row.get(0)?,
                        session_id: row.get(1)?,
                        source_message_id: row.get(2)?,
                        role: row.get(3)?,
                        body: row.get(4)?,
                        content_hash: row.get(5)?,
                        occurred_at: row.get(6)?,
                        session_title: row.get(7)?,
                        project_dir: row.get(8)?,
                    })
                },
            )?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(episodes)
    }

    pub fn get_owned(
        &self,
        episode_id: i64,
        user_id: Option<&str>,
    ) -> Result<Option<ConversationEpisode>> {
        self.db
            .conn()
            .query_row(
                "SELECT e.id, e.session_id, e.source_message_id, e.role, e.body,
                        e.content_hash, e.occurred_at, s.title, s.project_dir
                 FROM conversation_episodes e
                 JOIN sessions s ON s.id = e.session_id
                 WHERE e.id = ?1
                   AND ((?2 IS NULL AND s.user_id IS NULL) OR s.user_id = ?2)",
                params![episode_id, user_id],
                |row| {
                    Ok(ConversationEpisode {
                        id: row.get(0)?,
                        session_id: row.get(1)?,
                        source_message_id: row.get(2)?,
                        role: row.get(3)?,
                        body: row.get(4)?,
                        content_hash: row.get(5)?,
                        occurred_at: row.get(6)?,
                        session_title: row.get(7)?,
                        project_dir: row.get(8)?,
                    })
                },
            )
            .optional()
            .context("load owned conversation episode")
    }
}

fn episode_body(role: &str, content_json: &str) -> Option<String> {
    if !matches!(role, "user" | "assistant") {
        return None;
    }
    let content = serde_json::from_str::<Vec<Content>>(content_json).ok()?;
    let joined = content
        .into_iter()
        .filter_map(|block| match block {
            Content::Text { text, .. } => Some(text),
            _ => None,
        })
        .map(|text| text.split_whitespace().collect::<Vec<_>>().join(" "))
        .filter(|text| !text.is_empty())
        .collect::<Vec<_>>()
        .join("\n");
    if joined.is_empty() {
        return None;
    }

    Some(truncate_utf8_bytes(&joined, MAX_EPISODE_BYTES))
}

fn content_hash(role: &str, body: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(role.as_bytes());
    hasher.update([0]);
    hasher.update(body.as_bytes());
    format!("{:x}", hasher.finalize())
}

fn safe_fts_query(query: &str) -> Option<String> {
    let mut terms = query
        .split(|character: char| !character.is_alphanumeric() && character != '_')
        .map(str::trim)
        .filter(|term| term.chars().count() >= 2)
        .map(|term| format!("\"{}\"", term.replace('"', "\"\"")))
        .take(12)
        .collect::<Vec<_>>();
    terms.dedup();
    (!terms.is_empty()).then(|| terms.join(" OR "))
}

fn truncate_utf8_bytes(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_string();
    }
    let mut end = max_bytes;
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    value[..end].to_string()
}

#[cfg(test)]
mod private_tests {
    use super::{episode_body, safe_fts_query};

    #[test]
    fn extracts_only_text_content() {
        let json = r#"[{"type":"thinking","thinking":"secret","signature":"x"},{"type":"text","text":"  hello   world  "},{"type":"tool_result","tool_use_id":"1","output":"raw"}]"#;
        assert_eq!(
            episode_body("assistant", json).as_deref(),
            Some("hello world")
        );
        assert!(episode_body("tool", json).is_none());
        assert!(episode_body("pending_user:1", json).is_none());
    }

    #[test]
    fn builds_literal_bounded_fts_query() {
        assert_eq!(
            safe_fts_query("scheduler OR profile").as_deref(),
            Some("\"scheduler\" OR \"OR\" OR \"profile\"")
        );
        assert!(safe_fts_query("!").is_none());
    }
}

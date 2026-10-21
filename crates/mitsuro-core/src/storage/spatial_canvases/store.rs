use std::collections::HashSet;
use std::str::FromStr;
use std::time::Duration;

use anyhow::{anyhow, ensure, Context, Result};
use chrono::Utc;
use rusqlite::{params, types::Type, OptionalExtension, Transaction, TransactionBehavior};
use serde::Deserialize;
use uuid::Uuid;

use super::model::{
    CanvasEvent, CanvasEventPage, CanvasNode, CanvasNodeBinding, CanvasOperation, CanvasPatchInput,
    CanvasResourceCreateIntent, CanvasResourceCreateStatus, CanvasResourceKind, CanvasState,
    CreateCanvasInput, ProjectCandidate, ReserveCanvasResourceCreateInput, SpatialCanvas,
    SpatialCanvasSurface, WorldRect, SPATIAL_CANVAS_SCHEMA_VERSION,
};
use crate::storage::{database::Database, hash_request_bytes};

const MAX_CANVAS_NODES: usize = 256;
const MAX_PATCH_OPERATIONS: usize = 256;
const MAX_RETAINED_EVENTS: u64 = 2_048;
const MAX_EVENT_PAGE: usize = 256;
const MAX_ID_BYTES: usize = 256;
const MAX_TITLE_BYTES: usize = 512;
const MAX_THEME_ID_BYTES: usize = 128;
const MAX_STATE_JSON_BYTES: usize = 2 * 1024 * 1024;
// Browser launch plus stream discovery may consume two 45-second command
// windows. Keep the automatic stale bound comfortably above every current
// provisioner; per-claim tokens still fence a genuinely stale worker.
const RESOURCE_CREATE_CLAIM_STALE_AFTER: Duration = Duration::from_secs(300);

#[derive(Debug, Clone, PartialEq)]
pub struct CreateCanvasOutcome {
    pub canvas: SpatialCanvas,
    pub created: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PatchCanvasOutcome {
    pub canvas: SpatialCanvas,
    pub applied: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReserveCanvasResourceCreateOutcome {
    pub intent: CanvasResourceCreateIntent,
    pub created: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CanvasResourceCreateClaim {
    Acquired(CanvasResourceCreateIntent),
    Busy(CanvasResourceCreateIntent),
    Completed(CanvasResourceCreateIntent),
    Ambiguous(CanvasResourceCreateIntent),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CanvasResourceCleanupClaim {
    BoundRequiresAuthorization(CanvasResourceCreateIntent),
    Acquired {
        intent: CanvasResourceCreateIntent,
        prior_status: CanvasResourceCreateStatus,
        /// A missing in-memory external resource is only proof of absence when
        /// this process previously owned the authoritative lifecycle, or the
        /// create intent was never provisioned at all.
        absence_is_authoritative: bool,
    },
    Busy(CanvasResourceCreateIntent),
    Completed(CanvasResourceCreateIntent),
}

pub struct SpatialCanvasStore {
    db: Database,
}

impl SpatialCanvasStore {
    pub fn new(db: Database) -> Self {
        Self { db }
    }

    pub fn create_for_exact_owner(
        &self,
        owner_user_id: Option<&str>,
        mut input: CreateCanvasInput,
    ) -> Result<CreateCanvasOutcome> {
        input.name = input.name.trim().to_string();
        input.shared_theme_id = input.shared_theme_id.trim().to_string();
        validate_create_input(&input)?;
        let create_request_json =
            serde_json::to_string(&input).context("serializing canvas create request")?;
        let tx = Transaction::new_unchecked(self.db.conn(), TransactionBehavior::Immediate)
            .context("acquiring spatial canvas creation lock")?;

        if let Some((existing, existing_request_json)) =
            load_by_create_op(&tx, owner_user_id, &input.create_op_id)?
        {
            ensure!(
                existing_request_json == create_request_json,
                "canvas create operation was reused with different input"
            );
            tx.commit()?;
            return Ok(CreateCanvasOutcome {
                canvas: existing,
                created: false,
            });
        }

        let id = Uuid::new_v4().to_string();
        let now = Utc::now().to_rfc3339();
        let state = CanvasState::default();
        let state_json = serde_json::to_string(&state).context("serializing empty canvas state")?;
        tx.execute(
            "INSERT INTO spatial_canvases (
                 id, owner_user_id, project_dir, surface, name, schema_version,
                 revision, shared_theme_id, state_json, create_op_id, create_request_json,
                 created_at, updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 0, ?7, ?8, ?9, ?10, ?11, ?11)",
            params![
                id,
                owner_user_id,
                input.project_dir,
                input.surface.to_string(),
                input.name,
                SPATIAL_CANVAS_SCHEMA_VERSION,
                input.shared_theme_id,
                state_json,
                input.create_op_id,
                create_request_json,
                now,
            ],
        )
        .map_err(|error| map_canvas_insert_error(error, "creating spatial canvas"))?;
        tx.execute(
            "INSERT INTO spatial_canvas_events (
                 canvas_id, sequence, op_id, client_id, operations_json, created_at
             ) VALUES (?1, 0, ?2, 'server', '[]', ?3)",
            params![id, input.create_op_id, now],
        )?;
        let canvas = load_owned_canvas(&tx, &id, owner_user_id)?
            .ok_or_else(|| anyhow!("created spatial canvas could not be reloaded"))?;
        tx.commit()?;
        Ok(CreateCanvasOutcome {
            canvas,
            created: true,
        })
    }

    /// Return a previously completed create operation without consulting the
    /// current filesystem. Callers use this before revalidating a project path
    /// so a network retry remains idempotent after that project becomes
    /// temporarily unavailable.
    pub fn create_replay_for_exact_owner(
        &self,
        owner_user_id: Option<&str>,
        create_op_id: &str,
    ) -> Result<Option<(CreateCanvasOutcome, CreateCanvasInput)>> {
        validate_identifier("canvas create operation", create_op_id)?;
        let Some((canvas, request_json)) =
            load_by_create_op(self.db.conn(), owner_user_id, create_op_id)?
        else {
            return Ok(None);
        };
        let request = serde_json::from_str(&request_json)
            .context("deserializing stored canvas create request")?;
        Ok(Some((
            CreateCanvasOutcome {
                canvas,
                created: false,
            },
            request,
        )))
    }

    pub fn list_for_exact_owner(
        &self,
        owner_user_id: Option<&str>,
        surface: Option<SpatialCanvasSurface>,
    ) -> Result<Vec<SpatialCanvas>> {
        let mut statement = self.db.conn().prepare(
            "SELECT id, owner_user_id, project_dir, surface, name, schema_version,
                    revision, shared_theme_id, state_json, created_at, updated_at, archived_at
             FROM spatial_canvases
             WHERE owner_user_id IS ?1
               AND archived_at IS NULL
               AND (?2 IS NULL OR surface = ?2)
             ORDER BY updated_at DESC, id ASC",
        )?;
        let surface = surface.map(|surface| surface.to_string());
        let rows = statement.query_map(params![owner_user_id, surface], row_to_canvas)?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .context("listing spatial canvases for exact owner")
    }

    pub fn get_for_exact_owner(
        &self,
        canvas_id: &str,
        owner_user_id: Option<&str>,
    ) -> Result<Option<SpatialCanvas>> {
        load_owned_canvas(self.db.conn(), canvas_id, owner_user_id)
    }

    pub fn patch_for_exact_owner(
        &self,
        canvas_id: &str,
        owner_user_id: Option<&str>,
        input: CanvasPatchInput,
    ) -> Result<PatchCanvasOutcome> {
        validate_patch_input(&input)?;
        ensure!(
            !input.op_id.starts_with("resource-cleanup:")
                && !input.op_id.starts_with("resource-detach:"),
            "canvas operation id uses a server-only resource lifecycle namespace"
        );
        let tx = Transaction::new_unchecked(self.db.conn(), TransactionBehavior::Immediate)
            .context("acquiring spatial canvas mutation lock")?;
        let mut canvas = load_owned_canvas(&tx, canvas_id, owner_user_id)?
            .ok_or_else(|| anyhow!("spatial canvas not found"))?;
        let operations_json =
            serde_json::to_string(&input.operations).context("serializing canvas operations")?;
        let already_applied = tx
            .query_row(
                "SELECT client_id, operations_json FROM spatial_canvas_events
                 WHERE canvas_id = ?1 AND op_id = ?2",
                params![canvas_id, input.op_id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?;
        if let Some((existing_client_id, existing_operations_json)) = already_applied {
            ensure!(
                existing_client_id == input.client_id
                    && existing_operations_json == operations_json,
                "canvas operation id was reused with different input"
            );
            tx.commit()?;
            return Ok(PatchCanvasOutcome {
                canvas,
                applied: false,
            });
        }

        ensure!(
            canvas.revision == input.expected_revision,
            "canvas revision conflict: expected {}, current {}",
            input.expected_revision,
            canvas.revision
        );
        validate_resource_recovery_before_archive(
            &tx,
            canvas_id,
            owner_user_id,
            &input.operations,
        )?;
        validate_resource_node_reservations(&tx, canvas_id, &input)?;
        validate_persisted_bindings(&tx, &canvas, owner_user_id, &input.operations, None)?;
        let now = Utc::now().to_rfc3339();
        let touched_node_ids = input
            .operations
            .iter()
            .filter_map(|operation| match operation {
                CanvasOperation::UpsertNode { node } => Some(node.id.clone()),
                CanvasOperation::RemoveNode { node_id } => Some(node_id.clone()),
                _ => None,
            })
            .collect::<HashSet<_>>();
        apply_operations(&mut canvas, &input.operations, &now)?;
        for node_id in touched_node_ids {
            let exact_agent_binding_remains = canvas.state.nodes.iter().any(|node| {
                node.id == node_id
                    && matches!(
                        &node.binding,
                        CanvasNodeBinding::Agent { session_id } if session_id == &node_id
                    )
            });
            if !exact_agent_binding_remains {
                tx.execute(
                    "UPDATE spatial_canvas_resource_creates
                     SET request_json = json_remove(request_json, '$.initial_prompt'),
                         updated_at = ?1
                     WHERE canvas_id = ?2 AND owner_user_id IS ?3 AND node_id = ?4
                       AND status = 'bound' AND resource_kind = 'agent'",
                    params![now, canvas_id, owner_user_id, node_id],
                )?;
            }
        }
        validate_canvas_state(&canvas.state)?;
        validate_canvas_capacity_with_reservations(&tx, canvas_id, &canvas.state)?;
        let next_revision = canvas
            .revision
            .checked_add(1)
            .ok_or_else(|| anyhow!("canvas revision overflow"))?;
        let state_json =
            serde_json::to_string(&canvas.state).context("serializing canvas state")?;
        ensure!(
            state_json.len() <= MAX_STATE_JSON_BYTES,
            "canvas state exceeds the supported size"
        );
        let updated = tx
            .execute(
                "UPDATE spatial_canvases
             SET revision = ?1, name = ?2, shared_theme_id = ?3, state_json = ?4,
                 archived_at = ?5, updated_at = ?6
             WHERE id = ?7 AND owner_user_id IS ?8 AND revision = ?9",
                params![
                    next_revision,
                    canvas.name,
                    canvas.shared_theme_id,
                    state_json,
                    canvas.archived_at,
                    now,
                    canvas_id,
                    owner_user_id,
                    input.expected_revision,
                ],
            )
            .map_err(|error| map_canvas_insert_error(error, "updating spatial canvas"))?;
        ensure!(updated == 1, "canvas revision changed during mutation");
        tx.execute(
            "INSERT INTO spatial_canvas_events (
                 canvas_id, sequence, op_id, client_id, operations_json, created_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                canvas_id,
                next_revision,
                input.op_id,
                input.client_id,
                operations_json,
                now,
            ],
        )?;
        tx.execute(
            "DELETE FROM spatial_canvas_events
             WHERE canvas_id = ?1 AND sequence < ?2",
            params![
                canvas_id,
                next_revision.saturating_sub(MAX_RETAINED_EVENTS.saturating_sub(1))
            ],
        )?;
        canvas.revision = next_revision;
        canvas.updated_at = now;
        tx.commit()?;
        Ok(PatchCanvasOutcome {
            canvas,
            applied: true,
        })
    }

    /// Return a completed patch operation before callers revalidate mutable
    /// Agent, Terminal, or Browser bindings.
    pub fn patch_replay_for_exact_owner(
        &self,
        canvas_id: &str,
        owner_user_id: Option<&str>,
        input: &CanvasPatchInput,
    ) -> Result<Option<PatchCanvasOutcome>> {
        validate_patch_input(input)?;
        let canvas = load_owned_canvas(self.db.conn(), canvas_id, owner_user_id)?
            .ok_or_else(|| anyhow!("spatial canvas not found"))?;
        let operations_json =
            serde_json::to_string(&input.operations).context("serializing canvas operations")?;
        let already_applied = self
            .db
            .conn()
            .query_row(
                "SELECT event.client_id, event.operations_json
                 FROM spatial_canvas_events event
                 JOIN spatial_canvases canvas ON canvas.id = event.canvas_id
                 WHERE event.canvas_id = ?1
                   AND event.op_id = ?2
                   AND canvas.owner_user_id IS ?3",
                params![canvas_id, input.op_id, owner_user_id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?;
        let Some((existing_client_id, existing_operations_json)) = already_applied else {
            return Ok(None);
        };
        ensure!(
            existing_client_id == input.client_id && existing_operations_json == operations_json,
            "canvas operation id was reused with different input"
        );
        Ok(Some(PatchCanvasOutcome {
            canvas,
            applied: false,
        }))
    }

    pub fn list_events_after_for_exact_owner(
        &self,
        canvas_id: &str,
        owner_user_id: Option<&str>,
        after_sequence: u64,
        limit: usize,
    ) -> Result<Option<CanvasEventPage>> {
        let Some(canvas) = self.get_for_exact_owner(canvas_id, owner_user_id)? else {
            return Ok(None);
        };
        let earliest_sequence = self.db.conn().query_row(
            "SELECT MIN(event.sequence)
                 FROM spatial_canvas_events event
                 JOIN spatial_canvases canvas ON canvas.id = event.canvas_id
                 WHERE event.canvas_id = ?1 AND canvas.owner_user_id IS ?2",
            params![canvas_id, owner_user_id],
            |row| row.get::<_, Option<u64>>(0),
        )?;
        let gap = after_sequence > canvas.revision
            || earliest_sequence
                .is_some_and(|earliest| after_sequence.saturating_add(1) < earliest);
        let mut statement = self.db.conn().prepare(
            "SELECT event.canvas_id, event.sequence, event.op_id, event.client_id,
                    event.operations_json, event.created_at
             FROM spatial_canvas_events event
             JOIN spatial_canvases canvas ON canvas.id = event.canvas_id
             WHERE event.canvas_id = ?1
               AND canvas.owner_user_id IS ?2
               AND event.sequence > ?3
             ORDER BY event.sequence ASC
             LIMIT ?4",
        )?;
        let limit = limit.clamp(1, MAX_EVENT_PAGE) as u64;
        let events = statement
            .query_map(
                params![canvas_id, owner_user_id, after_sequence, limit],
                row_to_event,
            )?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(Some(CanvasEventPage {
            events,
            earliest_sequence,
            latest_sequence: canvas.revision,
            gap,
        }))
    }

    pub fn list_project_candidates_for_exact_owner(
        &self,
        owner_user_id: Option<&str>,
        surface: SpatialCanvasSurface,
    ) -> Result<Vec<ProjectCandidate>> {
        let surface = surface.to_string();
        let mut statement = self.db.conn().prepare(
            "SELECT project_dir, MAX(updated_at) AS updated_at, SUM(canvas_count)
             FROM (
                 SELECT project_dir, updated_at, 1 AS canvas_count
                 FROM spatial_canvases
                 WHERE owner_user_id IS ?1 AND surface = ?2 AND archived_at IS NULL
                 UNION ALL
                 SELECT COALESCE(NULLIF(project_dir, ''), NULLIF(working_dir, '')),
                        updated_at, 0 AS canvas_count
                 FROM sessions
                 WHERE user_id IS ?1
                   AND session_type = ?2
                   AND archived_at IS NULL
                   AND COALESCE(NULLIF(project_dir, ''), NULLIF(working_dir, '')) IS NOT NULL
             )
             GROUP BY project_dir
             ORDER BY updated_at DESC, project_dir ASC",
        )?;
        let rows = statement.query_map(params![owner_user_id, surface], |row| {
            Ok(ProjectCandidate {
                project_dir: row.get(0)?,
                updated_at: row.get(1)?,
                canvas_count: row.get(2)?,
            })
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .context("listing project candidates for exact owner")
    }

    /// Reserve an exact node/resource create operation before any durable DB
    /// session or external runtime is started. Replays must carry byte-for-byte
    /// equivalent canonical input.
    pub fn reserve_resource_create_for_exact_owner(
        &self,
        canvas_id: &str,
        owner_user_id: Option<&str>,
        input: ReserveCanvasResourceCreateInput,
    ) -> Result<ReserveCanvasResourceCreateOutcome> {
        validate_resource_create_input(&input)?;
        let tx = Transaction::new_unchecked(self.db.conn(), TransactionBehavior::Immediate)
            .context("acquiring canvas resource create reservation lock")?;
        let canvas = load_owned_canvas(&tx, canvas_id, owner_user_id)?
            .ok_or_else(|| anyhow!("spatial canvas not found"))?;

        if let Some(existing) =
            load_resource_create_intent(&tx, canvas_id, owner_user_id, &input.create_op_id)?
        {
            ensure_resource_create_replay_matches(&existing, &input)?;
            tx.commit()?;
            return Ok(ReserveCanvasResourceCreateOutcome {
                intent: existing,
                created: false,
            });
        }

        ensure!(
            canvas.archived_at.is_none(),
            "cannot create a resource in an archived canvas"
        );

        ensure_resource_identity_is_unused(&tx, &input)?;

        ensure!(
            canvas
                .state
                .nodes
                .iter()
                .all(|node| node.id != input.node_id),
            "canvas resource node id already exists"
        );
        let bind_operation_exists: bool = tx.query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM spatial_canvas_events
                 WHERE canvas_id = ?1 AND op_id = ?2
             )",
            params![canvas_id, input.bind_op_id],
            |row| row.get(0),
        )?;
        ensure!(
            !bind_operation_exists,
            "canvas resource bind operation id was already used"
        );
        let reserved_slots = unfinished_resource_slots(&tx, canvas_id, &canvas.state)?;
        ensure!(
            canvas.state.nodes.len().saturating_add(reserved_slots) < MAX_CANVAS_NODES,
            "canvas has no unreserved node capacity for another resource"
        );
        let now = Utc::now().to_rfc3339();
        let request_fingerprint = hash_request_bytes(input.request_json.as_bytes());
        tx.execute(
            "INSERT INTO spatial_canvas_resource_creates (
                 canvas_id, owner_user_id, create_op_id, bind_op_id, client_id,
                 node_id, resource_kind, request_json, request_fingerprint,
                 status, created_at, updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 'reserved', ?10, ?10)",
            params![
                canvas_id,
                owner_user_id,
                input.create_op_id,
                input.bind_op_id,
                input.client_id,
                input.node_id,
                input.resource_kind.to_string(),
                input.request_json,
                request_fingerprint,
                now,
            ],
        )
        .map_err(|error| anyhow!(error).context("reserving canvas resource create intent"))?;
        let intent =
            load_resource_create_intent(&tx, canvas_id, owner_user_id, &input.create_op_id)?
                .ok_or_else(|| {
                    anyhow!("reserved canvas resource create intent could not be reloaded")
                })?;
        tx.commit()?;
        Ok(ReserveCanvasResourceCreateOutcome {
            intent,
            created: true,
        })
    }

    /// Atomically make cleanup win when the create POST has not reached the
    /// server yet. The terminal tombstone carries the immutable request
    /// fingerprint, so a delayed exact create replay cannot provision the
    /// resource and a changed replay still conflicts.
    pub fn reserve_resource_cleanup_tombstone_for_exact_owner(
        &self,
        canvas_id: &str,
        owner_user_id: Option<&str>,
        input: ReserveCanvasResourceCreateInput,
    ) -> Result<ReserveCanvasResourceCreateOutcome> {
        validate_resource_create_input(&input)?;
        let tx = Transaction::new_unchecked(self.db.conn(), TransactionBehavior::Immediate)
            .context("acquiring canvas resource cleanup reservation lock")?;
        load_owned_canvas(&tx, canvas_id, owner_user_id)?
            .ok_or_else(|| anyhow!("spatial canvas not found"))?;
        if let Some(existing) =
            load_resource_create_intent(&tx, canvas_id, owner_user_id, &input.create_op_id)?
        {
            ensure_resource_create_replay_matches(&existing, &input)?;
            tx.commit()?;
            return Ok(ReserveCanvasResourceCreateOutcome {
                intent: existing,
                created: false,
            });
        }

        ensure_resource_identity_is_unused(&tx, &input)?;

        let final_status = if input.resource_kind == CanvasResourceKind::Agent {
            CanvasResourceCreateStatus::Abandoned
        } else {
            CanvasResourceCreateStatus::Cleaned
        };
        let request_fingerprint = hash_request_bytes(input.request_json.as_bytes());
        let request_json = if input.resource_kind == CanvasResourceKind::Agent {
            let mut request: serde_json::Value = serde_json::from_str(&input.request_json)
                .context("parsing cleanup tombstone request")?;
            if let Some(object) = request.as_object_mut() {
                object.remove("initial_prompt");
            }
            serde_json::to_string(&request).context("serializing cleanup tombstone request")?
        } else {
            input.request_json
        };
        let now = Utc::now().to_rfc3339();
        tx.execute(
            "INSERT INTO spatial_canvas_resource_creates (
                 canvas_id, owner_user_id, create_op_id, bind_op_id, client_id,
                 node_id, resource_kind, request_json, request_fingerprint,
                 status, created_at, updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?11)",
            params![
                canvas_id,
                owner_user_id,
                input.create_op_id,
                input.bind_op_id,
                input.client_id,
                input.node_id,
                input.resource_kind.to_string(),
                request_json,
                request_fingerprint,
                final_status.to_string(),
                now,
            ],
        )
        .map_err(|error| anyhow!(error).context("reserving canvas resource cleanup tombstone"))?;
        let intent =
            load_resource_create_intent(&tx, canvas_id, owner_user_id, &input.create_op_id)?
                .ok_or_else(|| anyhow!("cleanup tombstone could not be reloaded"))?;
        tx.commit()?;
        Ok(ReserveCanvasResourceCreateOutcome {
            intent,
            created: true,
        })
    }

    pub fn get_resource_create_for_exact_owner(
        &self,
        canvas_id: &str,
        owner_user_id: Option<&str>,
        create_op_id: &str,
    ) -> Result<Option<CanvasResourceCreateIntent>> {
        validate_identifier("canvas resource create operation", create_op_id)?;
        load_resource_create_intent(self.db.conn(), canvas_id, owner_user_id, create_op_id)
    }

    pub fn list_unfinished_resource_creates_for_exact_owner(
        &self,
        canvas_id: &str,
        owner_user_id: Option<&str>,
    ) -> Result<Vec<CanvasResourceCreateIntent>> {
        let mut statement = self.db.conn().prepare(
            "SELECT intent.canvas_id, intent.owner_user_id, intent.create_op_id,
                    intent.bind_op_id, intent.client_id, intent.node_id,
                    intent.resource_kind, intent.request_json, intent.request_fingerprint,
                    intent.status,
                    intent.executor_id, intent.claim_token, intent.result_json, intent.last_error,
                    intent.created_at, intent.updated_at, intent.detach_generation
             FROM spatial_canvas_resource_creates intent
             JOIN spatial_canvases canvas ON canvas.id = intent.canvas_id
             WHERE intent.canvas_id = ?1
               AND canvas.owner_user_id IS ?2
               AND intent.owner_user_id IS ?2
               AND intent.status NOT IN ('bound', 'cleaned', 'abandoned')
             ORDER BY intent.created_at ASC, intent.create_op_id ASC",
        )?;
        let intents = statement
            .query_map(
                params![canvas_id, owner_user_id],
                row_to_resource_create_intent,
            )?
            .collect::<rusqlite::Result<Vec<_>>>()
            .context("listing unfinished canvas resource creates for exact owner")?;
        Ok(intents)
    }

    /// List unfinished work, Bound Agents whose initial prompt still needs
    /// durable chat completion, and Bound external resources so the server
    /// route can surface only those whose runtime authority is absent/ended.
    pub fn list_recoverable_resource_creates_for_exact_owner(
        &self,
        canvas_id: &str,
        owner_user_id: Option<&str>,
    ) -> Result<Vec<CanvasResourceCreateIntent>> {
        let mut statement = self.db.conn().prepare(
            "SELECT intent.canvas_id, intent.owner_user_id, intent.create_op_id,
                    intent.bind_op_id, intent.client_id, intent.node_id,
                    intent.resource_kind, intent.request_json, intent.request_fingerprint,
                    intent.status, intent.executor_id, intent.claim_token,
                    intent.result_json, intent.last_error, intent.created_at, intent.updated_at,
                    intent.detach_generation
             FROM spatial_canvas_resource_creates intent
             JOIN spatial_canvases canvas ON canvas.id = intent.canvas_id
             WHERE intent.canvas_id = ?1
               AND canvas.owner_user_id IS ?2
               AND intent.owner_user_id IS ?2
               AND (
                    intent.status NOT IN ('bound', 'cleaned', 'abandoned')
                    OR (
                        intent.status = 'bound'
                        AND intent.resource_kind = 'agent'
                        AND json_type(intent.request_json, '$.initial_prompt') = 'text'
                    )
                    OR (
                        intent.status = 'bound'
                        AND intent.resource_kind IN ('terminal', 'browser')
                        AND EXISTS(
                            SELECT 1
                            FROM spatial_canvases AS current_canvas,
                                 json_each(current_canvas.state_json, '$.nodes') AS node
                            WHERE current_canvas.id = intent.canvas_id
                              AND json_extract(node.value, '$.id') = intent.node_id
                              AND json_extract(node.value, '$.binding.kind') = intent.resource_kind
                              AND (
                                  (intent.resource_kind = 'terminal'
                                   AND json_extract(node.value, '$.binding.terminal_id') = intent.node_id)
                                  OR
                                  (intent.resource_kind = 'browser'
                                   AND json_extract(node.value, '$.binding.browser_session_id') = intent.node_id)
                              )
                        )
                    )
               )
             ORDER BY intent.created_at ASC, intent.create_op_id ASC",
        )?;
        let intents = statement
            .query_map(
                params![canvas_id, owner_user_id],
                row_to_resource_create_intent,
            )?
            .collect::<rusqlite::Result<Vec<_>>>()
            .context("listing recoverable canvas resource creates for exact owner")?;
        Ok(intents)
    }

    /// Claim one detached reconciliation attempt. A different executor means
    /// the server restarted. Agents can be safely re-ensured from SQLite;
    /// external PTY/Browser lifecycles become explicit `Ambiguous` unless the
    /// caller deliberately authorizes a retry.
    pub fn claim_resource_create_for_exact_owner(
        &self,
        canvas_id: &str,
        owner_user_id: Option<&str>,
        create_op_id: &str,
        executor_id: &str,
        retry_ambiguous: bool,
    ) -> Result<CanvasResourceCreateClaim> {
        self.claim_resource_create_for_exact_owner_with_stale_after(
            canvas_id,
            owner_user_id,
            create_op_id,
            executor_id,
            retry_ambiguous,
            RESOURCE_CREATE_CLAIM_STALE_AFTER,
        )
    }

    /// Variant with an explicit staleness bound for deterministic recovery
    /// tests and server watchdogs.
    pub fn claim_resource_create_for_exact_owner_with_stale_after(
        &self,
        canvas_id: &str,
        owner_user_id: Option<&str>,
        create_op_id: &str,
        executor_id: &str,
        retry_ambiguous: bool,
        stale_after: Duration,
    ) -> Result<CanvasResourceCreateClaim> {
        validate_identifier("canvas resource executor", executor_id)?;
        let tx = Transaction::new_unchecked(self.db.conn(), TransactionBehavior::Immediate)
            .context("acquiring canvas resource reconciliation claim")?;
        let mut intent = load_resource_create_intent(&tx, canvas_id, owner_user_id, create_op_id)?
            .ok_or_else(|| anyhow!("canvas resource create intent not found"))?;

        if intent.status == CanvasResourceCreateStatus::Bound {
            tx.commit()?;
            return Ok(CanvasResourceCreateClaim::Completed(intent));
        }
        ensure!(
            !matches!(
                intent.status,
                CanvasResourceCreateStatus::Cleaned | CanvasResourceCreateStatus::Abandoned
            ),
            "canvas resource create operation was permanently cleaned or abandoned"
        );
        if intent.status == CanvasResourceCreateStatus::Cleaning {
            tx.commit()?;
            return Ok(CanvasResourceCreateClaim::Busy(intent));
        }
        let claim_is_stale = resource_create_claim_is_stale(&intent, stale_after);
        if matches!(
            intent.status,
            CanvasResourceCreateStatus::Provisioning | CanvasResourceCreateStatus::Provisioned
        ) && intent.executor_id.as_deref() == Some(executor_id)
            && intent.claim_token.is_some()
            && !claim_is_stale
        {
            tx.commit()?;
            return Ok(CanvasResourceCreateClaim::Busy(intent));
        }

        let prior_executor_changed = intent
            .executor_id
            .as_deref()
            .is_some_and(|prior| prior != executor_id);
        let external_lifecycle_is_uncertain = matches!(
            intent.resource_kind,
            CanvasResourceKind::Terminal | CanvasResourceKind::Browser
        ) && (prior_executor_changed || claim_is_stale)
            && matches!(
                intent.status,
                CanvasResourceCreateStatus::Provisioning | CanvasResourceCreateStatus::Provisioned
            );
        if (intent.status == CanvasResourceCreateStatus::Ambiguous
            || external_lifecycle_is_uncertain)
            && !retry_ambiguous
        {
            let message = intent.last_error.clone().unwrap_or_else(|| {
                    "The previous server stopped while the external resource lifecycle was uncertain; explicit retry or cleanup is required".to_string()
                });
            intent.claim_token = None;
            let now = Utc::now().to_rfc3339();
            tx.execute(
                "UPDATE spatial_canvas_resource_creates
                     SET status = 'ambiguous', last_error = ?1, updated_at = ?2
                     WHERE canvas_id = ?3 AND create_op_id = ?4 AND owner_user_id IS ?5",
                params![message, now, canvas_id, create_op_id, owner_user_id],
            )?;
            intent.status = CanvasResourceCreateStatus::Ambiguous;
            intent.last_error = Some(message);
            intent.updated_at = now;
            tx.commit()?;
            return Ok(CanvasResourceCreateClaim::Ambiguous(intent));
        }

        let claim_token = Uuid::new_v4().to_string();
        let now = Utc::now().to_rfc3339();
        tx.execute(
            "UPDATE spatial_canvas_resource_creates
             SET status = 'provisioning', executor_id = ?1, claim_token = ?2,
                 result_json = NULL, last_error = NULL, updated_at = ?3
             WHERE canvas_id = ?4 AND create_op_id = ?5 AND owner_user_id IS ?6",
            params![
                executor_id,
                claim_token,
                now,
                canvas_id,
                create_op_id,
                owner_user_id
            ],
        )?;
        intent.status = CanvasResourceCreateStatus::Provisioning;
        intent.executor_id = Some(executor_id.to_string());
        intent.claim_token = Some(claim_token);
        intent.result_json = None;
        intent.last_error = None;
        intent.updated_at = now;
        tx.commit()?;
        Ok(CanvasResourceCreateClaim::Acquired(intent))
    }

    pub fn mark_resource_create_provisioned_for_exact_owner(
        &self,
        canvas_id: &str,
        owner_user_id: Option<&str>,
        create_op_id: &str,
        executor_id: &str,
        claim_token: &str,
        result_json: &str,
    ) -> Result<CanvasResourceCreateIntent> {
        validate_result_json(result_json)?;
        self.update_resource_create_phase(
            canvas_id,
            owner_user_id,
            create_op_id,
            executor_id,
            claim_token,
            CanvasResourceCreateStatus::Provisioned,
            Some(result_json),
            None,
        )
    }

    pub fn mark_resource_create_bound_for_exact_owner(
        &self,
        canvas_id: &str,
        owner_user_id: Option<&str>,
        create_op_id: &str,
        executor_id: &str,
        claim_token: &str,
    ) -> Result<CanvasResourceCreateIntent> {
        self.update_resource_create_phase(
            canvas_id,
            owner_user_id,
            create_op_id,
            executor_id,
            claim_token,
            CanvasResourceCreateStatus::Bound,
            None,
            None,
        )
    }

    /// Recover the final ledger phase after the Canvas bind patch committed but
    /// the former executor stopped before recording `Bound`. Callers must first
    /// prove that the persisted node has the exact reserved resource identity.
    pub fn recover_resource_create_bound_for_exact_owner(
        &self,
        canvas_id: &str,
        owner_user_id: Option<&str>,
        create_op_id: &str,
        executor_id: &str,
    ) -> Result<CanvasResourceCreateIntent> {
        validate_identifier("canvas resource executor", executor_id)?;
        let tx = Transaction::new_unchecked(self.db.conn(), TransactionBehavior::Immediate)
            .context("acquiring canvas resource bound recovery lock")?;
        load_resource_create_intent(&tx, canvas_id, owner_user_id, create_op_id)?
            .ok_or_else(|| anyhow!("canvas resource create intent not found"))?;
        let now = Utc::now().to_rfc3339();
        tx.execute(
            "UPDATE spatial_canvas_resource_creates
             SET status = 'bound', executor_id = ?1, last_error = NULL, updated_at = ?2
             WHERE canvas_id = ?3 AND create_op_id = ?4 AND owner_user_id IS ?5",
            params![executor_id, now, canvas_id, create_op_id, owner_user_id],
        )?;
        let intent = load_resource_create_intent(&tx, canvas_id, owner_user_id, create_op_id)?
            .ok_or_else(|| anyhow!("recovered canvas resource intent could not be reloaded"))?;
        tx.commit()?;
        Ok(intent)
    }

    /// Atomically materialize the exact node reserved by a provisioned
    /// resource intent and advance the ledger to `Bound` in the same SQLite
    /// transaction. Generic Canvas PATCH is never authorized to consume a
    /// resource reservation or its deterministic bind operation id.
    pub fn bind_resource_create_for_exact_owner(
        &self,
        canvas_id: &str,
        owner_user_id: Option<&str>,
        create_op_id: &str,
        executor_id: &str,
        claim_token: &str,
    ) -> Result<SpatialCanvas> {
        validate_identifier("canvas resource executor", executor_id)?;
        validate_identifier("canvas resource claim", claim_token)?;
        let tx = Transaction::new_unchecked(self.db.conn(), TransactionBehavior::Immediate)
            .context("acquiring atomic canvas resource bind lock")?;
        let intent = load_resource_create_intent(&tx, canvas_id, owner_user_id, create_op_id)?
            .ok_or_else(|| anyhow!("canvas resource create intent not found"))?;
        ensure!(
            intent.executor_id.as_deref() == Some(executor_id)
                && intent.claim_token.as_deref() == Some(claim_token),
            "canvas resource create claim changed"
        );
        ensure!(
            intent.status == CanvasResourceCreateStatus::Provisioned
                && intent.result_json.is_some(),
            "canvas resource must be provisioned before binding"
        );

        let node = reserved_resource_node(&intent)?;
        let mut canvas = load_owned_canvas(&tx, canvas_id, owner_user_id)?
            .ok_or_else(|| anyhow!("spatial canvas not found"))?;
        ensure!(
            canvas
                .state
                .nodes
                .iter()
                .all(|existing| existing.id != node.id),
            "reserved canvas resource node already exists before atomic bind"
        );
        let operations = vec![CanvasOperation::UpsertNode { node }];
        validate_persisted_bindings(
            &tx,
            &canvas,
            owner_user_id,
            &operations,
            Some((intent.resource_kind.to_string(), intent.node_id.clone())),
        )?;
        let operations_json =
            serde_json::to_string(&operations).context("serializing resource bind operation")?;
        let bind_operation_exists: bool = tx.query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM spatial_canvas_events
                 WHERE canvas_id = ?1 AND op_id = ?2
             )",
            params![canvas_id, intent.bind_op_id],
            |row| row.get(0),
        )?;
        ensure!(
            !bind_operation_exists,
            "canvas resource bind operation was already used"
        );

        let now = Utc::now().to_rfc3339();
        apply_operations(&mut canvas, &operations, &now)?;
        validate_canvas_state(&canvas.state)?;
        validate_canvas_capacity_with_reservations(&tx, canvas_id, &canvas.state)?;
        let next_revision = canvas
            .revision
            .checked_add(1)
            .ok_or_else(|| anyhow!("canvas revision overflow"))?;
        let state_json =
            serde_json::to_string(&canvas.state).context("serializing canvas state")?;
        ensure!(
            state_json.len() <= MAX_STATE_JSON_BYTES,
            "canvas state exceeds the supported size"
        );
        let canvas_updated = tx.execute(
            "UPDATE spatial_canvases
             SET revision = ?1, state_json = ?2, updated_at = ?3
             WHERE id = ?4 AND owner_user_id IS ?5 AND revision = ?6",
            params![
                next_revision,
                state_json,
                now,
                canvas_id,
                owner_user_id,
                canvas.revision,
            ],
        )?;
        ensure!(
            canvas_updated == 1,
            "canvas revision changed during resource bind"
        );
        tx.execute(
            "INSERT INTO spatial_canvas_events (
                 canvas_id, sequence, op_id, client_id, operations_json, created_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                canvas_id,
                next_revision,
                intent.bind_op_id,
                intent.client_id,
                operations_json,
                now,
            ],
        )?;
        tx.execute(
            "DELETE FROM spatial_canvas_events
             WHERE canvas_id = ?1 AND sequence < ?2",
            params![
                canvas_id,
                next_revision.saturating_sub(MAX_RETAINED_EVENTS.saturating_sub(1))
            ],
        )?;
        let intent_updated = tx.execute(
            "UPDATE spatial_canvas_resource_creates
             SET status = 'bound', last_error = NULL, updated_at = ?1
             WHERE canvas_id = ?2 AND create_op_id = ?3 AND owner_user_id IS ?4
               AND executor_id = ?5 AND claim_token = ?6 AND status = 'provisioned'",
            params![
                now,
                canvas_id,
                create_op_id,
                owner_user_id,
                executor_id,
                claim_token,
            ],
        )?;
        ensure!(intent_updated == 1, "canvas resource create claim changed");
        canvas.revision = next_revision;
        canvas.updated_at = now;
        tx.commit()?;
        Ok(canvas)
    }

    pub fn mark_resource_create_failed_for_exact_owner(
        &self,
        canvas_id: &str,
        owner_user_id: Option<&str>,
        create_op_id: &str,
        executor_id: &str,
        claim_token: &str,
        error: &str,
    ) -> Result<CanvasResourceCreateIntent> {
        self.update_resource_create_phase(
            canvas_id,
            owner_user_id,
            create_op_id,
            executor_id,
            claim_token,
            CanvasResourceCreateStatus::Failed,
            None,
            Some(error),
        )
    }

    pub fn record_resource_bind_failure_for_exact_owner(
        &self,
        canvas_id: &str,
        owner_user_id: Option<&str>,
        create_op_id: &str,
        executor_id: &str,
        claim_token: &str,
        error: &str,
    ) -> Result<CanvasResourceCreateIntent> {
        self.update_resource_create_phase(
            canvas_id,
            owner_user_id,
            create_op_id,
            executor_id,
            claim_token,
            CanvasResourceCreateStatus::Provisioned,
            None,
            Some(error),
        )
    }

    pub fn claim_resource_cleanup_for_exact_owner(
        &self,
        canvas_id: &str,
        owner_user_id: Option<&str>,
        create_op_id: &str,
        executor_id: &str,
    ) -> Result<CanvasResourceCleanupClaim> {
        self.claim_resource_cleanup_for_exact_owner_with_stale_after_and_bound_authorization(
            canvas_id,
            owner_user_id,
            create_op_id,
            executor_id,
            RESOURCE_CREATE_CLAIM_STALE_AFTER,
            false,
        )
    }

    pub fn claim_resource_cleanup_for_exact_owner_with_bound_authorization(
        &self,
        canvas_id: &str,
        owner_user_id: Option<&str>,
        create_op_id: &str,
        executor_id: &str,
        bound_authorized: bool,
    ) -> Result<CanvasResourceCleanupClaim> {
        self.claim_resource_cleanup_for_exact_owner_with_stale_after_and_bound_authorization(
            canvas_id,
            owner_user_id,
            create_op_id,
            executor_id,
            RESOURCE_CREATE_CLAIM_STALE_AFTER,
            bound_authorized,
        )
    }

    pub fn claim_resource_cleanup_for_exact_owner_with_stale_after(
        &self,
        canvas_id: &str,
        owner_user_id: Option<&str>,
        create_op_id: &str,
        executor_id: &str,
        stale_after: Duration,
    ) -> Result<CanvasResourceCleanupClaim> {
        self.claim_resource_cleanup_for_exact_owner_with_stale_after_and_bound_authorization(
            canvas_id,
            owner_user_id,
            create_op_id,
            executor_id,
            stale_after,
            false,
        )
    }

    pub fn claim_resource_cleanup_for_exact_owner_with_stale_after_and_bound_authorization(
        &self,
        canvas_id: &str,
        owner_user_id: Option<&str>,
        create_op_id: &str,
        executor_id: &str,
        stale_after: Duration,
        bound_authorized: bool,
    ) -> Result<CanvasResourceCleanupClaim> {
        validate_identifier("canvas resource create operation", create_op_id)?;
        validate_identifier("canvas resource cleanup executor", executor_id)?;
        let tx = Transaction::new_unchecked(self.db.conn(), TransactionBehavior::Immediate)
            .context("acquiring canvas resource cleanup claim")?;
        let mut intent = load_resource_create_intent(&tx, canvas_id, owner_user_id, create_op_id)?
            .ok_or_else(|| anyhow!("canvas resource create intent not found"))?;

        if matches!(
            intent.status,
            CanvasResourceCreateStatus::Cleaned | CanvasResourceCreateStatus::Abandoned
        ) {
            tx.commit()?;
            return Ok(CanvasResourceCleanupClaim::Completed(intent));
        }
        if intent.status == CanvasResourceCreateStatus::Bound {
            if !bound_authorized {
                tx.commit()?;
                return Ok(CanvasResourceCleanupClaim::BoundRequiresAuthorization(
                    intent,
                ));
            }
            ensure!(
                intent.result_json.is_some(),
                "bound canvas resource has no provision result evidence"
            );
        }

        // Reserved work has never crossed the provisioning boundary. Failed
        // provisioners in the current contracts likewise return before an
        // external resource is live. Finalize these states in this same
        // SQLite transaction so a server crash cannot lose that provenance
        // by leaving a generic Cleaning row behind.
        if matches!(intent.status, CanvasResourceCreateStatus::Reserved)
            || (intent.status == CanvasResourceCreateStatus::Failed && intent.result_json.is_none())
        {
            let final_status = if intent.resource_kind == CanvasResourceKind::Agent {
                CanvasResourceCreateStatus::Abandoned
            } else {
                CanvasResourceCreateStatus::Cleaned
            };
            let now = Utc::now().to_rfc3339();
            let updated = tx.execute(
                "UPDATE spatial_canvas_resource_creates
                 SET status = ?1, executor_id = ?2, claim_token = NULL,
                     request_json = CASE
                         WHEN resource_kind = 'agent'
                         THEN json_remove(request_json, '$.initial_prompt')
                         ELSE request_json
                     END,
                     last_error = NULL, updated_at = ?3
                 WHERE canvas_id = ?4 AND create_op_id = ?5 AND owner_user_id IS ?6
                   AND status IN ('reserved', 'failed')",
                params![
                    final_status.to_string(),
                    executor_id,
                    now,
                    canvas_id,
                    create_op_id,
                    owner_user_id,
                ],
            )?;
            ensure!(updated == 1, "canvas resource cleanup claim changed");
            intent.status = final_status;
            intent.executor_id = Some(executor_id.to_string());
            intent.claim_token = None;
            intent.last_error = None;
            intent.updated_at = now;
            tx.commit()?;
            return Ok(CanvasResourceCleanupClaim::Completed(intent));
        }

        let claim_is_stale = resource_create_claim_is_stale(&intent, stale_after);
        if matches!(
            intent.status,
            CanvasResourceCreateStatus::Provisioning | CanvasResourceCreateStatus::Cleaning
        ) && !claim_is_stale
        {
            tx.commit()?;
            return Ok(CanvasResourceCleanupClaim::Busy(intent));
        }

        let prior_status = intent.status;
        let prior_executor_matches = intent.executor_id.as_deref() == Some(executor_id);
        let absence_is_authoritative = prior_executor_matches
            && matches!(
                prior_status,
                CanvasResourceCreateStatus::Bound
                    | CanvasResourceCreateStatus::Provisioned
                    | CanvasResourceCreateStatus::Cleaning
                    | CanvasResourceCreateStatus::Ambiguous
                    | CanvasResourceCreateStatus::Failed
            );
        let claim_token = Uuid::new_v4().to_string();
        let now = Utc::now().to_rfc3339();
        let updated = tx.execute(
            "UPDATE spatial_canvas_resource_creates
             SET status = 'cleaning', executor_id = ?1, claim_token = ?2,
                 last_error = NULL, updated_at = ?3
             WHERE canvas_id = ?4 AND create_op_id = ?5 AND owner_user_id IS ?6",
            params![
                executor_id,
                claim_token,
                now,
                canvas_id,
                create_op_id,
                owner_user_id,
            ],
        )?;
        ensure!(updated == 1, "canvas resource cleanup claim changed");
        intent.status = CanvasResourceCreateStatus::Cleaning;
        intent.executor_id = Some(executor_id.to_string());
        intent.claim_token = Some(claim_token);
        intent.last_error = None;
        intent.updated_at = now;
        tx.commit()?;
        Ok(CanvasResourceCleanupClaim::Acquired {
            intent,
            prior_status,
            absence_is_authoritative,
        })
    }

    pub fn finish_resource_cleanup_for_exact_owner(
        &self,
        canvas_id: &str,
        owner_user_id: Option<&str>,
        create_op_id: &str,
        executor_id: &str,
        claim_token: &str,
        final_status: CanvasResourceCreateStatus,
    ) -> Result<CanvasResourceCreateIntent> {
        ensure!(
            matches!(
                final_status,
                CanvasResourceCreateStatus::Cleaned | CanvasResourceCreateStatus::Abandoned
            ),
            "canvas resource cleanup terminal status must be cleaned or abandoned"
        );
        validate_identifier("canvas resource cleanup claim", claim_token)?;
        let tx = Transaction::new_unchecked(self.db.conn(), TransactionBehavior::Immediate)
            .context("acquiring canvas resource cleanup completion lock")?;
        let current = load_resource_create_intent(&tx, canvas_id, owner_user_id, create_op_id)?
            .ok_or_else(|| anyhow!("canvas resource create intent not found"))?;
        ensure!(
            current.status == CanvasResourceCreateStatus::Cleaning
                && current.executor_id.as_deref() == Some(executor_id)
                && current.claim_token.as_deref() == Some(claim_token),
            "canvas resource cleanup claim changed"
        );
        ensure!(
            (current.resource_kind == CanvasResourceKind::Agent
                && final_status == CanvasResourceCreateStatus::Abandoned)
                || (current.resource_kind != CanvasResourceKind::Agent
                    && final_status == CanvasResourceCreateStatus::Cleaned),
            "canvas resource cleanup terminal status does not match its resource kind"
        );
        let now = Utc::now().to_rfc3339();
        let updated = tx.execute(
            "UPDATE spatial_canvas_resource_creates
             SET status = ?1, claim_token = NULL,
                 request_json = CASE
                     WHEN resource_kind = 'agent'
                     THEN json_remove(request_json, '$.initial_prompt')
                     ELSE request_json
                 END,
                 last_error = NULL, updated_at = ?2
             WHERE canvas_id = ?3 AND create_op_id = ?4 AND owner_user_id IS ?5
               AND executor_id = ?6 AND claim_token = ?7 AND status = 'cleaning'",
            params![
                final_status.to_string(),
                now,
                canvas_id,
                create_op_id,
                owner_user_id,
                executor_id,
                claim_token,
            ],
        )?;
        ensure!(updated == 1, "canvas resource cleanup claim changed");
        let intent = load_resource_create_intent(&tx, canvas_id, owner_user_id, create_op_id)?
            .ok_or_else(|| anyhow!("completed cleanup intent could not be reloaded"))?;
        tx.commit()?;
        Ok(intent)
    }

    /// Return a newly claimed bound cleanup to `Bound` when the caller's
    /// control-lease proof stopped being valid before external termination.
    /// The exact claim token fences this rollback from a later cleanup worker.
    pub fn release_bound_resource_cleanup_claim_for_exact_owner(
        &self,
        canvas_id: &str,
        owner_user_id: Option<&str>,
        create_op_id: &str,
        executor_id: &str,
        claim_token: &str,
        error: &str,
    ) -> Result<CanvasResourceCreateIntent> {
        ensure!(
            error.len() <= 8_192,
            "canvas resource cleanup error is too long"
        );
        validate_identifier("canvas resource cleanup claim", claim_token)?;
        let tx = Transaction::new_unchecked(self.db.conn(), TransactionBehavior::Immediate)
            .context("acquiring bound canvas resource cleanup release lock")?;
        let current = load_resource_create_intent(&tx, canvas_id, owner_user_id, create_op_id)?
            .ok_or_else(|| anyhow!("canvas resource create intent not found"))?;
        ensure!(
            current.status == CanvasResourceCreateStatus::Cleaning
                && current.executor_id.as_deref() == Some(executor_id)
                && current.claim_token.as_deref() == Some(claim_token)
                && current.result_json.is_some(),
            "bound canvas resource cleanup claim changed"
        );
        let now = Utc::now().to_rfc3339();
        let updated = tx.execute(
            "UPDATE spatial_canvas_resource_creates
             SET status = 'bound', claim_token = NULL, last_error = ?1, updated_at = ?2
             WHERE canvas_id = ?3 AND create_op_id = ?4 AND owner_user_id IS ?5
               AND executor_id = ?6 AND claim_token = ?7 AND status = 'cleaning'
               AND result_json IS NOT NULL",
            params![
                error,
                now,
                canvas_id,
                create_op_id,
                owner_user_id,
                executor_id,
                claim_token,
            ],
        )?;
        ensure!(updated == 1, "bound canvas resource cleanup claim changed");
        let intent = load_resource_create_intent(&tx, canvas_id, owner_user_id, create_op_id)?
            .ok_or_else(|| anyhow!("released cleanup intent could not be reloaded"))?;
        tx.commit()?;
        Ok(intent)
    }

    /// Complete cleanup of a previously bound live resource and remove its
    /// exact Canvas binding in the same SQLite transaction. The external
    /// runtime must already be authoritatively ended before calling this
    /// method. A generic Canvas PATCH cannot consume the Cleaning reservation,
    /// so the exact node cannot be rebound between claim and completion.
    pub fn finish_bound_resource_cleanup_and_unbind_for_exact_owner(
        &self,
        canvas_id: &str,
        owner_user_id: Option<&str>,
        create_op_id: &str,
        executor_id: &str,
        claim_token: &str,
        final_status: CanvasResourceCreateStatus,
    ) -> Result<(CanvasResourceCreateIntent, SpatialCanvas)> {
        ensure!(
            matches!(
                final_status,
                CanvasResourceCreateStatus::Cleaned | CanvasResourceCreateStatus::Abandoned
            ),
            "canvas resource cleanup terminal status must be cleaned or abandoned"
        );
        validate_identifier("canvas resource cleanup claim", claim_token)?;
        let tx = Transaction::new_unchecked(self.db.conn(), TransactionBehavior::Immediate)
            .context("acquiring bound canvas resource cleanup completion lock")?;
        let current = load_resource_create_intent(&tx, canvas_id, owner_user_id, create_op_id)?
            .ok_or_else(|| anyhow!("canvas resource create intent not found"))?;
        ensure!(
            current.status == CanvasResourceCreateStatus::Cleaning
                && current.executor_id.as_deref() == Some(executor_id)
                && current.claim_token.as_deref() == Some(claim_token)
                && current.result_json.is_some(),
            "bound canvas resource cleanup claim changed"
        );
        ensure!(
            (current.resource_kind == CanvasResourceKind::Agent
                && final_status == CanvasResourceCreateStatus::Abandoned)
                || (current.resource_kind != CanvasResourceKind::Agent
                    && final_status == CanvasResourceCreateStatus::Cleaned),
            "canvas resource cleanup terminal status does not match its resource kind"
        );

        let expected = reserved_resource_node(&current)?;
        let mut canvas = load_owned_canvas(&tx, canvas_id, owner_user_id)?
            .ok_or_else(|| anyhow!("spatial canvas not found"))?;
        let now = Utc::now().to_rfc3339();
        let exact_binding_present = canvas
            .state
            .nodes
            .iter()
            .any(|node| canvas_node_has_resource_identity(node, &expected));
        if exact_binding_present {
            let operations = vec![CanvasOperation::RemoveNode {
                node_id: expected.id,
            }];
            let operations_json = serde_json::to_string(&operations)
                .context("serializing resource cleanup operation")?;
            let cleanup_op_id = format!("resource-cleanup:{create_op_id}:{claim_token}");
            validate_identifier("canvas resource cleanup operation", &cleanup_op_id)?;
            let cleanup_operation_exists: bool = tx.query_row(
                "SELECT EXISTS(
                     SELECT 1 FROM spatial_canvas_events
                     WHERE canvas_id = ?1 AND op_id = ?2
                 )",
                params![canvas_id, cleanup_op_id],
                |row| row.get(0),
            )?;
            ensure!(
                !cleanup_operation_exists,
                "canvas resource cleanup operation was already used"
            );

            apply_operations(&mut canvas, &operations, &now)?;
            validate_canvas_state(&canvas.state)?;
            validate_canvas_capacity_with_reservations(&tx, canvas_id, &canvas.state)?;
            let next_revision = canvas
                .revision
                .checked_add(1)
                .ok_or_else(|| anyhow!("canvas revision overflow"))?;
            let state_json =
                serde_json::to_string(&canvas.state).context("serializing canvas state")?;
            ensure!(
                state_json.len() <= MAX_STATE_JSON_BYTES,
                "canvas state exceeds the supported size"
            );
            let canvas_updated = tx.execute(
                "UPDATE spatial_canvases
                 SET revision = ?1, state_json = ?2, updated_at = ?3
                 WHERE id = ?4 AND owner_user_id IS ?5 AND revision = ?6",
                params![
                    next_revision,
                    state_json,
                    now,
                    canvas_id,
                    owner_user_id,
                    canvas.revision,
                ],
            )?;
            ensure!(
                canvas_updated == 1,
                "canvas revision changed during resource cleanup"
            );
            tx.execute(
                "INSERT INTO spatial_canvas_events (
                     canvas_id, sequence, op_id, client_id, operations_json, created_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    canvas_id,
                    next_revision,
                    cleanup_op_id,
                    current.client_id,
                    operations_json,
                    now,
                ],
            )?;
            tx.execute(
                "DELETE FROM spatial_canvas_events
                 WHERE canvas_id = ?1 AND sequence < ?2",
                params![
                    canvas_id,
                    next_revision.saturating_sub(MAX_RETAINED_EVENTS.saturating_sub(1))
                ],
            )?;
            canvas.revision = next_revision;
            canvas.updated_at = now.clone();
        }
        let intent_updated = tx.execute(
            "UPDATE spatial_canvas_resource_creates
             SET status = ?1, claim_token = NULL,
                 request_json = CASE
                     WHEN resource_kind = 'agent'
                     THEN json_remove(request_json, '$.initial_prompt')
                     ELSE request_json
                 END,
                 last_error = NULL, updated_at = ?2
             WHERE canvas_id = ?3 AND create_op_id = ?4 AND owner_user_id IS ?5
               AND executor_id = ?6 AND claim_token = ?7 AND status = 'cleaning'",
            params![
                final_status.to_string(),
                now,
                canvas_id,
                create_op_id,
                owner_user_id,
                executor_id,
                claim_token,
            ],
        )?;
        ensure!(intent_updated == 1, "canvas resource cleanup claim changed");
        let intent = load_resource_create_intent(&tx, canvas_id, owner_user_id, create_op_id)?
            .ok_or_else(|| anyhow!("completed cleanup intent could not be reloaded"))?;
        tx.commit()?;
        Ok((intent, canvas))
    }

    /// Idempotently detach the exact node reserved by a Bound external
    /// resource without claiming that its runtime ended. This is the safe
    /// escape hatch for cleanup whose external lifecycle cannot be proven
    /// after restart. A replay never removes a later node rebind.
    pub fn detach_bound_resource_for_exact_owner(
        &self,
        canvas_id: &str,
        owner_user_id: Option<&str>,
        create_op_id: &str,
        expected_detach_generation: u64,
        expected_request_json: &str,
    ) -> Result<(CanvasResourceCreateIntent, SpatialCanvas, bool, bool)> {
        validate_identifier("canvas resource create operation", create_op_id)?;
        ensure!(
            expected_request_json.len() <= 65_536,
            "canvas resource detach request is too large"
        );
        let request: serde_json::Value = serde_json::from_str(expected_request_json)
            .context("parsing canvas resource detach request")?;
        ensure!(
            request.is_object(),
            "canvas resource detach request must be a JSON object"
        );

        let tx = Transaction::new_unchecked(self.db.conn(), TransactionBehavior::Immediate)
            .context("acquiring bound canvas resource detach lock")?;
        let intent = load_resource_create_intent(&tx, canvas_id, owner_user_id, create_op_id)?
            .ok_or_else(|| anyhow!("canvas resource create intent not found"))?;
        ensure!(
            matches!(
                intent.status,
                CanvasResourceCreateStatus::Bound | CanvasResourceCreateStatus::Ambiguous
            ) && intent.result_json.is_some(),
            "only a bound or ambiguous provisioned canvas resource can be detached"
        );
        ensure!(
            intent.resource_kind != CanvasResourceKind::Agent,
            "Canvas Agent resources must be abandoned through authoritative cleanup"
        );
        ensure!(
            intent.request_fingerprint == hash_request_bytes(expected_request_json.as_bytes()),
            "canvas resource detach request does not match the reserved operation"
        );

        let expected = reserved_resource_node(&intent)?;
        let mut canvas = load_owned_canvas(&tx, canvas_id, owner_user_id)?
            .ok_or_else(|| anyhow!("spatial canvas not found"))?;
        ensure!(
            expected_detach_generation <= intent.detach_generation,
            "canvas resource detach generation is ahead of the durable intent"
        );
        if expected_detach_generation < intent.detach_generation {
            let binding_absent = canvas
                .state
                .nodes
                .iter()
                .all(|node| !canvas_node_has_resource_identity(node, &expected));
            tx.commit()?;
            return Ok((intent, canvas, binding_absent, true));
        }
        ensure!(
            intent.detach_generation < i64::MAX as u64,
            "canvas resource detach generation overflow"
        );
        let next_detach_generation = intent
            .detach_generation
            .checked_add(1)
            .ok_or_else(|| anyhow!("canvas resource detach generation overflow"))?;
        let detach_event_op_id = format!(
            "resource-detach:{create_op_id}:{}",
            intent.detach_generation
        );
        validate_identifier("canvas resource detach operation", &detach_event_op_id)?;
        let exact_binding_present = canvas
            .state
            .nodes
            .iter()
            .any(|node| canvas_node_has_resource_identity(node, &expected));
        {
            let operations = if exact_binding_present {
                vec![CanvasOperation::RemoveNode {
                    node_id: expected.id.clone(),
                }]
            } else {
                Vec::new()
            };
            let operations_json = serde_json::to_string(&operations)
                .context("serializing resource detach operation")?;
            let now = Utc::now().to_rfc3339();
            if !operations.is_empty() {
                apply_operations(&mut canvas, &operations, &now)?;
            }
            validate_canvas_state(&canvas.state)?;
            validate_canvas_capacity_with_reservations(&tx, canvas_id, &canvas.state)?;
            let next_revision = canvas
                .revision
                .checked_add(1)
                .ok_or_else(|| anyhow!("canvas revision overflow"))?;
            let state_json =
                serde_json::to_string(&canvas.state).context("serializing canvas state")?;
            ensure!(
                state_json.len() <= MAX_STATE_JSON_BYTES,
                "canvas state exceeds the supported size"
            );
            let canvas_updated = tx.execute(
                "UPDATE spatial_canvases
                 SET revision = ?1, state_json = ?2, updated_at = ?3
                 WHERE id = ?4 AND owner_user_id IS ?5 AND revision = ?6",
                params![
                    next_revision,
                    state_json,
                    now,
                    canvas_id,
                    owner_user_id,
                    canvas.revision,
                ],
            )?;
            ensure!(
                canvas_updated == 1,
                "canvas revision changed during resource detach"
            );
            tx.execute(
                "INSERT INTO spatial_canvas_events (
                     canvas_id, sequence, op_id, client_id, operations_json, created_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    canvas_id,
                    next_revision,
                    detach_event_op_id,
                    &intent.client_id,
                    operations_json,
                    now,
                ],
            )?;
            tx.execute(
                "DELETE FROM spatial_canvas_events
                 WHERE canvas_id = ?1 AND sequence < ?2",
                params![
                    canvas_id,
                    next_revision.saturating_sub(MAX_RETAINED_EVENTS.saturating_sub(1))
                ],
            )?;
            canvas.revision = next_revision;
            canvas.updated_at = now;
        }
        let generation_updated = tx.execute(
            "UPDATE spatial_canvas_resource_creates
             SET detach_generation = ?1, updated_at = ?2
             WHERE canvas_id = ?3 AND create_op_id = ?4 AND owner_user_id IS ?5
               AND detach_generation = ?6",
            params![
                next_detach_generation,
                canvas.updated_at,
                canvas_id,
                create_op_id,
                owner_user_id,
                expected_detach_generation,
            ],
        )?;
        ensure!(
            generation_updated == 1,
            "canvas resource detach generation changed"
        );
        let binding_absent = canvas
            .state
            .nodes
            .iter()
            .all(|node| !canvas_node_has_resource_identity(node, &expected));
        let intent = load_resource_create_intent(&tx, canvas_id, owner_user_id, create_op_id)?
            .ok_or_else(|| anyhow!("detached canvas resource intent could not be reloaded"))?;
        tx.commit()?;
        Ok((intent, canvas, binding_absent, true))
    }

    pub fn record_resource_cleanup_failure_for_exact_owner(
        &self,
        canvas_id: &str,
        owner_user_id: Option<&str>,
        create_op_id: &str,
        executor_id: &str,
        claim_token: &str,
        error: &str,
    ) -> Result<CanvasResourceCreateIntent> {
        ensure!(
            error.len() <= 8_192,
            "canvas resource cleanup error is too long"
        );
        validate_identifier("canvas resource cleanup claim", claim_token)?;
        let tx = Transaction::new_unchecked(self.db.conn(), TransactionBehavior::Immediate)
            .context("acquiring canvas resource cleanup failure lock")?;
        let now = Utc::now().to_rfc3339();
        let updated = tx.execute(
            "UPDATE spatial_canvas_resource_creates
             SET status = 'ambiguous', claim_token = NULL, last_error = ?1, updated_at = ?2
             WHERE canvas_id = ?3 AND create_op_id = ?4 AND owner_user_id IS ?5
               AND executor_id = ?6 AND claim_token = ?7 AND status = 'cleaning'",
            params![
                error,
                now,
                canvas_id,
                create_op_id,
                owner_user_id,
                executor_id,
                claim_token,
            ],
        )?;
        ensure!(updated == 1, "canvas resource cleanup claim changed");
        let intent = load_resource_create_intent(&tx, canvas_id, owner_user_id, create_op_id)?
            .ok_or_else(|| anyhow!("failed cleanup intent could not be reloaded"))?;
        tx.commit()?;
        Ok(intent)
    }

    fn update_resource_create_phase(
        &self,
        canvas_id: &str,
        owner_user_id: Option<&str>,
        create_op_id: &str,
        executor_id: &str,
        claim_token: &str,
        status: CanvasResourceCreateStatus,
        result_json: Option<&str>,
        last_error: Option<&str>,
    ) -> Result<CanvasResourceCreateIntent> {
        ensure!(
            last_error.is_none_or(|error| error.len() <= 8_192),
            "canvas resource create error is too long"
        );
        validate_identifier("canvas resource claim", claim_token)?;
        let tx = Transaction::new_unchecked(self.db.conn(), TransactionBehavior::Immediate)
            .context("acquiring canvas resource phase update lock")?;
        let current = load_resource_create_intent(&tx, canvas_id, owner_user_id, create_op_id)?
            .ok_or_else(|| anyhow!("canvas resource create intent not found"))?;
        ensure!(
            current.executor_id.as_deref() == Some(executor_id)
                && current.claim_token.as_deref() == Some(claim_token),
            "canvas resource create claim changed"
        );
        match status {
            CanvasResourceCreateStatus::Provisioned if result_json.is_some() => ensure!(
                current.status == CanvasResourceCreateStatus::Provisioning,
                "canvas resource can only become provisioned from provisioning"
            ),
            CanvasResourceCreateStatus::Provisioned => ensure!(
                current.status == CanvasResourceCreateStatus::Provisioned,
                "canvas resource bind failure requires provisioned evidence"
            ),
            CanvasResourceCreateStatus::Failed => ensure!(
                current.status == CanvasResourceCreateStatus::Provisioning,
                "canvas resource can only fail while provisioning"
            ),
            _ => anyhow::bail!("unsupported canvas resource phase transition"),
        }
        let retained_result = result_json.or(current.result_json.as_deref());
        let now = Utc::now().to_rfc3339();
        let updated = tx.execute(
            "UPDATE spatial_canvas_resource_creates
             SET status = ?1, result_json = ?2, last_error = ?3, updated_at = ?4
             WHERE canvas_id = ?5 AND create_op_id = ?6 AND owner_user_id IS ?7
               AND executor_id = ?8 AND claim_token = ?9",
            params![
                status.to_string(),
                retained_result,
                last_error,
                now,
                canvas_id,
                create_op_id,
                owner_user_id,
                executor_id,
                claim_token,
            ],
        )?;
        ensure!(updated == 1, "canvas resource create claim changed");
        let intent = load_resource_create_intent(&tx, canvas_id, owner_user_id, create_op_id)?
            .ok_or_else(|| {
                anyhow!("updated canvas resource create intent could not be reloaded")
            })?;
        tx.commit()?;
        Ok(intent)
    }
}

fn validate_create_input(input: &CreateCanvasInput) -> Result<()> {
    validate_identifier("canvas create operation", &input.create_op_id)?;
    validate_text("canvas name", &input.name, MAX_TITLE_BYTES)?;
    validate_text(
        "shared theme id",
        &input.shared_theme_id,
        MAX_THEME_ID_BYTES,
    )?;
    ensure!(
        input.project_dir.starts_with('/'),
        "canvas project directory must be an absolute canonical path"
    );
    Ok(())
}

fn validate_patch_input(input: &CanvasPatchInput) -> Result<()> {
    validate_identifier("canvas operation", &input.op_id)?;
    validate_identifier("canvas client", &input.client_id)?;
    ensure!(
        !input.operations.is_empty() && input.operations.len() <= MAX_PATCH_OPERATIONS,
        "canvas patch must contain between 1 and {MAX_PATCH_OPERATIONS} operations"
    );
    Ok(())
}

fn validate_resource_create_input(input: &ReserveCanvasResourceCreateInput) -> Result<()> {
    validate_identifier("canvas resource create operation", &input.create_op_id)?;
    ensure!(
        input.create_op_id.len() <= 192,
        "canvas resource create operation id is too long"
    );
    validate_identifier("canvas resource bind operation", &input.bind_op_id)?;
    validate_identifier("canvas resource client", &input.client_id)?;
    validate_identifier("canvas resource node", &input.node_id)?;
    ensure!(
        Uuid::parse_str(&input.node_id).is_ok(),
        "canvas resource node id must be a UUID"
    );
    ensure!(
        input.request_json.len() <= 65_536,
        "canvas resource create request is too large"
    );
    let request: serde_json::Value = serde_json::from_str(&input.request_json)
        .context("parsing canvas resource create request")?;
    ensure!(
        request.is_object(),
        "canvas resource create request must be a JSON object"
    );
    Ok(())
}

fn validate_result_json(result_json: &str) -> Result<()> {
    ensure!(
        result_json.len() <= 65_536,
        "canvas resource create result is too large"
    );
    let result: serde_json::Value =
        serde_json::from_str(result_json).context("parsing canvas resource create result")?;
    ensure!(
        result.is_object(),
        "canvas resource create result must be a JSON object"
    );
    Ok(())
}

fn resource_create_claim_is_stale(
    intent: &CanvasResourceCreateIntent,
    stale_after: Duration,
) -> bool {
    if !matches!(
        intent.status,
        CanvasResourceCreateStatus::Provisioning
            | CanvasResourceCreateStatus::Provisioned
            | CanvasResourceCreateStatus::Cleaning
    ) {
        return false;
    }
    let Ok(updated_at) = chrono::DateTime::parse_from_rfc3339(&intent.updated_at) else {
        return true;
    };
    let age = Utc::now().signed_duration_since(updated_at.with_timezone(&Utc));
    age >= chrono::Duration::from_std(stale_after).unwrap_or(chrono::Duration::MAX)
}

fn validate_identifier(label: &str, value: &str) -> Result<()> {
    let trimmed = value.trim();
    ensure!(!trimmed.is_empty(), "{label} id cannot be empty");
    ensure!(trimmed.len() <= MAX_ID_BYTES, "{label} id is too long");
    Ok(())
}

fn validate_text(label: &str, value: &str, max_bytes: usize) -> Result<()> {
    let trimmed = value.trim();
    ensure!(!trimmed.is_empty(), "{label} cannot be empty");
    ensure!(trimmed.len() <= max_bytes, "{label} is too long");
    Ok(())
}

fn validate_canvas_state(state: &CanvasState) -> Result<()> {
    ensure!(
        state.schema_version == SPATIAL_CANVAS_SCHEMA_VERSION,
        "unsupported canvas state schema version {}",
        state.schema_version
    );
    ensure!(
        state.nodes.len() <= MAX_CANVAS_NODES,
        "canvas contains more than {MAX_CANVAS_NODES} nodes"
    );
    let mut ids = HashSet::with_capacity(state.nodes.len());
    let mut live_bindings = HashSet::with_capacity(state.nodes.len());
    for node in &state.nodes {
        validate_node(node)?;
        ensure!(
            ids.insert(node.id.as_str()),
            "canvas node ids must be unique"
        );
        let live_binding = match &node.binding {
            CanvasNodeBinding::Agent { session_id } => Some(("agent", session_id.as_str())),
            CanvasNodeBinding::Terminal { terminal_id } => Some(("terminal", terminal_id.as_str())),
            CanvasNodeBinding::Browser {
                browser_session_id, ..
            } => Some(("browser", browser_session_id.as_str())),
            CanvasNodeBinding::Files { .. } | CanvasNodeBinding::Changes => None,
        };
        if let Some(live_binding) = live_binding {
            ensure!(
                live_bindings.insert(live_binding),
                "live Agent, Terminal, and Browser bindings must be unique within a canvas"
            );
        }
    }
    Ok(())
}

fn validate_node(node: &CanvasNode) -> Result<()> {
    validate_identifier("canvas node", &node.id)?;
    validate_text("canvas node title", &node.title, MAX_TITLE_BYTES)?;
    node.rect.validate().map_err(anyhow::Error::msg)?;
    match &node.binding {
        CanvasNodeBinding::Agent { session_id } => {
            validate_identifier("agent session", session_id)?;
        }
        CanvasNodeBinding::Terminal { terminal_id } => {
            validate_identifier("terminal", terminal_id)?;
        }
        CanvasNodeBinding::Browser {
            browser_session_id,
            last_url,
        } => {
            validate_identifier("browser session", browser_session_id)?;
            if let Some(url) = last_url {
                ensure!(url.len() <= 16_384, "browser URL is too long");
            }
        }
        CanvasNodeBinding::Files { path } => {
            if let Some(path) = path {
                ensure!(path.len() <= 16_384, "files path is too long");
            }
        }
        CanvasNodeBinding::Changes => {}
    }
    Ok(())
}

fn validate_persisted_bindings(
    conn: &rusqlite::Connection,
    canvas: &SpatialCanvas,
    owner_user_id: Option<&str>,
    operations: &[CanvasOperation],
    allowed_reserved_identity: Option<(String, String)>,
) -> Result<()> {
    for operation in operations {
        let CanvasOperation::UpsertNode { node } = operation else {
            continue;
        };
        if let Some((resource_kind, resource_id)) = live_resource_binding_identity(&node.binding) {
            let is_private_reserved_bind =
                allowed_reserved_identity
                    .as_ref()
                    .is_some_and(|(allowed_kind, allowed_id)| {
                        allowed_kind == resource_kind && allowed_id == resource_id
                    });
            let is_same_node_identity_update = canvas.state.nodes.iter().any(|current| {
                current.id == node.id
                    && live_resource_binding_identity(&current.binding)
                        == Some((resource_kind, resource_id))
            });
            let ledger_identity_exists: bool = conn.query_row(
                "SELECT EXISTS(
                     SELECT 1 FROM spatial_canvas_resource_creates
                     WHERE resource_kind = ?1 AND node_id = ?2
                 )",
                params![resource_kind, resource_id],
                |row| row.get(0),
            )?;
            ensure!(
                !ledger_identity_exists || is_private_reserved_bind || is_same_node_identity_update,
                "server-owned Canvas resource identities cannot be rebound through generic PATCH"
            );
            let other_canvas_binding_exists =
                canvas_binding_identity_exists(conn, resource_kind, resource_id, Some(&canvas.id))?;
            ensure!(
                !other_canvas_binding_exists,
                "live resource is already bound to another canvas"
            );
        }
        if let CanvasNodeBinding::Agent { session_id } = &node.binding {
            let matches_canvas: bool = conn.query_row(
                "SELECT EXISTS(
                     SELECT 1 FROM sessions
                     WHERE id = ?1
                       AND user_id IS ?2
                       AND session_type = 'code'
                       AND COALESCE(NULLIF(project_dir, ''), working_dir) = ?3
                 )",
                params![session_id, owner_user_id, canvas.project_dir],
                |row| row.get(0),
            )?;
            ensure!(
                matches_canvas,
                "agent session must be an exact-owner Code session in the same project"
            );
        }
    }
    Ok(())
}

fn ensure_resource_identity_is_unused(
    conn: &rusqlite::Connection,
    input: &ReserveCanvasResourceCreateInput,
) -> Result<()> {
    let resource_kind = input.resource_kind.to_string();
    let ledger_identity_exists: bool = conn.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM spatial_canvas_resource_creates
             WHERE resource_kind = ?1 AND node_id = ?2
               AND status NOT IN ('cleaned', 'abandoned')
         )",
        params![resource_kind, input.node_id],
        |row| row.get(0),
    )?;
    ensure!(
        !ledger_identity_exists,
        "canvas resource identity was already reserved by another operation"
    );
    ensure!(
        !canvas_binding_identity_exists(
            conn,
            &input.resource_kind.to_string(),
            &input.node_id,
            None,
        )?,
        "canvas resource identity is already bound to a canvas"
    );
    Ok(())
}

fn live_resource_binding_identity(binding: &CanvasNodeBinding) -> Option<(&'static str, &str)> {
    match binding {
        CanvasNodeBinding::Agent { session_id } => Some(("agent", session_id)),
        CanvasNodeBinding::Terminal { terminal_id } => Some(("terminal", terminal_id)),
        CanvasNodeBinding::Browser {
            browser_session_id, ..
        } => Some(("browser", browser_session_id)),
        CanvasNodeBinding::Files { .. } | CanvasNodeBinding::Changes => None,
    }
}

fn canvas_node_has_resource_identity(node: &CanvasNode, expected: &CanvasNode) -> bool {
    if node.id != expected.id {
        return false;
    }
    match (&node.binding, &expected.binding) {
        (
            CanvasNodeBinding::Agent { session_id },
            CanvasNodeBinding::Agent {
                session_id: expected_id,
            },
        ) => session_id == expected_id,
        (
            CanvasNodeBinding::Terminal { terminal_id },
            CanvasNodeBinding::Terminal {
                terminal_id: expected_id,
            },
        ) => terminal_id == expected_id,
        (
            CanvasNodeBinding::Browser {
                browser_session_id, ..
            },
            CanvasNodeBinding::Browser {
                browser_session_id: expected_id,
                ..
            },
        ) => browser_session_id == expected_id,
        _ => false,
    }
}

fn canvas_binding_identity_exists(
    conn: &rusqlite::Connection,
    resource_kind: &str,
    resource_id: &str,
    excluded_canvas_id: Option<&str>,
) -> Result<bool> {
    let id_field = match resource_kind {
        "agent" => "session_id",
        "terminal" => "terminal_id",
        "browser" => "browser_session_id",
        _ => anyhow::bail!("unsupported Canvas resource kind"),
    };
    conn.query_row(
        "SELECT EXISTS(
             SELECT 1
             FROM spatial_canvases AS canvas,
                  json_each(canvas.state_json, '$.nodes') AS node
             WHERE (?1 IS NULL OR canvas.id != ?1)
               AND json_extract(node.value, '$.binding.kind') = ?2
               AND json_extract(node.value, '$.binding.' || ?3) = ?4
         )",
        params![excluded_canvas_id, resource_kind, id_field, resource_id],
        |row| row.get(0),
    )
    .context("checking permanent Canvas resource identity ownership")
}

fn validate_resource_recovery_before_archive(
    conn: &rusqlite::Connection,
    canvas_id: &str,
    owner_user_id: Option<&str>,
    operations: &[CanvasOperation],
) -> Result<()> {
    if !operations
        .iter()
        .any(|operation| matches!(operation, CanvasOperation::SetArchived { archived: true }))
    {
        return Ok(());
    }
    let recovery_pending: bool = conn.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM spatial_canvas_resource_creates AS intent
             WHERE intent.canvas_id = ?1 AND intent.owner_user_id IS ?2
               AND (
                    intent.status IN ('reserved', 'provisioning', 'provisioned', 'failed', 'ambiguous', 'cleaning')
                    OR (
                        intent.status = 'bound' AND intent.resource_kind = 'agent'
                        AND json_type(intent.request_json, '$.initial_prompt') IS NOT NULL
                    )
                    OR (
                        intent.status = 'bound'
                        AND intent.resource_kind IN ('terminal', 'browser')
                        AND EXISTS(
                            SELECT 1
                            FROM spatial_canvases AS canvas,
                                 json_each(canvas.state_json, '$.nodes') AS node
                            WHERE canvas.id = intent.canvas_id
                              AND json_extract(node.value, '$.id') = intent.node_id
                              AND json_extract(node.value, '$.binding.kind') = intent.resource_kind
                              AND (
                                  (intent.resource_kind = 'terminal'
                                   AND json_extract(node.value, '$.binding.terminal_id') = intent.node_id)
                                  OR
                                  (intent.resource_kind = 'browser'
                                   AND json_extract(node.value, '$.binding.browser_session_id') = intent.node_id)
                              )
                        )
                    )
               )
         )",
        params![canvas_id, owner_user_id],
        |row| row.get(0),
    )?;
    ensure!(
        !recovery_pending,
        "canvas cannot be archived while a live resource or initial Agent prompt still needs recovery"
    );
    Ok(())
}

fn validate_resource_node_reservations(
    conn: &rusqlite::Connection,
    canvas_id: &str,
    input: &CanvasPatchInput,
) -> Result<()> {
    let reserved_bind_operation: bool = conn.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM spatial_canvas_resource_creates
             WHERE canvas_id = ?1 AND bind_op_id = ?2
               AND status NOT IN ('bound', 'cleaned', 'abandoned')
         )",
        params![canvas_id, input.op_id],
        |row| row.get(0),
    )?;
    ensure!(
        !reserved_bind_operation,
        "canvas operation id is reserved by an unfinished resource create operation"
    );
    for operation in &input.operations {
        let node_id = match operation {
            CanvasOperation::UpsertNode { node } => Some(node.id.as_str()),
            CanvasOperation::RemoveNode { node_id }
            | CanvasOperation::SetGeometry { node_id, .. }
            | CanvasOperation::SetTitle { node_id, .. } => Some(node_id.as_str()),
            CanvasOperation::RenameCanvas { .. }
            | CanvasOperation::SetSharedTheme { .. }
            | CanvasOperation::SetArchived { .. } => None,
        };
        let Some(node_id) = node_id else {
            continue;
        };
        let reserved_create = conn
            .query_row(
                "SELECT create_op_id
                 FROM spatial_canvas_resource_creates
                 WHERE canvas_id = ?1 AND node_id = ?2
                   AND status NOT IN ('bound', 'cleaned', 'abandoned')",
                params![canvas_id, node_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        if reserved_create.is_some() {
            anyhow::bail!("canvas node is reserved by an unfinished resource create operation");
        }
    }
    Ok(())
}

fn unfinished_resource_slots(
    conn: &rusqlite::Connection,
    canvas_id: &str,
    state: &CanvasState,
) -> Result<usize> {
    let represented = state
        .nodes
        .iter()
        .map(|node| node.id.as_str())
        .collect::<HashSet<_>>();
    let mut statement = conn.prepare(
        "SELECT node_id
         FROM spatial_canvas_resource_creates
         WHERE canvas_id = ?1
           AND status NOT IN ('bound', 'cleaned', 'abandoned')",
    )?;
    let node_ids = statement
        .query_map([canvas_id], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(node_ids
        .iter()
        .filter(|node_id| !represented.contains(node_id.as_str()))
        .count())
}

fn validate_canvas_capacity_with_reservations(
    conn: &rusqlite::Connection,
    canvas_id: &str,
    state: &CanvasState,
) -> Result<()> {
    let reserved_slots = unfinished_resource_slots(conn, canvas_id, state)?;
    ensure!(
        state.nodes.len().saturating_add(reserved_slots) <= MAX_CANVAS_NODES,
        "canvas node capacity is reserved by unfinished resource creates"
    );
    Ok(())
}

fn apply_operations(
    canvas: &mut SpatialCanvas,
    operations: &[CanvasOperation],
    now: &str,
) -> Result<()> {
    for operation in operations {
        match operation {
            CanvasOperation::UpsertNode { node } => {
                validate_node(node)?;
                if let Some(existing) = canvas
                    .state
                    .nodes
                    .iter_mut()
                    .find(|item| item.id == node.id)
                {
                    *existing = node.clone();
                } else {
                    ensure!(
                        canvas.state.nodes.len() < MAX_CANVAS_NODES,
                        "canvas contains more than {MAX_CANVAS_NODES} nodes"
                    );
                    canvas.state.nodes.push(node.clone());
                }
            }
            CanvasOperation::RemoveNode { node_id } => {
                validate_identifier("canvas node", node_id)?;
                canvas.state.nodes.retain(|node| node.id != *node_id);
            }
            CanvasOperation::SetGeometry {
                node_id,
                rect,
                z_index,
            } => {
                rect.validate().map_err(anyhow::Error::msg)?;
                let node = canvas
                    .state
                    .nodes
                    .iter_mut()
                    .find(|node| node.id == *node_id)
                    .ok_or_else(|| anyhow!("canvas node {node_id} not found"))?;
                node.rect = *rect;
                node.z_index = *z_index;
                node.revision = node.revision.saturating_add(1);
            }
            CanvasOperation::SetTitle { node_id, title } => {
                validate_text("canvas node title", title, MAX_TITLE_BYTES)?;
                let node = canvas
                    .state
                    .nodes
                    .iter_mut()
                    .find(|node| node.id == *node_id)
                    .ok_or_else(|| anyhow!("canvas node {node_id} not found"))?;
                node.title = title.trim().to_string();
                node.revision = node.revision.saturating_add(1);
            }
            CanvasOperation::RenameCanvas { name } => {
                validate_text("canvas name", name, MAX_TITLE_BYTES)?;
                canvas.name = name.trim().to_string();
            }
            CanvasOperation::SetSharedTheme { theme_id } => {
                validate_text("shared theme id", theme_id, MAX_THEME_ID_BYTES)?;
                canvas.shared_theme_id = theme_id.trim().to_string();
            }
            CanvasOperation::SetArchived { archived } => {
                canvas.archived_at = archived.then(|| now.to_string());
            }
        }
    }
    Ok(())
}

fn load_by_create_op(
    conn: &rusqlite::Connection,
    owner_user_id: Option<&str>,
    create_op_id: &str,
) -> Result<Option<(SpatialCanvas, String)>> {
    conn.query_row(
        "SELECT id, owner_user_id, project_dir, surface, name, schema_version,
                revision, shared_theme_id, state_json, created_at, updated_at, archived_at,
                create_request_json
         FROM spatial_canvases
         WHERE owner_user_id IS ?1 AND create_op_id = ?2",
        params![owner_user_id, create_op_id],
        |row| Ok((row_to_canvas(row)?, row.get(12)?)),
    )
    .optional()
    .context("loading canvas create operation")
}

fn load_owned_canvas(
    conn: &rusqlite::Connection,
    canvas_id: &str,
    owner_user_id: Option<&str>,
) -> Result<Option<SpatialCanvas>> {
    conn.query_row(
        "SELECT id, owner_user_id, project_dir, surface, name, schema_version,
                revision, shared_theme_id, state_json, created_at, updated_at, archived_at
         FROM spatial_canvases
         WHERE id = ?1 AND owner_user_id IS ?2",
        params![canvas_id, owner_user_id],
        row_to_canvas,
    )
    .optional()
    .context("loading spatial canvas for exact owner")
}

fn load_resource_create_intent(
    conn: &rusqlite::Connection,
    canvas_id: &str,
    owner_user_id: Option<&str>,
    create_op_id: &str,
) -> Result<Option<CanvasResourceCreateIntent>> {
    conn.query_row(
        "SELECT intent.canvas_id, intent.owner_user_id, intent.create_op_id,
                intent.bind_op_id, intent.client_id, intent.node_id,
                intent.resource_kind, intent.request_json, intent.request_fingerprint,
                intent.status,
                intent.executor_id, intent.claim_token, intent.result_json, intent.last_error,
                intent.created_at, intent.updated_at, intent.detach_generation
         FROM spatial_canvas_resource_creates intent
         JOIN spatial_canvases canvas ON canvas.id = intent.canvas_id
         WHERE intent.canvas_id = ?1
           AND intent.create_op_id = ?2
           AND canvas.owner_user_id IS ?3
           AND intent.owner_user_id IS ?3",
        params![canvas_id, create_op_id, owner_user_id],
        row_to_resource_create_intent,
    )
    .optional()
    .context("loading canvas resource create intent for exact owner")
}

fn ensure_resource_create_replay_matches(
    existing: &CanvasResourceCreateIntent,
    input: &ReserveCanvasResourceCreateInput,
) -> Result<()> {
    ensure!(
        existing.bind_op_id == input.bind_op_id
            && existing.client_id == input.client_id
            && existing.node_id == input.node_id
            && existing.resource_kind == input.resource_kind
            && existing.request_fingerprint == hash_request_bytes(input.request_json.as_bytes()),
        "canvas resource create operation was reused with different input"
    );
    Ok(())
}

#[derive(Deserialize)]
struct ReservedResourceRequest {
    node: ReservedResourceNode,
    resource: ReservedResourceSpec,
}

#[derive(Deserialize)]
struct ReservedResourceNode {
    id: String,
    title: String,
    rect: WorldRect,
    z_index: i64,
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum ReservedResourceSpec {
    Agent,
    Terminal {
        #[serde(rename = "size")]
        _size: serde_json::Value,
    },
    Browser {
        #[serde(default)]
        url: Option<String>,
    },
}

fn reserved_resource_node(intent: &CanvasResourceCreateIntent) -> Result<CanvasNode> {
    let request: ReservedResourceRequest = serde_json::from_str(&intent.request_json)
        .context("parsing reserved canvas resource request")?;
    ensure!(
        request.node.id == intent.node_id,
        "reserved canvas resource node identity changed"
    );
    let binding = match request.resource {
        ReservedResourceSpec::Agent => {
            ensure!(
                intent.resource_kind == CanvasResourceKind::Agent,
                "reserved canvas resource kind changed"
            );
            CanvasNodeBinding::Agent {
                session_id: request.node.id.clone(),
            }
        }
        ReservedResourceSpec::Terminal { .. } => {
            ensure!(
                intent.resource_kind == CanvasResourceKind::Terminal,
                "reserved canvas resource kind changed"
            );
            CanvasNodeBinding::Terminal {
                terminal_id: request.node.id.clone(),
            }
        }
        ReservedResourceSpec::Browser { url } => {
            ensure!(
                intent.resource_kind == CanvasResourceKind::Browser,
                "reserved canvas resource kind changed"
            );
            CanvasNodeBinding::Browser {
                browser_session_id: request.node.id.clone(),
                last_url: url,
            }
        }
    };
    let node = CanvasNode {
        id: request.node.id,
        title: request.node.title,
        rect: request.node.rect,
        z_index: request.node.z_index,
        binding,
        revision: 0,
    };
    validate_node(&node)?;
    Ok(node)
}

fn row_to_canvas(row: &rusqlite::Row<'_>) -> rusqlite::Result<SpatialCanvas> {
    let surface_raw: String = row.get(3)?;
    let surface = SpatialCanvasSurface::from_str(&surface_raw)
        .map_err(|error| rusqlite::Error::FromSqlConversionFailure(3, Type::Text, error.into()))?;
    let state_json: String = row.get(8)?;
    let state = serde_json::from_str::<CanvasState>(&state_json).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(8, Type::Text, Box::new(error))
    })?;
    Ok(SpatialCanvas {
        id: row.get(0)?,
        owner_user_id: row.get(1)?,
        project_dir: row.get(2)?,
        surface,
        name: row.get(4)?,
        schema_version: row.get(5)?,
        revision: row.get(6)?,
        shared_theme_id: row.get(7)?,
        state,
        created_at: row.get(9)?,
        updated_at: row.get(10)?,
        archived_at: row.get(11)?,
    })
}

fn row_to_event(row: &rusqlite::Row<'_>) -> rusqlite::Result<CanvasEvent> {
    let operations_json: String = row.get(4)?;
    let operations =
        serde_json::from_str::<Vec<CanvasOperation>>(&operations_json).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(4, Type::Text, Box::new(error))
        })?;
    Ok(CanvasEvent {
        canvas_id: row.get(0)?,
        sequence: row.get(1)?,
        op_id: row.get(2)?,
        client_id: row.get(3)?,
        operations,
        created_at: row.get(5)?,
    })
}

fn row_to_resource_create_intent(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<CanvasResourceCreateIntent> {
    let kind_raw: String = row.get(6)?;
    let resource_kind = CanvasResourceKind::from_str(&kind_raw)
        .map_err(|error| rusqlite::Error::FromSqlConversionFailure(6, Type::Text, error.into()))?;
    let status_raw: String = row.get(9)?;
    let status = CanvasResourceCreateStatus::from_str(&status_raw)
        .map_err(|error| rusqlite::Error::FromSqlConversionFailure(9, Type::Text, error.into()))?;
    Ok(CanvasResourceCreateIntent {
        canvas_id: row.get(0)?,
        owner_user_id: row.get(1)?,
        create_op_id: row.get(2)?,
        bind_op_id: row.get(3)?,
        client_id: row.get(4)?,
        node_id: row.get(5)?,
        resource_kind,
        request_json: row.get(7)?,
        request_fingerprint: row.get(8)?,
        status,
        executor_id: row.get(10)?,
        claim_token: row.get(11)?,
        result_json: row.get(12)?,
        last_error: row.get(13)?,
        created_at: row.get(14)?,
        updated_at: row.get(15)?,
        detach_generation: row.get(16)?,
    })
}

fn map_canvas_insert_error(error: rusqlite::Error, context: &'static str) -> anyhow::Error {
    match &error {
        rusqlite::Error::SqliteFailure(_, Some(message))
            if message.contains("spatial_canvases") || message.contains("UNIQUE") =>
        {
            anyhow!("a canvas with this name already exists for the project")
        }
        _ => anyhow!(error).context(context),
    }
}

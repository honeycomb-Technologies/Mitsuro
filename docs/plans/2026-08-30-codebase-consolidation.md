# Codebase Consolidation Plan — 2026-08-30

## Goal

Finish all four consolidation phases so that the codebase contains only the
main purpose of the code: one canonical agent loop, thin server routes, and no
dead code, duplicated paths, or unowned fallbacks. Completion is proven by the
required validation gates (`cargo check --workspace`, `cargo test --workspace`,
`cargo clippy --workspace -- -D warnings`, `cargo fmt --all`) passing on every
phase commit, plus the per-phase evidence listed below.

Baseline audit: three parallel deep-dive audits of `mitsuro-core/src/agent/`
(+`ai/`), `mitsuro-server`, and workspace-wide dead-code/duplication hunting
(2026-08-30). Agent loop verdict 7/10, server 7/10, ~2,250 LOC outright dead,
~250 LOC duplicate logic, one unfinished migration.

## Ground rules

- Each phase lands as its own commit; keep changes small and reversible.
- No behavior change unless a phase explicitly says so and the user approved it.
- Product-behavior decisions marked **[DECISION]** require user sign-off before
  the item is implemented.
- AGENTS.md invariants always win (e.g., legacy identity compatibility is documented and
  intentional — it is out of scope).

## Phase 1 — Pure subtraction (~2,200 LOC, zero behavior change)

Every item must be verified dead by ref-counting before deletion.

1. **Dead event system**: delete `agent/event_bus.rs`, `agent/events.rs`,
   `AgentState` struct (`agent/state.rs:117-155`), legacy
   `AgentConfig.max_turns` alias (`state.rs:160-164`); update `agent/mod.rs`
   re-exports. Evidence: zero non-self references in workspace.
2. **Dead `LoopEvent::Teammate*` chain** across 5 crates: core
   `agent/loop_events.rs:349-366`, `storage/runtime_traces/mapping.rs`,
   `extensions/agent/mod.rs:1249-1252`, `acp/processor/loop_impl.rs:324-327`,
   server `types/events.rs:318-334,678-700`, client `types.rs:871-886`,
   client-state `chat.rs:687-690`, cli `tui_v2/projection/live.rs:468-510`.
   Only exhaustive-match arms and serde mappings exist; no constructors.
3. **Uncompiled `agent/autonomy/team/` directory** (~858 LOC) — outside the
   module tree; delete.
4. **Legacy plan/verify delegation resume path**:
   `tools/implementations/agent/single.rs:622-1267` (`execute_plan`,
   `execute_verify`, both `#[allow(dead_code)]`) and the entire
   `agent/agent_types.rs` (280 LOC, `PlanConfig`/`VerifyConfig`).
   **[DECISION]** Confirm no production DBs must resume planner/verifier-role
   runs before deleting; else time-box with an expiry note.
5. **Dead pub APIs** (each verified zero-caller):
   - `ai/models/registry.rs`: `has_models`, `try_get_model`, `mark_recent`,
     `try_get_organized_models`, `try_has_models`
   - `ai/models/metadata.rs`: `with_catalog_provenance`, `pricing_tier`,
     `context_display`
   - `acp/session/state.rs`: `clear_messages`, `add_tool_call`,
     `add_tool_result`, `add_system_context`
   - `skills/manager.rs`: `ensure_global_dir`, `unregister_origin`,
     `load_skill_content_for_user`, `get_skills_metadata`, `create_skill`,
     `delete_skill`, `reload_skill` — **[DECISION]** if skill authoring via
     UI is a planned feature, keep and wire instead
   - `mcp/manager/runtime.rs`: `register_package_config_path`,
     `set_package_config_paths`, `cancel_oauth`, `get_client`;
     `mcp/client.rs` `is_alive`
   - `ai/client/thinking.rs` `call_with_thinking`;
     `ai/client/request_builder.rs` `build_request_body_with_messages`;
     `ai/client/config/ai_client.rs` `for_openai_with_auth_detection`;
     `ai/client/core/client.rs` `with_api_key`; `ai/catalog.rs`
     `credential_for_dynamic_models`
   - `agent/state.rs`: `exceeded_max_turns`, `turn_duration`
   - `plan/manager/mod.rs` `update_plan`; `plan/file/model.rs`
     `increment_version`, `version_matches`
   - `auth/device_flow.rs` `run_with_callback`;
     `process/registry.rs` `try_oldest_running_elapsed`;
     `plugins/manager/package.rs` `install_from_package_ref`;
     `tools/image.rs` `LoadedImage` alias + `is_image_extension`
6. **Test helper dedup**: hoist byte-identical `current_user` (×9) into one
   test-util module; unify `current_user_id` (`session_access.rs:16` vs
   `hooks.rs:182`).

Evidence: deletions compile, full gate passes, `grep` confirms removed
identifiers have zero remaining references.

## Phase 2 — Agent loop consolidation (behavior-preserving refactor)

1. Extract from `run_inner` (`orchestrator.rs:748-2310`):
   `reset_recovery_flags()`, `finish_run(stop_reason, ...)`, and one
   `compact_and_retry_once(trigger)` replacing the ×3 pasted overflow blocks.
2. Fix the asymmetric cancel branch (`orchestrator.rs:2204-2212`) — either all
   six cancel checks clear recovery state or none do; make it one helper.
3. One shared module (both kernels) for `LOOP_GUARD_LANDING_FALLBACK`,
   `loop_guard_landing_instruction`, `tool_call_requires_completion_shield`
   (currently duplicated `orchestrator.rs`/`subagent/execution/runtime.rs`).
4. Typed `StreamResult.last_error`: replace string + substring re-classification
   (`stream.rs:46`, orchestrator 529-544, 1430-1436) with a typed enum reusing
   `ProviderHttpError`/overflow classification.
5. Unify the 4 provider catalog paginators (anthropic/minimax/openrouter/grok)
   behind one generic paginator + per-provider request builder.
6. Rename `ai/stream_buffer.rs` → `ai/ui_smoothing.rs` (or similar) to remove
   the naming collision with `agent/stream.rs` — different layers, confusingly
   named.

Evidence: full gate passes; existing colocated loop tests pass unmodified
(orchestrator, executor, stream, tool_control, runtime tests); no new public
API surface beyond the shared helpers.

## Phase 3 — Server consolidation

1. Delete dead `HiveRuntimeManager::observe` (`hive_runtime.rs:784-807`).
2. Extract one shared `SseBridge` (bounded queue + explicit lag + required-
   delivery timeout policy) and use it at all five bridge sites
   (`chat/stream.rs:658,577`, `chat/interactions.rs:631`, `hive/sessions.rs:512`,
   plus any remaining) — behavior-preserving on the live four.
3. ~~Kill the 92 per-request `Database::new` + migration sweeps~~ — RESOLVED AS
   NO-CHANGE: `run_migrations` already fast-paths to one versioned SELECT, and
   the short-lived-connection model is deliberate (WAL arbitration sequences
   concurrent access instead of a process-global mutex). A migration
   verification gate was implemented and reverted: migration tests encode the
   contract that a replaced database file at the same path must re-migrate,
   and a global `Arc<Mutex<Database>>` would serialize parallel route
   handlers. Migration safety first (AGENTS.md).
4. Move duplicated derivations into core: `filter_code_tools_for_mode`
   (server `chat/tools.rs:188` → core wrapper next to `tool_surface.rs`),
   thinking-level normalization (`session.rs:894-951`), the model-override
   persistence block (4 copies).
5. Split the 197-line `chat` handler into prepare/authorize/dispatch helpers.
6. **[DECISION]** Multi-tenant hardening: thread `user_id` through
   `resolve_ai_client_for_key_for_user` (`lib.rs:263-267`); resolve
   `/tools/execute` `PermissionMode::Autonomous` hardcode (`routes/tools.rs:71`)
   to the shared delegated-contract resolver; scope `/ws/terminal` PTY or
   document it as single-user self-host behavior explicitly.
7. **[DECISION]** Silent vision-model substitution (`session.rs:578-592`):
   surface an explicit error/notice instead of proceeding on a model the user
   did not select.

Evidence: full gate passes; server route tests pass; SSE lag/timeout tests
pass unmodified or updated only where the shared bridge replaces copies.

## Phase 4 — Finish-or-revert migrations and compat expiry

1. **[DECISION]** `ToolContext.sandbox_root` deprecated mirror
   (`tools/registry/context.rs:113-118`, ~10 consumers with
   `unwrap_or(working_dir)`): finish the migration onto `filesystem_access`
   and delete the mirror, or revert. Choose one; no third state.
2. **[DECISION]** Legacy fallbacks to expire on a schedule (recorded 2026-08-30;
   deletion requires confirming no live databases can still hold the legacy
   shapes):
   - `server/child_wake.rs` pre-migration pending-row reader
     (`role LIKE 'pending_user:child-wake-%'`): expires once every deployed
     server has run one migration cycle past schema 55 AND no upgrade path
     from a pre-55 database is supported. Check at the next coordinated
     release; cutover criteria = zero rows matched by the reader on any
     observed production database for one release window.
   - `chat/interactions.rs` "High as legacy fallback" thinking-level recovery
     shim: expires when stored sessions created before persisted
     thinking-preferences can no longer be resumed (same mixed-version
     criteria). Both deletions are one-line removals plus test updates.
3. Apply the simplify skill (code reuse / quality / efficiency review) to every
   phase diff before commit.

## Out of scope (documented-intentional)

- Legacy identity compatibility layer (~1,410 LOC, documented migration readers).
- Extension WIT version matrix (live compat surface; needs a documented
  minimum-version policy, tracked separately).
- `tui_support` crate (live service layer consumed by `tui_v2`).

## Completion audit checklist

- [ ] Phase 1 committed, gates green, zero references to deleted items
- [ ] Phase 2 committed, gates green, loop tests pass unmodified
- [ ] Phase 3 committed, gates green, route/SSE tests pass
- [ ] Phase 4 decisions taken and implemented, gates green
- [ ] All **[DECISION]** items resolved with explicit user sign-off recorded here
- [ ] Final `cargo clippy --workspace -- -D warnings` + `cargo test --workspace`
      green on the final commit

# GPUI desktop status and continuation — ABANDONED

Abandoned historical implementation. Do not develop, build, or release this target. Use Native Canvas (`apps/desktop/canvas`); the remaining text describes the former implementation.
workspace as the Mitsuro server and client contracts. The transport boundary is
`crates/mitsuro-desktop-backend`.

The former standalone prototype directory is a preserved local snapshot. Do not
resume implementation there. Its build target,
gauntlet reports, reverse-engineering dumps, and duplicate comparison captures
are not canonical product evidence.

## Backend selection

`MITSURO_BACKEND` accepts:

- `mitsuro-http` (default): `MITSURO_SERVER_URL`, default `http://127.0.0.1:3000`;
  optional `MITSURO_SERVER_TOKEN` for authenticated remote access.
- `codex-stdio`: managed `codex app-server --stdio`; optional `CODEX_BIN`.
- `fixture`: explicit deterministic UI development.
- `auto`: currently prefers Mitsuro HTTP. Probe/fallback policy still needs a
  product decision.
- `codex-ws`: reserved and rejected honestly until the WebSocket transport is
  implemented.

A Ready, authenticated product backend sends a provider-backed turn by default.
Use `MITSURO_NO_LIVE_TURN=1` for read-only UI review or an explicit fixture backend for
deterministic replay. Merely connecting and discovering auth does not itself send a turn.
`MITSURO_NO_LIVE_TURN` disables Send; it does not select fixture replay.

The client-decorated GPUI Component title bar owns real sidebar, back/forward,
File/Edit/View/Help, and native window-control interactions. Visual regression runs can
select a surface with `MITSURO_START_MODE` and open one menu with
`MITSURO_START_APP_MENU=file|edit|view|help`; both affect initial presentation only and
never substitute backend records.

## Production data invariant

Fixture records are allowed only when both the UI connection and active backend are
explicitly `fixture`. Mitsuro HTTP, Codex stdio/WebSocket, connecting, and error states
cannot use fixture catalogs or fixture process/file/session operations. Backend errors
clear the affected projection and render a typed loading, empty, unsupported, or error
state. They never substitute sample success.

Desktop preferences, current mode, backend-scoped ordered pin ids, local browser URL
history, and transient composer UI remain local application state. Server-owned sessions,
transcripts, models, files, processes, extensions, Hive runs/schedules, and account usage
come from the selected backend or are shown as unavailable. A saved pin never creates a
thread row: the corresponding real session must still be present in the active backend's
live catalog.

## Truth matrix

| Surface | Mitsuro HTTP | Codex stdio | Fixture/static status |
|---|---|---|---|
| Health/connect | Live | Live | Explicit fixture |
| Sessions list/create/read/rename/delete | Live | Live | Typed fixture |
| Pinned recents | Desktop-local ordered pins scoped to Mitsuro session identity | Desktop-local ordered pins scoped to Codex thread identity, matching native-host ownership | Unavailable; fixture rows have no durable product identity |
| Activity sidebar | Priority from real in-flight interaction state; live RFC 3339 session updates grouped by local day | Priority from real in-flight interaction state; live app-server epoch updates grouped by local day | No product rows; fixture timestamps are never presented as live activity |
| Streaming chat | Live SSE + durable steering; bounded client queue dispatches subsequent real turns | Live JSON-RPC notifications + `turn/steer`; bounded client queue dispatches subsequent real turns | Sample replay; product queue unavailable |
| Conversation find/history | Live search over the real persisted transcript plus bounded turn pages | Live `thread/searchOccurrences` plus bidirectional `thread/turns/list`; runtimes returning `-32601` fall back to a real `thread/read(includeTurns)` projection | Typed fixture transcript; no invented matches |
| Edit latest user message | Unsupported; hidden because there is no destructive tail mutation | Live `thread/rollback { numTurns: 1 }`, authoritative returned-thread replacement, then real turn resubmit with retained text/media/reference inputs | Unsupported in product UI; typed rollback fixture is contract-test only |
| Image attachments | Live base64 image content | Live schema-exact `localImage` input | Unsupported, hidden |
| Audio-file attachments | Unsupported, hidden and rejected before I/O | Live schema-exact `localAudio`, selected-model gated | Unsupported, hidden |
| Skill references | Unsupported, hidden and rejected before I/O | Live schema-exact `skill { name, path }` from enabled server skills | Unsupported, hidden |
| File mentions | Unsupported, hidden and rejected before I/O | Live schema-exact `mention { name, path }` from native file picker | Unsupported, hidden |
| Models | Live | Live | Typed fixture catalog |
| Reasoning effort | Live model-advertised `thinking_enabled` | Live model-advertised `turn/start.effort` | Typed fixture options |
| Project/workspace | Native-host saved canonical folder roots with backend-qualified Move to project/No project overrides layered over authoritative `working_dir`; the selected root becomes `working_dir` on real session creation | The same native-host projects and backend-qualified membership overrides; selected root becomes absolute `cwd` plus runtime workspace root on real session creation | Creation, selection, and membership hidden; no fixture project or thread rows |
| Access mode | Live typed `permission_mode`: Supervised or Autonomous | Live allowed Read-only, Auto, or Full access named profile from `permissionProfile/list` plus managed requirements | Hidden |
| Interrupt | Live session cancel | Live turn interrupt | Typed fixture |
| Tool approval | Live | Live command, file, and exact-profile permission decisions | Sample approval |
| Archive/unarchive | Unsupported, capability-gated | Live | Typed fixture |
| Fork | Unsupported, capability-gated | Live | Typed fixture |
| Review changes | Unsupported, capability-gated | Live streamed `review/start` with approvals | Unsupported, hidden |
| Account authentication | Unsupported, unavailable state | Live browser OAuth, cancel, completion notification, logout | Explicit offline fixture only |
| Account usage/billing | Unsupported; no ChatGPT account projection | Live token summary, all named rate-limit buckets, credits/spend control, workspace messages, confirmed reset redemption, and member owner-nudge actions | Explicit offline fixture snapshot only |
| Files | Live read-only tree/read/fuzzy adapter; mutations and watches unsupported | Live typed tree/read/fuzzy, create/write/copy/remove, and directory watches | Typed read-only fixture |
| Processes | Live tracked-process catalog and kill; interactive terminal spawn/stdin/PTY unsupported | Live spawn/stdin/PTY plus selected-thread background-terminal list/clean/terminate | Typed fixture for standalone process flow |
| Extensions/MCP/skills/hooks | Live read-only installed extensions, MCP status, and skills; plugin and marketplace mutations, configuration writes, OAuth, and hooks unsupported | Live catalog, typed plugin install/uninstall, marketplace add/remove/upgrade, MCP OAuth login, HTTP/stdio MCP configuration writes, MCP status, skills, and per-workspace hooks | Typed read-only fixture; no sample hooks |
| Interactive MCP Apps | Unsupported because Mitsuro HTTP does not expose app resources or tool calls; no substitute UI | Live app metadata, resource read, MIME/size/CSP validation, ephemeral permission-denying WebKitGTK sandbox, bidirectional JSON-RPC, tool/resource proxying, consented follow-up turns, standard embedded/linked downloads, model context, inline/fullscreen display, and live resource subscription polling | Never synthesized; fixture transcripts cannot start the runtime |
| Hive Work | Live catalog plus per-session task/runtime detail; typed dispatch, message, pause/resume, priority, crew, and confirmed cancellation, with idempotent writes and authoritative refresh | Unsupported; no Mitsuro Hive control plane | Local goal/plan behavior only in explicit fixture mode |
| Hive schedules | Live global schedule catalog; native create/replace plus pause/resume and confirmed cancellation use the complete typed, revisioned, idempotent server contract | Unsupported | Typed fixture UI |
| Pull requests | No product adapter; explicit unavailable state | No product adapter; explicit unavailable state | No fake catalog |
| Sites | No product adapter; explicit unavailable state | No product adapter; explicit unavailable state | No fake catalog |
| Browser | Real offscreen WebKitGTK page rendered and interacted with inside GPUI; explicit system-browser fallback | Same backend-independent embedded page runtime | Same real browser runtime; fixture mode does not synthesize page content |
| Computer environments/permissions | Unsupported; no invented rows or grants | Live environment add/status/info and exact requested permission grants; no list method | Explicit fixture catalog, labeled fixture |
| Remote Control | Unsupported; explicit capability boundary | Live status, enable/disable, pairing, authorized-device list/revoke, and status lifecycle | Explicit fixture state; no invented devices |
| External-agent import | Unsupported; explicit capability boundary | Live Claude Code/Cursor detection, explicit review/confirmation, import lifecycle, and completed history | Explicit fixture state; no invented sources or history |
| Settings writes | Only controls with an observable local/runtime effect are interactive; server config writes unsupported | Send shortcut, sidebar-name visibility, archived-recents visibility, and Full access availability persist locally; permission profiles, requirements, provider capabilities, and MCP config use live typed contracts; remaining reference controls render disabled | Same explicit capability boundary; no decorative preference mutations |
| Experimental features | Unsupported; explicit capability boundary | User-facing beta catalog from `experimentalFeature/list`; atomic persistent toggles through `config/batchWrite` and effective-state refresh | Explicit fixture state; no invented toggles |
| Personalization memory | Unsupported; Mitsuro HTTP has no compatible local-memory contract | Effective Codex config with protocol defaults, atomic persistent toggles through `config/batchWrite`, confirmed `memory/reset`, and typed per-thread memory mode | Explicit unsupported/fixture state; no invented memory values |

Unsupported operations must return `NotImplemented` or be disabled through
`BackendCapabilities`. A method name appearing in the Codex inventory does not make it
implemented. The committed `codex-cli 0.147.0` baseline contains 95 stable client
methods, 133 methods with experimental APIs enabled, 70 server notifications, 10 server
requests in the stable contract (11 with experimental APIs), and 18 thread item
variants. The desktop automatically answers the experimental `currentTime/read` server
request. Every generated notification is classified as a core transcript event or a
typed lifecycle event. Every generated server request has an explicit disposition:
native approval/user-input/MCP interaction, a structured unsupported dynamic-tool
result, or an honest JSON-RPC error for unadvertised token/attestation capabilities.
The desktop negotiates experimental APIs because its process, environment, realtime,
and background-terminal surfaces require them. Fixture `call_raw` no longer manufactures
generic success payloads.

The executable client-method coverage matrix currently identifies 119 typed adapters
and 14 raw-transport-only methods. Every method used by the reviewed reference client
on Linux product paths now has a schema-backed adapter: feedback upload, marketplace
add/remove/upgrade, MCP resource read, plugin skill read, plugin share
list/save/update/delete, and Guardian denied-action approval. Of the remaining raw
methods, two are Windows-sandbox-only and 12 appear only in the generated inventory or
unused reference modules; their exact names are asserted by the matrix test. Typed
transport coverage is not feature-surface completion. The native MCP-app host now uses
`mcpServer/resource/read` and `mcpServer/tool/call` for real Codex data, retains current
and legacy app metadata through hydration, and renders the returned app in a dedicated
ephemeral WebKitGTK process on Wayland. Because app-server does not expose a
resource-subscribe client method, `resources/subscribe` is implemented with bounded live
resource reads and emits `notifications/resources/updated` only after a real value
changes. No fixture HTML, generic web page, or locally generated widget enters this path.

## Established recovery baseline

- GPUI and GPUI Component resolve from crates.io; there is no machine-specific
  `/home/.../vendor/gpui` patch.
- The Files surface uses exact typed Codex filesystem contracts for create, write,
  copy, remove, watch, and unwatch. Mutations are enabled only for Codex, names are
  constrained to the current directory, deletion requires a second confirmation,
  and Mitsuro/fixture remain explicitly read-only. Watch events are coalesced before
  refreshing, and large directory layouts render a bounded 200-row window with an
  exact overflow disclosure and fuzzy search for the remainder.
- Terminal keeps independent process contracts explicit: Codex `command/exec*` powers
  the desktop-launched sandboxed interactive session and streams output through the
  application-lifetime lifecycle subscription. Its deferred exec response is not
  subject to the generic JSON-RPC timeout. Codex `thread/backgroundTerminals/*` lists,
  cleans, and terminates processes retained by the selected thread, and Mitsuro uses
  its real global `/processes` catalog and `/:id/kill` endpoint. Mitsuro does not
  advertise interactive spawn/stdin/PTY because that contract is absent; `process/*`
  remains only for compatibility and explicit fixture testing.
- The Extensions marketplace renders only backend data. Codex plugin Install/Remove
  actions call typed `plugin/install` and `plugin/uninstall`, disable concurrent
  mutations, and refresh the live catalog after success. The Personal view retains exact
  `plugin/list` marketplace buckets and exposes typed add, upgrade-all, and confirmed
  remove actions only for Codex. Successful mutations refresh from server authority;
  no marketplace row is added optimistically. Mitsuro and explicit fixture catalogs are
  visibly read-only because neither backend exposes a production mutation contract.
  Search filters the live plugin, skill, and MCP records; expanding a category uses its
  exact hidden-record count and never pads the catalog with decorative totals.
- Codex remote-environment registration sends exact `environment/add` parameters after
  local `ws://`/`wss://` validation. Because the protocol returns an empty response and
  has no list method, GPUI retains only successful submissions for the current app
  session and immediately probes `environment/status` and `environment/info`. Mitsuro
  renders the mutation as unsupported.
- Codex Remote Control uses all seven generated `remoteControl/*` request contracts,
  application-lifetime status notifications, bounded cursor pagination, and confirmed
  client revocation. Its Settings page renders only live server state; Mitsuro and
  fixture modes cannot inherit or synthesize Codex devices.
- Settings Import uses all four generated `externalAgentConfig/*` request contracts.
  GPUI detects Claude Code and Cursor independently, renders only server-returned
  migration items, requires confirmation before mutation, follows typed progress and
  completion notifications, and refreshes server-owned history. Mitsuro and fixture
  modes cannot inherit or synthesize import sources or completed history.
- General Settings reads the paginated typed `experimentalFeature/list` catalog and
  renders only beta rows with server-supplied display copy. Changes persist to the
  canonical `features.<name>` key through typed `config/batchWrite` with user-config
  reload, then refresh effective enablement. Mitsuro and fixture modes never inherit
  or synthesize Codex feature flags.
- Personalization Memory derives effective state from the live Codex `config/read`
  response, including the protocol defaults when the optional `memories` object or
  keys are absent. Both user-facing toggles write the exact `memories.*` keys through
  one typed `config/batchWrite` transaction, reload and re-read effective state, and
  surface policy overrides or failures. Deleting all memories requires two clicks and
  calls typed `memory/reset`; live validation does not invoke it. Mitsuro and fixture
  modes cannot inherit or synthesize Codex memory state. Per-thread memory mode, model,
  reasoning effort, service tier, and collaboration settings use typed authoritative
  thread contracts and reconcile from server notifications.
- Codex `thread/shellCommand` is represented by its exact typed request and empty
  acknowledgement, including its explicit unsandboxed host-local semantics. It is not
  routed from the GPUI composer: the reviewed desktop bundle contains the method in its
  protocol/request telemetry but no product caller, while Codex TUI's `!command`
  affordance is a different client. Mitsuro reports this capability as unsupported.
- Codex side chats use the reference product's real lifecycle: an ephemeral
  `thread/fork` with excluded visible turns and side-only developer instructions,
  followed by a hidden typed `thread/inject_items` boundary. GPUI supports both
  `/side [prompt]` and **Open side chat**, keeps the fork out of Recents, and deletes
  it when **Back to main chat** is selected. Mitsuro reports the feature as unsupported
  because its HTTP API has no model-history injection primitive. Main and side turns
  retain independent generation, turn-id, interruption, approval, structured-input,
  and MCP-elicitation ownership. The side banner reports whether the main thread is
  working, needs input/approval, or finished, so a side chat can run without losing or
  misrouting the parent's live events. Typed-answer editors are conversation-owned,
  and fork creation refreshes effective layered configuration for the selected project
  before preserving its model, policy, permission, workspace, and instruction context.
  Ambient MCP startup-status updates refresh Extensions state without being inserted as
  faux chat transcript rows.
- The composer recognizes the reference `/feedback` command without sending it as a
  chat turn. Codex presents the five reference categories, requires details, defaults
  session-log inclusion on, and calls typed `feedback/upload`; validation never submits
  external feedback. Mitsuro reports feedback upload as unsupported because its HTTP
  API has no equivalent contract.
- The reference `/approve` command is driven only by live
  `item/autoApprovalReview/completed` denials. GPUI keeps at most ten eligible actions
  per Codex thread and sends the exact server assessment event through typed
  `thread/approveGuardianDeniedAction` for one retry. It never invents denials, and
  Mitsuro reports the operation as unsupported.
- Generic Settings controls fail closed. Only Send shortcut, sidebar profile-name
  visibility, and archived-recents visibility can mutate the privacy-safe local
  preference store; Full access and realtime voice retain their specialized live paths.
  Reference controls without a runtime contract remain visible for parity but have no
  pointer, hover, or click affordance, display an unavailable label, and cannot add
  decorative values to the preference file.
- Keyboard Shortcuts is backed by registered GPUI actions rather than a reference-only
  catalog. It wires the supported in-window navigation and composer/run commands,
  preserves component-specific Escape handling for IME and dialogs, reflects the live
  Send preference, and labels unsupported global/remapping commands unavailable.
- MCP servers advertising `notLoggedIn` expose a real Codex sign-in action. GPUI sends
  typed `mcpServer/oauth/login`, opens only the returned authorization URL, tracks the
  server name until `mcpServer/oauthLogin/completed`, and refreshes the live catalog.
- Codex MCP additions use typed `config/value/write` with the exact
  `mcp_servers.<name>` upsert shape, followed by `config/mcpServer/reload`. The GPUI form
  supports streamable HTTP URLs and stdio command plus JSON string-array arguments,
  validates all fields before I/O, serializes one mutation at a time, and refreshes the
  live catalog after success. Mitsuro renders this mutation as unsupported.
- Settings Hooks renders only typed `hooks/list` entries scoped to the active workspace.
  It preserves hook event, handler, source path/source, enabled/managed state, trust,
  warnings, and errors. The previous local toggle and hard-coded Mitsuro paths were
  removed because they were not connected to either backend.
- Settings Connections hydrates the real Codex app/connector catalog through typed
  `app/list` and `app/installed` adapters, with `app/read` available for exact detail
  reads. Rows derive connected, installed, disabled, accessible, and unavailable states
  only from server fields. Connect actions validate and open the exact server-returned
  HTTP(S) install URL. Mitsuro renders this Codex-only capability as unsupported, and
  explicit fixture mode does not invent connector account state.
- The Skills catalog uses typed `skills/config/write` mutations for live Codex enable
  and disable actions, serializes changes, applies the server-returned effective state,
  and refreshes `skills/list`. Mitsuro and fixture skill inventories remain explicitly
  read-only because they do not expose that mutation contract.
- Codex notifications use an application-lifetime broadcast hub. Independent
  turn subscribers do not consume each other's events. The GPUI shell owns one
  backend-generation-scoped lifecycle subscriber for idle-time account, skills/MCP,
  thread-list, and file updates; turn transcript deltas remain single-delivery.
- Reopened and live Codex transcripts preserve every current thread item type. Tool,
  search, image, collaboration, review-mode, compaction, hook, and sleep items render as
  real activity rows; unknown future variants remain visible rather than disappearing.
- Server-originated requests are modeled separately from notifications because Codex
  can pause a turn until the client answers. The native interaction strip supports
  request-user-input options/freeform/secrets, standard MCP forms and URL elicitations,
  plus command/file/permission approvals without synthetic response data.
- Codex review turns subscribe before `review/start`, follow the response's review
  thread identity, and reuse the progressive transcript and approval pipeline. The
  selected-thread action requests an inline review of real uncommitted changes; it is
  hidden for Mitsuro until the HTTP API exposes an equivalent contract.
- Codex account sign-in starts the real browser OAuth flow, retains the server login
  identity for cancel/reopen, and waits for `account/login/completed` before claiming
  success or loading authenticated usage. Failed logout keeps the last server snapshot
  visible instead of pretending local sign-out succeeded.
- Settings Usage preserves every `rateLimitsByLimitId` record rather than collapsing
  the response to two generic bars. It renders server-provided names, window durations,
  remaining percentages, reset times, credits, individual spend control, earned resets,
  and enabled workspace messages. Reset consumption is a two-step confirmed mutation
  with a unique idempotency key; workspace-member exhaustion states alone expose the
  typed owner-nudge action. Mitsuro explicitly reports this Codex-only surface as
  unsupported, and read-only acceptance does not invoke either mutation.
- The composer path picker accepts up to four supported images at 20 MiB each. Codex
  receives absolute `localImage` user inputs; Mitsuro reads the selected file and sends
  a real MIME-labeled base64 content block. Both thread-read adapters preserve persisted
  local-path, remote, and embedded image inputs, and GPUI renders them in the originating
  user message. Missing files, failed remote loads, invalid MIME types, oversized data,
  and decode failures use explicit visual fallbacks. Attachments are cleared on backend
  switch and never fall back to fixture data.
- Codex models advertising the `audio` input modality expose a local audio-file picker
  using the same limit of four combined attachments at 20 MiB per file. The product
  adapter emits `localAudio`; persisted local-path, remote, and embedded audio inputs are
  retained as truthful transcript attachment rows. Invalid or oversized embedded data
  stays visible as unavailable metadata. Mitsuro's text/image `ContentBlock` contract
  does not accept audio, so its UI action is hidden and both adapter layers reject it
  before I/O. This is file attachment support, not microphone recording or playback.
- Codex's composer add menu uses the live enabled-skill catalog and a native regular-file
  picker to emit exact `skill { name, path }` and `mention { name, path }` inputs. Up to
  eight combined references are retained as structured user-message attachments when a
  thread is reopened. Mitsuro and fixture modes hide these actions and both product and
  low-level Mitsuro adapters reject the Codex-only records; they are never flattened into
  prompt text or synthesized from fixture data.
- Reasoning choices come only from the selected model's live capability metadata and
  persist per backend/model. The transport-neutral turn adapter maps the same selection
  to Codex `effort` and Mitsuro `thinking_enabled`; models with zero or one advertised
  option do not show a misleading selector.
- Project and access controls are real product adapters rather than prompt decoration.
  Codex-surface Projects match the reversed native-host ownership contract: the desktop
  stores stable ids, display names, canonical folder roots, backend-qualified explicit
  assignments, and a separate backend-qualified projectless-session set. A real
  session's authoritative working directory supplies its default membership; Move to
  project or No project adds the minimal native-host override without changing the
  server workspace, permissions, transcript, or files. Selecting a project filters the
  current Mitsuro or Codex catalog and seeds the next real thread with its first root.
  Removing a project is a two-step local operation that clears its membership overrides
  but never deletes a server thread or filesystem content. Fixture identities cannot be
  assigned. The app-server `threadSection/*` family is a separate server-persisted
  thread-section capability and is not misrepresented as the native Projects sidebar.
  A new conversation is an optimistic local draft until first Send, when the selected
  absolute project path and backend-specific access preset are used for the real session
  and first turn. Codex Read-only/Auto/Full access comes from its live allowed permission
  profiles and maps to the exact named profile plus runtime-root fields. Managed
  requirements can remove a profile, and enabling Full access in Settings only exposes
  the choice after confirmation; it does not select it. Mitsuro Supervised/Autonomous
  maps to its typed permission contract. Transport-only Mitsuro metadata is skipped from
  Codex JSON, and cross-backend access variants are rejected before I/O.
- Mitsuro uses the canonical `mitsuro-client` HTTP/SSE implementation.
- Work uses typed Mitsuro Hive control-plane adapters end to end. Catalog rows come from
  `/api/hive/current`; selecting a row loads `/api/hive/sessions/:id/status`, including
  the exact autonomous tasks, runtime error/wake state, cadence, crew, and priority.
  Dispatch and message/pause/resume/priority/crew/delete writes use unique idempotency
  keys, serialize in the native UI, and refresh server-owned state after success.
  Cancellation is armed on the first click and executed only on the second. No live row
  is padded with aggregate or fixture plan items, and Codex remains capability-gated.
- Mitsuro schedule rows use the real per-session Hive control-plane routes for pause,
  resume, and cancellation. The client sends the current revision as `If-Match`, adds a
  unique idempotency key, serializes mutations, requires cancellation confirmation, and
  re-reads the global catalog after success. Codex remains explicitly unsupported.
- The native schedule editor creates and replaces schedules without reducing the server
  contract: once/daily/weekdays/weekly/monthly recurrence, IANA timezone and DST behavior,
  workspace/model/crew identity, priority, misfire, overlap, and retry policy are preserved.
  Replacements retain the exact provider/auth/transport model key unless the model changes.
- Thread reads preserve the canonical transcript rather than limiting history to
  eight 280-character bubbles.
- Mitsuro persisted model messages are projected block by block: authored text/images
  remain user messages, thinking becomes reasoning, tool calls become typed command or
  activity rows, and provider-role tool results update their originating activity.
  Tool-result JSON is never presented as an authored user bubble, and unknown future
  blocks remain visible as bounded forward-compatible activity.
- Opening a persisted Codex conversation uses `thread/resume`, matching the reference
  client's subscription lifecycle. Leaving an idle Codex conversation issues the exact
  `thread/unsubscribe` request, while returning to it resumes and refreshes authoritative
  history. An exact app-server active-writer conflict falls back to the same persisted
  `thread/read(includeTurns)` transcript and is explicitly read-only: writer actions are
  disabled and no `thread/unsubscribe` is sent for a subscription this client never
  acquired. Mitsuro remains snapshot-only and never receives the Codex lifecycle method;
  active turns are not unsubscribed while their stream is still running.
- Find in conversation searches only backend-owned user/final-assistant text. Selecting
  an unloaded match hydrates five real turns in both directions from the returned turn
  cursor, deduplicates already loaded item ids, and scrolls to the exact persisted item.
  Codex uses `thread/searchOccurrences` and `thread/turns/list` when the live runtime
  implements them. The 0.147.0 schema advertises both while some app-server builds still
  return JSON-RPC `-32601`; only for that exact response the adapter projects the same
  contracts from a real `thread/read(includeTurns)` payload. Mitsuro derives its
  read-only contract from its persisted transcript. Codex `thread/rollback` is typed for
  the reference edit/retry workflow, while Mitsuro explicitly rejects destructive
  rollback because its HTTP API has no equivalent mutation.
- The exact `thread/settings/update` and `thread/metadata/update` nullable-patch
  contracts are typed and origin-checked, including the difference between an omitted
  field and an explicit `null` clear. A subscribed Codex thread hydrates model,
  reasoning effort, service tier, and active permission profile from `thread/resume`.
  Model, effort, speed, collaboration mode, and access changes are serialized before
  app-server I/O, while the authoritative `thread/settings/updated` notification
  reconciles the selected composer. Active-writer snapshots never send these mutations.
  `thread/items/list` is also typed with turn identity, direction, and cursor
  preservation; a runtime returning JSON-RPC `-32601` is projected from the same real
  `thread/read(includeTurns)` payload. The Codex open path now follows the reference
  fast path by requesting five full newest turns atomically with `thread/resume`, then
  retains the opaque `thread/turns/list` cursor for five-turn **Load earlier messages**
  pages. Cursor-guarded sequential item pages complete any turn a runtime nevertheless
  returns as summary or `notLoaded`.
  Mitsuro continues to use its complete real HTTP snapshot and never fabricates an
  older-history cursor. Local GPUI layout remains bounded independently of server
  hydration.
- Backend session IDs are namespaced (`BackendSessionId`) and are stored on every
  live GPUI thread. Session/model/turn flows use the transport-neutral
  `ProductBackend` contract, and mutations reject a session whose origin differs
  from the active backend.
- The current UI still shows one selected backend at a time. A future mixed-backend
  list must use `BackendSessionId::qualified()` as its row/selection key instead of
  the raw server ID.

## Release posture

- The fixture/live visual matrix, dual-provider acceptance gauntlet, focused workspace
  gates, optimized GPUI build, and isolated runtime provenance check are complete. The
  tagged Linux workflow tests and packages this GPUI binary as the canonical `.deb` and
  `.rpm`, including executable-relative SVG asset lookup. The release visual harness
  launches an extracted package in every top-level mode against both real backends and
  captures 26 mapped windows without authorizing provider turns. Residual destinations
  without a backend contract are explicit exclusions described above rather than faux
  completion states.
- The production-data purity matrix was revalidated on both live transports for Work,
  Scheduled, Computer, Extensions, Settings, and Files. A unit regression matrix forbids
  fixture records for every product backend connection state.
- Codex WebSocket stays explicitly unsupported unless attaching to an already-running
  app-server becomes a required deployment mode; managed stdio is the supported path.
- Installation and deployment are separate operator actions. A built GPUI artifact is
  not evidence that the installed Mitsuro CLI/server or any live process changed.
- Splitting `app.rs` into state/controllers and bounded views is a post-release
  maintainability slice and must not change the backend capability boundary.

## Validation

```bash
cargo check -p mitsuro-desktop-backend
cargo test -p mitsuro-client -p mitsuro-desktop-backend
cargo check -p mitsuro-gpui-desktop
cargo test -p mitsuro-gpui-desktop
scripts/gpui-codex-protocol-check.sh

# Real WebKitGTK render, two-way JSON-RPC, click, Blob, and resize probe
MITSURO_RUN_MCP_APP_RUNTIME_TEST=1 cargo test -p mitsuro-gpui-desktop \
  live_webkit_probe_renders_and_bridges_json_rpc --bin mitsuro-gpui-desktop -- --nocapture

# Explicit opt-in because this method deliberately executes outside the thread sandbox
MITSURO_RUN_LIVE_SHELL_ACCEPTANCE=1 cargo test -p mitsuro-desktop-backend \
  real_app_server_thread_shell_command_round_trip -- --nocapture

# No model turn: fork a real persisted parent into an ephemeral child, then
# prove the hidden model-history boundary is accepted by the installed runtime
MITSURO_RUN_LIVE_INJECT_ACCEPTANCE=1 cargo test -p mitsuro-desktop-backend \
  real_app_server_side_thread_fork_and_injection_round_trip -- --nocapture

# Read-only check against a running local Mitsuro server
MITSURO_RUN_SERVER_IT=1 cargo test -p mitsuro-desktop-backend \
  live_server_read_only_contract -- --nocapture
```

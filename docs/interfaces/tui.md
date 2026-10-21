# Terminal UI

Running `mitsuro` without a subcommand opens the Ratatui/crossterm terminal
client. Its entry point is [tui_v2::run](../../crates/mitsuro-cli/src/tui_v2/mod.rs),
called by [the CLI](../../crates/mitsuro-cli/src/main.rs). It uses the shared
Mitsuro agent runtime, provider catalog, tools, and persisted sessions.

## Application and runtime

[PreviewApp](../../crates/mitsuro-cli/src/tui_v2/app/mod.rs) is the current
application type despite its historical name. It owns the UI state, conversation
projection, layout and measurement caches, service snapshots, and async event
receivers. The application handles terminal input and runtime events, applies
updates, and requests redraws when the displayed state changes.

[UiState](../../crates/mitsuro-cli/src/tui_v2/app/state.rs) holds presentation
concerns: routes, focus, overlays, composer state, transcript position, expanded
artifacts, and appearance. The [reducer](../../crates/mitsuro-cli/src/tui_v2/app/reducer.rs)
handles typed actions and returns [effects](../../crates/mitsuro-cli/src/tui_v2/app/effect.rs).
The application executes those effects, including sending messages, resolving
pending decisions, loading overlays, and persisting preferences.

[RuntimeServices](../../crates/mitsuro-cli/src/tui_v2/services.rs) adapts the
shared runtime to terminal needs. It loads sessions and recovery state, resolves
exact model identities, manages provider authentication, and starts agent runs.
[Service initialization](../../crates/mitsuro-cli/src/tui_support/app_builder.rs)
constructs the tool registry, storage managers, MCP connections, skills, and
extension/plugin services. These services remain separate from terminal widgets.

[TerminalSession](../../crates/mitsuro-cli/src/tui_v2/terminal.rs) owns raw mode,
the alternate screen, mouse capture, focus reporting, and bracketed paste.
Its cleanup restores the terminal modes when the session ends.

## Conversation, layout, and rendering

[ConversationProjection](../../crates/mitsuro-cli/src/tui_v2/projection/mod.rs)
normalizes persisted messages, live loop events, recovery snapshots, and delegated
progress into the [conversation model](../../crates/mitsuro-cli/src/tui_v2/model/conversation.rs).
Text, thinking, tool artifacts, notices, and attachments have stable part IDs.
Pending approvals, questions, and plan confirmations use a separate interaction
collection, rendered by the [decision dock](../../crates/mitsuro-cli/src/tui_v2/components/conversation/decision_dock.rs).

The [presentation layer](../../crates/mitsuro-cli/src/tui_v2/presentation/transcript.rs)
builds display parts. The [layout engine](../../crates/mitsuro-cli/src/tui_v2/layout/engine.rs)
measures them and produces a snapshot containing visible geometry and interaction
targets. [Frame rendering](../../crates/mitsuro-cli/src/tui_v2/render/frame.rs)
and mouse hit testing consume that geometry. Artifact expansion, fullscreen
state, scrolling, and follow-live state belong to the UI rather than canonical
conversation history.

Assistant Markdown uses the [terminal Markdown adapter](../../crates/mitsuro-cli/src/tui_v2/presentation/markdown.rs)
and [shared terminal parser/renderer](../../crates/mitsuro-cli/src/tui_support/markdown/mod.rs).
Code highlighting uses Syntect. Link targets pass through the adapter's HTTP/HTTPS
filter before becoming terminal interactions. Measurement caching and presentation
retention are implemented separately from persisted message storage.

## Input and commands

The [action registry](../../crates/mitsuro-cli/src/tui_v2/input/action.rs) supplies
keyboard bindings, command-palette actions, help, and footer hints. Actions depend
on focus: composer, transcript, artifact, decision dock, or overlay. Use `/cmd`
for the current control list. Common controls include Enter to send,
Shift+Enter or Alt+Enter for a newline, Ctrl+K for the command palette, Ctrl+O for
sessions, Ctrl+T for the workspace sidebar, and Ctrl+Q to quit.

The [composer buffer](../../crates/mitsuro-cli/src/tui_v2/input/composer_buffer.rs)
tracks text, byte positions, selection, wrapping, and viewport movement.
[Slash completion](../../crates/mitsuro-cli/src/tui_v2/input/slash.rs) matches
command prefixes and aliases; [file search](../../crates/mitsuro-cli/src/tui_v2/input/file_search.rs)
provides project references. Clipboard/image attachment handling is coordinated
by the application and runtime services.

The slash parser and application dispatch currently support:

| Command | Aliases | Action |
| --- | --- | --- |
| `/home` | `/new`, `/clear` | Return Home with a fresh conversation draft |
| `/load` | | Open saved sessions |
| `/model` | | Select an exact provider/model identity |
| `/fast` | | Toggle Fast mode when the selected model supports it |
| `/auth` | | Manage provider connections |
| `/init [scope]` | | Ask the Agent to create or improve `KRAB.md` |
| `/theme` | | Choose theme and motion settings |
| `/pinch [instructions]` | `/compact` | Compact context within the current session |
| `/cmd` | `/help` | Show controls |
| `/ps` | `/processes` | Inspect background processes |
| `/extensions` | `/skills`, `/plugins`, `/mcp`, `/hooks` | Open extension and service management |
| `/plan` | `/goal` | Inspect the plan and Goal |
| `/permissions` | `/perm` | Toggle permission mode |
| `/update` | | Request installation of an available update |

## Appearance and capabilities

[SemanticTheme](../../crates/mitsuro-cli/src/tui_v2/presentation/theme.rs) provides
Mitsuro Dark, Mitsuro Light, Terminal Adaptive, and High Contrast palettes.
Components consume semantic colors. [Motion preferences](../../crates/mitsuro-cli/src/tui_v2/motion/preference.rs)
support full, reduced, and off settings.

[Capability detection](../../crates/mitsuro-cli/src/tui_v2/model/capability.rs)
reads `TERM`, `COLORTERM`, `NO_COLOR`, and `MITSURO_ASCII`. It selects Unicode or
ASCII glyphs and true-color, ANSI-256, ANSI-16, or monochrome colors before layout
and rendering. Image support has a separate terminal graphics adapter.

## Validation

Focused terminal tests can be run with `cargo test -p mitsuro tui_v2`.
Coverage includes live/recovered conversation projection, pending decisions,
input dispatch, layout, artifact interaction, and terminal-mode cleanup.
[TestBackend fixtures](../../crates/mitsuro-cli/src/tui_v2/test_support/harness.rs)
check rendered cells and interaction geometry without opening a terminal.
Interactive terminal behavior still needs validation in the terminal environment
being supported; a passing fixture is not evidence of a live rendering session.

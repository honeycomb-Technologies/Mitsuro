# Mobile App (Expo/React Native)

The Mitsuro mobile app is the primary user interface for interacting with the Mitsuro server from a phone, tablet, or web browser. It is built with Expo and React Native, lives at `apps/mobile/` in the monorepo, and uses a single codebase to produce native iOS and Android apps as well as a web build that can be embedded directly into the Mitsuro server.

## Architecture Overview

The app is built on **Expo SDK 55** with **React Native 0.83** and **React 19**. Routing is handled by **Expo Router**, which provides file-based routing -- you create a file inside `app/` and it becomes a route. The tab-based navigation structure lives inside `app/(tabs)/`, and there is a separate `app/onboarding.tsx` route for first-time setup.

The root layout at `app/_layout.tsx` composes a provider stack that wraps the entire app:

- **GestureHandlerRootView** -- enables gesture handling across the app
- **SafeAreaProvider** -- manages safe area insets for notched devices
- **SplashProvider** -- controls the animated splash screen lifecycle
- **ThemeProvider** -- supplies dark/light theme colors
- **ConnectionProvider** -- manages the server connection and authentication
- **StoresProvider** -- initializes Zustand stores once a connection is established

The `RootNavigator` component inside the layout handles a simple routing guard: if the user has not configured a server connection, they are redirected to the onboarding screen. Once connected, they land on the main tab interface.

## The Dual-Platform Trick

One of the more unusual things about this codebase is that the same React Native app serves two very different roles:

1. **Native mobile app** -- compiled and distributed through EAS Build as an iOS/Android binary
2. **Web frontend** -- exported as a static single-page application and embedded into the Mitsuro server's HTTP interface

This works because Expo supports a web target through Metro bundler and `react-native-web`. The `app.json` configuration sets `"web": { "bundler": "metro", "output": "single" }`, which tells Expo to produce a single-page web application. The `web:build` script in `package.json` runs `expo export --platform web` to generate this output.

The web build runs in a browser and can be served by the Mitsuro server. Native Canvas is a separate desktop application. The `useConnection` hook retains support for `window.__MITSURO_SERVER_URL` and `window.__MITSURO_SERVER_TOKEN` as compatibility inputs from older hosts.

The Expo codebase has three deployment targets: native iOS, native Android, and browser web.

## Platform Abstraction

Not every API is available on every platform. Haptic feedback, secure storage, speech recognition, blur effects, and file pickers all behave differently (or do not exist at all) on web versus native. The app solves this with a **platform module pattern** inside the `platform/` directory.

For each capability, there are two files sharing the same export signature:

- `haptics.native.ts` -- re-exports from `expo-haptics`
- `haptics.web.ts` -- exports no-op functions with matching signatures

Metro's platform-specific resolution picks the right file at build time. When a component imports from `../../platform/haptics`, it gets the native version on iOS/Android and the stub on web. The rest of the code never needs to check which platform it is running on.

The platform directory covers twelve capabilities:

| Module | Native | Web |
|---|---|---|
| `haptics` | `expo-haptics` | No-op stubs |
| `secure-store` | `expo-secure-store` | `localStorage` wrapper |
| `speech` | `expo-speech-recognition` (dynamic require) | Web Speech API polyfill |
| `blur` | `expo-blur` BlurView | Plain View with opacity |
| `clipboard` | `expo-clipboard` | `navigator.clipboard` |
| `image-picker` | `expo-image-picker` | File input element |
| `document-picker` | `expo-document-picker` | File input element |
| `linear-gradient` | `expo-linear-gradient` | CSS gradient div |
| `linking` | `expo-linking` | `window.location` wrapper |
| `fetch` | Native fetch | Browser fetch |
| `mitsuro-storage` | `AsyncStorage` | `localStorage` |

This pattern keeps platform-specific code isolated and the main component tree completely platform-agnostic.

## Component Architecture

Components are organized into six directories under `components/`:

**`chat/`** -- The core chat experience. This is the largest group and contains everything related to the conversation interface:

- `MessageBubble` -- Renders a single message. Handles user messages, assistant messages with markdown rendering, tool call cards, tool approval widgets, plan confirmation prompts, and thinking indicators. It delegates to specialized sub-components based on the message content.
- `ChatBar` -- The input bar at the bottom of the screen. Supports text input, voice dictation (via speech recognition), image and file attachments, and an accordion of controls for thinking level, permission mode, fast mode, build/plan mode, model selection, and research toggle. It uses animated spring effects via Reanimated for the send button.
- `ToolApprovalWidget` -- Appears inline when the AI requests permission to execute a tool. Shows the tool name and provides approve/deny buttons. Rendered inside `MessageBubble` when a tool call has `awaiting_approval` status.
- `ToolCallCard` -- Displays a completed tool invocation with its name, arguments summary, and result.
- `PlanConfirmWidget` -- Shown when the AI produces a plan and needs confirmation to execute or abandon it.
- `AskUserQuestionWidget` -- An interactive prompt embedded in the chat when the AI needs direct user input.
- `MarkdownContent` -- Renders markdown text inside messages using `react-native-markdown-display`.
- `BashOutput` -- Formatted display of shell command output.
- `SessionDrawer` -- Slide-in drawer for mobile that lists sessions and allows switching between them. Contains the session list, new session button, directory picker, and settings access.
- `SessionList` -- Shared session list used by both the mobile drawer and the desktop sidebar.
- `PlanTracker` -- A floating overlay that shows current plan progress during build mode.
- `AccordionControls` -- Expandable controls row inside the ChatBar.
- `Waveform` -- Animated audio waveform displayed during voice input.

**`layout/`** -- Structural components:

- `DesktopShell` -- On screens wider than 1024px (the desktop breakpoint), wraps the chat content in a sidebar layout with a persistent session list, toolbar, terminal panel, and workspace preview. On mobile, it is a pass-through. The `useBreakpoint` hook drives this decision.

**`desktop/`** -- Desktop-specific panels:

- `Terminal` -- A Ghostty-powered terminal: native libghostty on iOS/Android and Ghostty WASM on web.

**`settings/`** -- The settings UI:

- `SettingsPanel` -- Full settings screen with server connection details, theme selection, notification preferences, and account management.

**`splash/`** -- The splash screen:

- `SplashOverlay` -- Plays a Lottie animation (`assets/animations/splash.json`) on app launch, then reveals the main content. Uses `expo-splash-screen` to coordinate with the native splash.

**`ui/`** -- Shared primitive components:

- `GlassCard` -- A translucent card with blur effects, used throughout the app for list items and panels.
- `MitsuroLogo` and `MitsuroWordmark` -- The canonical rounded-cell mark and lowercase product wordmark used in branded app states.

There are also two top-level components: `ReportsViewer` for browsing Hive reports and `SettingsModal` for the desktop settings overlay.

## Custom Hooks

The `hooks/` directory contains ten hooks that manage the app's reactive behavior:

**`useConnection`** -- Provides the `MitsuroClient` instance and connection lifecycle. Stores the server URL and authentication token in secure storage. Exposes `connect`, `disconnect`, and `reconnect` methods. On mount, it honors stored logout state, then reads compatibility connection globals or restores saved credentials.

**`useStores`** -- Creates and provides Zustand stores scoped to the active `MitsuroClient`. Initializes five stores: `sessions` (session list), `session` (active session state including messages, streaming, model), `workspace` (current working directory and mode), `git` (repository state), and `plan` (task plan tracking). Stores are recreated when the client changes.

**`useTheme`** -- Wraps the `@mitsuro/ui` theme system. Resolves `system`, `dark`, or `light` preference against the device color scheme and provides the resolved `Theme` object with all color tokens.

**`useBreakpoint`** -- Reads window dimensions and returns a `mobile` / `tablet` / `desktop` breakpoint. The thresholds are 768px for tablet and 1024px for desktop. Components use `isDesktop` to decide between the sidebar layout and the mobile drawer layout.

**`useNotifications`** -- Registers push notification categories (tool approval, stream complete, Hive update) with actionable buttons. Handles notification responses to approve/deny tools or navigate to specific sessions. Only loads `expo-notifications` on native platforms.

**`useLiveActivity`** -- Manages iOS Live Activities during streaming. Starts a `ChatStreamActivity` that shows the model, status, elapsed time, token count, and progress on the lock screen and Dynamic Island. Includes approve/deny buttons for tool approvals directly in the Live Activity. Updated every second while active.

**`useWidgetSync`** -- Pushes current chat state (session title, last message, model, streaming status, token count) to the ChatWidget on iOS. Runs on every relevant state change so the home screen widget stays current.

**`useDeepLink`** -- Handles `mitsuro://connect` deep links and `https://.../#mitsuro-remote-token=...` universal links for one-tap server connection. Parses the URL, extracts credentials, and calls `connect`.

**`useEntranceAnimation`** -- Orchestrates a staggered entrance animation after the splash screen completes. The top bar slides down, the content fades in with a subtle scale, and the bottom bar slides up, all using Reanimated shared values.

**`useSplashState`** -- Simple boolean state that tracks whether the Lottie splash animation has finished, gating the entrance animation.

## State Management

Application state is managed with **Zustand**, with store factory functions defined in the shared `packages/state` package. The mobile app imports these factories and instantiates them inside the `StoresProvider`. This architecture shares client state between the Expo mobile and web targets. Native Canvas uses Rust client/server contracts.

The five stores are:

- **SessionsStore** -- List of all sessions, loaded from the server. Provides `loadSessions`.
- **SessionStore** -- Active session state: messages, model, streaming status, thinking level, permission mode, build/plan mode, token count. Provides `sendMessage`, `loadSession`, `stopStreaming`, `submitToolApproval`, and other session mutations. Manages the SSE streaming connection.
- **WorkspaceStore** -- Current working directory, workspace mode (neutral, selected, created). Persists per-session workspace state.
- **GitStore** -- Repository status for the current workspace directory.
- **PlanStore** -- Task plan with subtasks, dependencies, and progress.

Components subscribe to individual slices of these stores via selector hooks like `useSessionStore(state => state.messages)`, which ensures minimal re-renders.

## iOS and Android Widgets

The app includes two home screen widgets and one Live Activity, all built with `expo-widgets` and authored in JSX using `@expo/ui/swift-ui` components:

**HiveWidget** -- The Hive autonomous-mode widget. It supports six size
families: `systemSmall`, `systemMedium`, `systemLarge`, `accessoryCircular`,
`accessoryRectangular`, and `accessoryInline`. The small widget shows agent
status and briefing text. The medium and large variants add task progress bars
and completion counts. Lock-screen accessories use the product's rounded-cell
symbol with status text.

**ChatWidget** -- Shows the active chat session with title, last message preview, model name, and token count. Supports `systemSmall`, `systemMedium`, `accessoryCircular`, and `accessoryRectangular`. Includes a `widgetURL` modifier so tapping the widget opens the app directly to the chat. When no session is active, it shows a "New Chat" prompt.

**ChatStreamActivity** -- A Live Activity that appears on the lock screen and in the Dynamic Island while the AI is streaming a response. Shows the chat title, model, elapsed time, token count, and a progress bar. When a tool requires approval, it renders approve and deny buttons directly in the activity, allowing the user to grant permission without opening the app.

Widget data is pushed from the app via `useWidgetSync` and `useHiveWidgetSync`, which call `updateSnapshot` on the widget instances whenever relevant state changes.

All widgets use a shared `AccessoryBackground` component for consistent styling in the lock screen accessory sizes.

## Build System

The app uses **EAS Build** (Expo Application Services) with three profiles defined in `eas.json`:

- **development** -- Builds a development client with `distribution: internal` and iOS simulator support enabled. Used for local testing with `expo start`.
- **preview** -- Ad-hoc internal distribution build without simulator support. Used for direct internal testing on registered real devices, not TestFlight.
- **production** -- App Store distribution build with `autoIncrement: true` on iOS, which automatically bumps the remote build number. This is the profile submitted to TestFlight through the `submit.production` configuration.

The existing App Store and Play identities remain on their compatibility bundle/package identifier, `io.krusty.mobile`, while product copy and deep links are Mitsuro-first. The EAS project ID ties builds to the Expo dashboard.

The Metro bundler configuration in `metro.config.js` is customized for the monorepo. It adds watch folders for `packages/api`, `packages/state`, and `packages/ui`, and maps `@mitsuro/*` imports to the source directories of those packages. This allows the mobile app to import from shared packages without requiring them to be pre-built.

## Assets

The `assets/` directory contains:

- **`animations/splash.json`** -- A Lottie animation played during app launch by the `SplashOverlay` component.
- **`icons/ios-light.png`**, **`ios-dark.png`**, **`ios-tinted.png`** -- Three icon variants for iOS. The system automatically selects the appropriate variant based on the user's appearance settings and tint preferences. These are configured in `app.json` under `ios.icon`.
- **`adaptive-icon.png`** -- The Android adaptive icon foreground image, displayed on a `#0b1119` background.
- **`icon.png`** and **`favicon.png`** -- Default icon and web favicon.
- **`splash-icon.png`** -- Static splash image used as a fallback before the Lottie animation loads.

## Onboarding

First-time users see the onboarding screen, which asks for two pieces of information: the Mitsuro server URL and a remote access token. The server URL is typically a Tailscale address (e.g., `https://device.tail123.ts.net:8443`), and the token is generated by the server. A successful health check proves server reachability before navigation, while `bootstrapAuth()` is a no-op; authentication is established only when a protected API request succeeds.

Alternatively, users can skip manual entry entirely by using a deep link. The client accepts a privately supplied `mitsuro://connect?url=...&token=...` link through `useDeepLink`; the server's `remote_launch_url` contains only the bare Tailscale URL.

## Session Types and Tabs

The chat screen supports three session types organized by tab: **chat** (general conversation), **code** (coding sessions with workspace context), and **hive** (autonomous agent sessions). The active tab determines what type of session is created when the user starts a new conversation.

On mobile, sessions are accessed through the `SessionDrawer`, a slide-in panel from the left edge. On desktop-width screens (1024px and above), the `DesktopShell` provides a persistent sidebar with the session list, plus additional panels for a terminal emulator and workspace preview. This adaptive layout means the app feels native on a phone and productive on a wide screen, all from the same component tree.

import { useCallback, useEffect, useRef, useState } from "react";
import {
  ActivityIndicator,
  Platform,
  Pressable,
  StyleSheet,
  Text,
  View,
} from "react-native";
import { AlertCircle, Plus, RefreshCw, X } from "lucide-react-native";

import * as Haptics from "../../platform/haptics";
import * as Clipboard from "../../platform/clipboard";
import { useConnection } from "../../hooks/useConnection";
import { useThemeContext } from "../../hooks/useTheme";
import { authorizedTerminalWebSocketUrl } from "../terminalUrl";
import { TerminalQuickBar } from "../toolbox/TerminalQuickBar";

const MAX_RECONNECT_ATTEMPTS = 8;
const RECONNECT_INITIAL_DELAY_MS = 250;
const RECONNECT_MAX_DELAY_MS = 5_000;
const RECONNECT_STABLE_RESET_MS = 30_000;
const HEARTBEAT_INTERVAL_MS = 15_000;
const HEARTBEAT_TIMEOUT_MS = 45_000;
const OUTPUT_HIGH_WATERMARK_BYTES = 512 * 1024;
const DEFAULT_TERMINAL_FONT_SIZE = 14;

function terminalFontSizeForWidth(width: number): number {
  if (width <= 520) return 11;
  if (width <= 760) return 12;
  return DEFAULT_TERMINAL_FONT_SIZE;
}

interface TerminalTab {
  id: string;
  label: string;
  connected: boolean;
  error: string | null;
}

interface TerminalProps {
  visible: boolean;
  style?: import("react-native").ViewStyle;
}

function createTerminalId(): string {
  if (typeof crypto !== "undefined" && "randomUUID" in crypto) {
    return crypto.randomUUID();
  }

  return `term-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
}

function createGhosttyTheme(
  colors: ReturnType<typeof useThemeContext>["theme"]["colors"],
) {
  return {
    background: colors.background,
    foreground: colors.foreground,
    cursor: colors.userMessage,
    cursorAccent: colors.background,
    selectionBackground: `${colors.userMessage}35`,
    black: "#18181b",
    red: "#ef4444",
    green: "#22c55e",
    yellow: "#eab308",
    blue: "#3b82f6",
    magenta: "#a855f7",
    cyan: "#06b6d4",
    white: "#fafafa",
    brightBlack: "#52525b",
    brightRed: "#f87171",
    brightGreen: "#4ade80",
    brightYellow: "#facc15",
    brightBlue: "#60a5fa",
    brightMagenta: "#c084fc",
    brightCyan: "#22d3ee",
    brightWhite: "#ffffff",
  };
}

function suppressGhosttyScrollbar(terminal: any) {
  // ghostty-web 0.4 draws its scrollbar directly into the canvas and does not
  // expose a visibility option. Keep scrollback/wheel behavior, but suppress
  // that renderer-only rail at this compatibility boundary.
  terminal.scrollbarVisible = false;
  terminal.scrollbarOpacity = 0;
  if (typeof terminal.showScrollbar === "function") {
    terminal.showScrollbar = () => {
      terminal.scrollbarVisible = false;
      terminal.scrollbarOpacity = 0;
    };
  }
}

export function Terminal({ visible, style }: TerminalProps) {
  const { theme } = useThemeContext();
  const { serverUrl, serverToken } = useConnection();

  const [tabs, setTabs] = useState<TerminalTab[]>([]);
  const [activeTabId, setActiveTabId] = useState<string | null>(null);

  const isWeb = Platform.OS === "web";
  const t = theme.colors;

  const createTab = useCallback(() => {
    const id = createTerminalId();
    setTabs((current) => [
      ...current,
      {
        id,
        label: `Terminal ${current.length + 1}`,
        connected: false,
        error: null,
      },
    ]);
    setActiveTabId(id);
  }, []);

  const closeTab = useCallback(
    (tabId: string) => {
      setTabs((current) => {
        const index = current.findIndex((tab) => tab.id === tabId);
        if (index === -1) {
          return current;
        }

        const nextTabs = current.filter((tab) => tab.id !== tabId);
        setActiveTabId((currentActive) =>
          currentActive === tabId
            ? nextTabs[Math.max(0, index - 1)]?.id ?? nextTabs[0]?.id ?? null
            : currentActive
        );
        return nextTabs;
      });
    },
    [],
  );

  const handleStatusChange = useCallback(
    (
      tabId: string,
      patch: Partial<Pick<TerminalTab, "connected" | "error">>,
    ) => {
      setTabs((current) =>
        current.map((tab) => (tab.id === tabId ? { ...tab, ...patch } : tab))
      );
    },
    [],
  );

  useEffect(() => {
    if (!isWeb || !visible || !serverUrl || tabs.length > 0) return;
    createTab();
  }, [createTab, isWeb, serverUrl, tabs.length, visible]);

  if (!isWeb) {
    return null;
  }

  // Keep tab metadata warm, but freeze live Ghostty/websocket processes while
  // the toolbox is closed to avoid background CPU/memory tax.
  if (!visible) {
    return (
      <View
        pointerEvents="none"
        style={[
          styles.container,
          { backgroundColor: t.background, borderTopColor: t.border },
          styles.hiddenSurface,
          style,
        ]}
      />
    );
  }

  return (
    <View
      pointerEvents="auto"
      style={[
        styles.container,
        { backgroundColor: t.background, borderTopColor: t.border },
        style,
      ]}
    >
      <View style={[styles.tabBar, { borderBottomColor: t.border }]}>
        {tabs.map((tab) => {
          const active = activeTabId === tab.id;
          const statusColor = tab.connected
            ? t.success
            : tab.error && !tab.error.startsWith("Reconnecting")
            ? t.error
            : t.warning;

          return (
            <View
              key={tab.id}
              style={[
                styles.tab,
                {
                  backgroundColor: active
                    ? `${t.userMessage}18`
                    : "transparent",
                  borderColor: active ? `${t.userMessage}25` : "transparent",
                },
              ]}
            >
              <Pressable
                onPress={() => {
                  void Haptics.impactAsync(Haptics.ImpactFeedbackStyle.Light);
                  setActiveTabId(tab.id);
                }}
                style={styles.tabPressable}
              >
                <View
                  style={[styles.statusDot, { backgroundColor: statusColor }]}
                />
                <Text
                  style={[
                    styles.tabLabel,
                    { color: active ? t.foreground : t.mutedForeground },
                  ]}
                  numberOfLines={1}
                >
                  {tab.label}
                </Text>
              </Pressable>

              <Pressable
                onPress={() => {
                  void Haptics.impactAsync(Haptics.ImpactFeedbackStyle.Light);
                  closeTab(tab.id);
                }}
                hitSlop={8}
              >
                <X size={12} color={t.mutedForeground} strokeWidth={2} />
              </Pressable>
            </View>
          );
        })}

        <Pressable
          onPress={() => {
            void Haptics.impactAsync(Haptics.ImpactFeedbackStyle.Light);
            createTab();
          }}
          style={styles.iconBtn}
        >
          <Plus size={16} color={t.mutedForeground} strokeWidth={2} />
        </Pressable>
      </View>

      <View style={styles.terminalArea}>
        {!serverUrl
          ? (
            <View style={styles.centerState}>
              <Text style={[styles.stateText, { color: t.mutedForeground }]}>
                Connect to a server to open a terminal.
              </Text>
            </View>
          )
          : tabs.length === 0
          ? (
            <View style={styles.centerState}>
              <Text style={[styles.stateText, { color: t.mutedForeground }]}>
                No terminal open.
              </Text>
            </View>
          )
          : (
            tabs.map((tab) => (
              <TerminalPane
                key={tab.id}
                active={activeTabId === tab.id}
                serverUrl={serverUrl}
                serverToken={serverToken}
                tab={tab}
                themeColors={theme.colors}
                onStatusChange={handleStatusChange}
              />
            ))
          )}
      </View>
    </View>
  );
}

function TerminalPane({
  active,
  serverUrl,
  serverToken,
  tab,
  themeColors,
  onStatusChange,
}: {
  active: boolean;
  serverUrl: string;
  serverToken: string | null;
  tab: TerminalTab;
  themeColors: ReturnType<typeof useThemeContext>["theme"]["colors"];
  onStatusChange: (
    tabId: string,
    patch: Partial<Pick<TerminalTab, "connected" | "error">>,
  ) => void;
}) {
  const divRef = useRef<HTMLDivElement | null>(null);
  const terminalRef = useRef<any>(null);
  const fitAddonRef = useRef<any>(null);
  const wsRef = useRef<WebSocket | null>(null);
  const serverUrlRef = useRef(serverUrl);
  const serverTokenRef = useRef(serverToken);
  const activeRef = useRef(active);
  const reconnectAttemptsRef = useRef(0);
  const reconnectTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const stableResetTimerRef = useRef<ReturnType<typeof setTimeout> | null>(
    null,
  );
  const heartbeatIntervalRef = useRef<ReturnType<typeof setInterval> | null>(
    null,
  );
  const heartbeatTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(
    null,
  );
  const resizeObserverRef = useRef<ResizeObserver | null>(null);
  const resizeTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const flushFrameRef = useRef<number | null>(null);
  const pendingOutputRef = useRef<Array<string | Uint8Array>>([]);
  const pendingOutputBytesRef = useRef(0);
  const lastColsRef = useRef(0);
  const lastRowsRef = useRef(0);
  const manualDisconnectRef = useRef(false);
  const ticketRequestRef = useRef<AbortController | null>(null);

  const sendQuickInput = useCallback((data: string) => {
    const ws = wsRef.current;
    if (ws?.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({ type: "input", data }));
    }
  }, []);

  const pasteClipboard = useCallback(async () => {
    const text = await Clipboard.getStringAsync();
    if (!text) return;
    if (typeof terminalRef.current?.paste === "function") {
      terminalRef.current.paste(text);
      return;
    }
    sendQuickInput(text);
  }, [sendQuickInput]);

  const refocusTerminal = useCallback(() => {
    terminalRef.current?.focus?.();
  }, []);

  const queueOutput = useCallback((data: string | Uint8Array) => {
    const byteLength = typeof data === "string"
      ? data.length * 2
      : data.byteLength;
    if (
      pendingOutputBytesRef.current + byteLength > OUTPUT_HIGH_WATERMARK_BYTES
    ) {
      pendingOutputRef.current = [];
      pendingOutputBytesRef.current = 0;
      onStatusChange(tab.id, {
        connected: false,
        error:
          "Terminal output exceeded the safe buffer. Reconnect to continue.",
      });
      wsRef.current?.close(1008, "terminal output buffer exceeded");
      return;
    }
    pendingOutputRef.current.push(data);
    pendingOutputBytesRef.current += byteLength;
    if (flushFrameRef.current !== null) {
      return;
    }

    flushFrameRef.current = window.requestAnimationFrame(() => {
      flushFrameRef.current = null;
      if (terminalRef.current && pendingOutputRef.current.length > 0) {
        const chunks = pendingOutputRef.current;
        pendingOutputRef.current = [];
        pendingOutputBytesRef.current = 0;
        for (const chunk of chunks) terminalRef.current.write(chunk);
      }
    });
  }, [onStatusChange, tab.id]);

  const clearReconnectTimer = useCallback(() => {
    if (reconnectTimerRef.current) {
      clearTimeout(reconnectTimerRef.current);
      reconnectTimerRef.current = null;
    }
  }, []);

  const clearStableResetTimer = useCallback(() => {
    if (stableResetTimerRef.current) {
      clearTimeout(stableResetTimerRef.current);
      stableResetTimerRef.current = null;
    }
  }, []);

  const clearHeartbeat = useCallback(() => {
    if (heartbeatIntervalRef.current) {
      clearInterval(heartbeatIntervalRef.current);
      heartbeatIntervalRef.current = null;
    }
    if (heartbeatTimeoutRef.current) {
      clearTimeout(heartbeatTimeoutRef.current);
      heartbeatTimeoutRef.current = null;
    }
  }, []);

  const touchHeartbeat = useCallback(
    (ws: WebSocket) => {
      if (heartbeatTimeoutRef.current) {
        clearTimeout(heartbeatTimeoutRef.current);
      }

      heartbeatTimeoutRef.current = setTimeout(() => {
        if (wsRef.current === ws && ws.readyState === WebSocket.OPEN) {
          ws.close(4000, "heartbeat_timeout");
        }
      }, HEARTBEAT_TIMEOUT_MS);
    },
    [],
  );

  const fitAndResize = useCallback((force = false) => {
    if (!terminalRef.current || !fitAddonRef.current || !divRef.current) return;

    const fontSize = terminalFontSizeForWidth(divRef.current.clientWidth);
    if (terminalRef.current.options.fontSize !== fontSize) {
      terminalRef.current.options.fontSize = fontSize;
      force = true;
    }

    fitAddonRef.current.fit();
    const cols = terminalRef.current.cols;
    const rows = terminalRef.current.rows;
    if (
      !force && cols === lastColsRef.current && rows === lastRowsRef.current
    ) {
      return;
    }

    lastColsRef.current = cols;
    lastRowsRef.current = rows;

    const ws = wsRef.current;
    if (ws?.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({ type: "resize", cols, rows }));
    }
  }, []);

  const connectToTerminal = useCallback(
    async (isReconnect: boolean) => {
      if (!terminalRef.current || !serverUrlRef.current) return;

      manualDisconnectRef.current = false;
      clearReconnectTimer();

      const existing = wsRef.current;
      if (
        existing?.readyState === WebSocket.OPEN ||
        existing?.readyState === WebSocket.CONNECTING
      ) {
        return;
      }

      if (existing && existing.readyState !== WebSocket.CLOSED) {
        existing.close();
      }

      clearHeartbeat();
      wsRef.current = null;

      if (!isReconnect) {
        reconnectAttemptsRef.current = 0;
        onStatusChange(tab.id, { connected: false, error: null });
      }

      ticketRequestRef.current?.abort();
      const ticketRequest = new AbortController(); ticketRequestRef.current = ticketRequest;
      let wsUrl: string;
      try { wsUrl = await authorizedTerminalWebSocketUrl(serverUrlRef.current, serverTokenRef.current, ticketRequest.signal); }
      catch { if (!ticketRequest.signal.aborted) onStatusChange(tab.id, { connected: false, error: "Stream authorization failed. Retry connection." }); return; }
      if (ticketRequest.signal.aborted || !terminalRef.current) return;
      const ws = new WebSocket(wsUrl);
      ws.binaryType = "arraybuffer";
      wsRef.current = ws;

      ws.onopen = () => {
        onStatusChange(tab.id, { connected: true, error: null });
        clearStableResetTimer();
        stableResetTimerRef.current = setTimeout(() => {
          if (wsRef.current === ws && ws.readyState === WebSocket.OPEN) {
            reconnectAttemptsRef.current = 0;
          }
          stableResetTimerRef.current = null;
        }, RECONNECT_STABLE_RESET_MS);

        clearHeartbeat();
        touchHeartbeat(ws);
        heartbeatIntervalRef.current = setInterval(() => {
          if (wsRef.current !== ws || ws.readyState !== WebSocket.OPEN) {
            return;
          }

          try {
            ws.send(JSON.stringify({ type: "ping" }));
          } catch {
            ws.close();
          }
        }, HEARTBEAT_INTERVAL_MS);

        try {
          ws.send(JSON.stringify({ type: "hello", binary_output: true }));
        } catch {
          ws.close();
          return;
        }

        window.requestAnimationFrame(() => {
          fitAndResize(true);
          terminalRef.current?.focus?.();
        });
      };

      ws.onmessage = (event) => {
        touchHeartbeat(ws);

        if (event.data instanceof ArrayBuffer) {
          queueOutput(new Uint8Array(event.data));
          return;
        }

        if (event.data instanceof Blob) {
          void event.data.arrayBuffer().then((buffer) => {
            queueOutput(new Uint8Array(buffer));
          });
          return;
        }

        const raw = typeof event.data === "string"
          ? event.data
          : String(event.data);
        try {
          const message = JSON.parse(raw);
          if (message.type === "output" && typeof message.data === "string") {
            queueOutput(message.data);
            return;
          }
          if (message.type === "error" && typeof message.error === "string") {
            onStatusChange(tab.id, { connected: false, error: message.error });
            return;
          }
        } catch {
          queueOutput(raw);
          return;
        }
      };

      ws.onerror = () => {
        onStatusChange(tab.id, { connected: false, error: "WebSocket error" });
      };

      ws.onclose = () => {
        clearHeartbeat();
        clearStableResetTimer();
        if (wsRef.current === ws) {
          wsRef.current = null;
        }

        onStatusChange(tab.id, { connected: false });

        if (manualDisconnectRef.current) {
          manualDisconnectRef.current = false;
          return;
        }

        if (reconnectTimerRef.current) {
          return;
        }

        const attempt = reconnectAttemptsRef.current;
        if (attempt >= MAX_RECONNECT_ATTEMPTS) {
          onStatusChange(tab.id, {
            connected: false,
            error: "Connection lost. Retry limit reached.",
          });
          return;
        }

        const attemptNumber = attempt + 1;
        reconnectAttemptsRef.current = attemptNumber;
        onStatusChange(tab.id, {
          connected: false,
          error: `Reconnecting (${attemptNumber}/${MAX_RECONNECT_ATTEMPTS})…`,
        });

        const delay = Math.min(
          RECONNECT_INITIAL_DELAY_MS * 2 ** attempt,
          RECONNECT_MAX_DELAY_MS,
        );
        reconnectTimerRef.current = setTimeout(() => {
          reconnectTimerRef.current = null;
          connectToTerminal(true);
        }, delay);
      };
    },
    [
      clearHeartbeat,
      clearReconnectTimer,
      clearStableResetTimer,
      fitAndResize,
      onStatusChange,
      queueOutput,
      tab.id,
      touchHeartbeat,
    ],
  );

  useEffect(() => {
    activeRef.current = active;
    if (active && terminalRef.current && fitAddonRef.current) {
      window.requestAnimationFrame(() => {
        if (pendingOutputRef.current.length > 0) {
          const chunks = pendingOutputRef.current;
          pendingOutputRef.current = [];
          pendingOutputBytesRef.current = 0;
          for (const chunk of chunks) terminalRef.current.write(chunk);
        }
        fitAndResize(true);
        terminalRef.current?.focus?.();
      });
    }
  }, [active, fitAndResize]);

  useEffect(() => {
    serverUrlRef.current = serverUrl;
  }, [serverUrl]);

  useEffect(() => {
    serverTokenRef.current = serverToken;
  }, [serverToken]);

  useEffect(() => {
    if (Platform.OS !== "web" || !divRef.current) {
      return;
    }

    let cancelled = false;

    void (async () => {
      try {
        const { FitAddon, Terminal, init } = await import("ghostty-web");
        await init();

        if (cancelled || !divRef.current) {
          return;
        }

        const terminal = new Terminal({
          cursorBlink: true,
          cursorStyle: "block",
          fontSize: terminalFontSizeForWidth(divRef.current.clientWidth),
          fontFamily: "JetBrains Mono, Fira Code, monospace",
          theme: createGhosttyTheme(themeColors),
        });

        const fitAddon = new FitAddon();
        terminal.loadAddon(fitAddon);
        terminal.open(divRef.current);
        suppressGhosttyScrollbar(terminal);

        terminalRef.current = terminal;
        fitAddonRef.current = fitAddon;

        terminal.onData((data: string) => {
          const ws = wsRef.current;
          if (ws?.readyState === WebSocket.OPEN) {
            ws.send(JSON.stringify({ type: "input", data }));
          }
        });

        resizeObserverRef.current = new ResizeObserver(() => {
          if (!activeRef.current) return;

          if (resizeTimeoutRef.current) {
            clearTimeout(resizeTimeoutRef.current);
          }
          resizeTimeoutRef.current = setTimeout(() => fitAndResize(), 75);
        });
        resizeObserverRef.current.observe(divRef.current);

        window.requestAnimationFrame(() => {
          fitAndResize(true);
          connectToTerminal(false);
        });
      } catch (err) {
        onStatusChange(tab.id, {
          connected: false,
          error: err instanceof Error
            ? err.message
            : "Failed to initialize terminal.",
        });
      }
    })();

    return () => {
      cancelled = true;
      manualDisconnectRef.current = true;
      ticketRequestRef.current?.abort();
      clearReconnectTimer();
      clearStableResetTimer();
      clearHeartbeat();
      resizeObserverRef.current?.disconnect();
      resizeObserverRef.current = null;

      if (resizeTimeoutRef.current) {
        clearTimeout(resizeTimeoutRef.current);
        resizeTimeoutRef.current = null;
      }
      if (flushFrameRef.current !== null) {
        cancelAnimationFrame(flushFrameRef.current);
        flushFrameRef.current = null;
      }

      wsRef.current?.close();
      wsRef.current = null;
      fitAddonRef.current = null;
      terminalRef.current?.dispose();
      terminalRef.current = null;
      pendingOutputRef.current = [];
      pendingOutputBytesRef.current = 0;
    };
  }, [
    clearHeartbeat,
    clearReconnectTimer,
    clearStableResetTimer,
    connectToTerminal,
    fitAndResize,
    onStatusChange,
    tab.id,
  ]);

  useEffect(() => {
    if (!terminalRef.current) return;
    terminalRef.current.options.theme = createGhosttyTheme(themeColors);
  }, [themeColors]);

  return (
    <View style={[styles.terminalPane, !active && styles.hiddenPane]}>
      <Pressable
        style={styles.terminalSurface}
        onPress={() => terminalRef.current?.focus?.()}
      >
        <div
          ref={divRef as any}
          style={{
            width: "100%",
            height: "100%",
            minWidth: 0,
            overflow: "hidden",
            position: "relative",
          }}
        />
      </Pressable>

      <TerminalQuickBar
        disabled={!tab.connected}
        onInput={sendQuickInput}
        onPaste={pasteClipboard}
        onRefocus={refocusTerminal}
      />

      {!tab.connected
        ? (
          <View style={styles.overlay}>
            {tab.error && !tab.error.startsWith("Reconnecting")
              ? (
                <>
                  <AlertCircle
                    size={16}
                    color={themeColors.error}
                    strokeWidth={1.8}
                  />
                  <Text
                    style={[styles.overlayText, { color: themeColors.error }]}
                  >
                    {tab.error}
                  </Text>
                  <Pressable
                    onPress={() => {
                      void Haptics.impactAsync(
                        Haptics.ImpactFeedbackStyle.Light,
                      );
                      reconnectAttemptsRef.current = 0;
                      clearReconnectTimer();
                      connectToTerminal(false);
                    }}
                    style={[styles.reconnectBtn, {
                      borderColor: themeColors.border,
                    }]}
                  >
                    <RefreshCw
                      size={14}
                      color={themeColors.foreground}
                      strokeWidth={1.8}
                    />
                    <Text
                      style={[styles.reconnectText, {
                        color: themeColors.foreground,
                      }]}
                    >
                      Reconnect
                    </Text>
                  </Pressable>
                </>
              )
              : (
                <>
                  <ActivityIndicator
                    color={themeColors.userMessage}
                    size="small"
                  />
                  <Text
                    style={[styles.overlayText, {
                      color: themeColors.mutedForeground,
                    }]}
                  >
                    {tab.error ?? "Connecting…"}
                  </Text>
                </>
              )}
          </View>
        )
        : null}
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    height: 320,
    borderTopWidth: StyleSheet.hairlineWidth,
  },
  hiddenSurface: {
    opacity: 0,
  },
  tabBar: {
    flexDirection: "row",
    alignItems: "center",
    gap: 6,
    paddingHorizontal: 12,
    paddingVertical: 8,
    borderBottomWidth: StyleSheet.hairlineWidth,
  },
  tab: {
    flexDirection: "row",
    alignItems: "center",
    gap: 6,
    paddingLeft: 10,
    paddingRight: 8,
    paddingVertical: 6,
    borderRadius: 10,
    borderWidth: StyleSheet.hairlineWidth,
    maxWidth: 180,
  },
  tabPressable: {
    flexDirection: "row",
    alignItems: "center",
    gap: 6,
    flexShrink: 1,
  },
  tabLabel: {
    fontSize: 12,
    fontWeight: "600",
  },
  statusDot: {
    width: 8,
    height: 8,
    borderRadius: 999,
  },
  iconBtn: {
    padding: 6,
    borderRadius: 8,
  },
  terminalArea: {
    flex: 1,
    position: "relative",
  },
  terminalPane: {
    ...StyleSheet.absoluteFillObject,
  },
  hiddenPane: {
    display: "none",
  },
  terminalSurface: {
    flex: 1,
    backgroundColor: "#0a0a0a",
  },
  overlay: {
    position: "absolute",
    top: 0,
    right: 0,
    bottom: 0,
    left: 0,
    alignItems: "center",
    justifyContent: "center",
    gap: 12,
    backgroundColor: "rgba(10,10,10,0.72)",
    paddingHorizontal: 24,
  },
  overlayText: {
    fontSize: 13,
    textAlign: "center",
  },
  reconnectBtn: {
    minHeight: 36,
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 10,
    paddingHorizontal: 12,
    paddingVertical: 8,
    flexDirection: "row",
    alignItems: "center",
    gap: 6,
  },
  reconnectText: {
    fontSize: 13,
    fontWeight: "600",
  },
  centerState: {
    flex: 1,
    alignItems: "center",
    justifyContent: "center",
    paddingHorizontal: 24,
  },
  stateText: {
    fontSize: 13,
    textAlign: "center",
  },
});

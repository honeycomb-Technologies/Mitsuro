import { useCallback, useEffect, useRef, useState } from 'react';
import type { ComponentType, Ref } from 'react';
import { ActivityIndicator, Platform, Pressable, StyleSheet, Text, useWindowDimensions, View } from 'react-native';
import { AlertCircle, Plus, RefreshCw, TerminalSquare, X } from 'lucide-react-native';
import type { TerminalInputEvent, TerminalResizeEvent, TerminalViewProps, TerminalViewRef } from 'expo-libghostty';

import * as Haptics from '../../platform/haptics';
import * as Clipboard from '../../platform/clipboard';
import { useThemeContext } from '../../hooks/useTheme';
import { useConnection } from '../../hooks/useConnection';
import { Terminal } from '../desktop/Terminal';
import { authorizedTerminalWebSocketUrl } from '../terminalUrl';
import { TerminalQuickBar } from './TerminalQuickBar';

type NativeTerminalViewProps = TerminalViewProps & { ref?: Ref<TerminalViewRef> };

let GhosttyTerminalView: ComponentType<NativeTerminalViewProps> | null = null;
if (Platform.OS !== 'web') {
  try {
    GhosttyTerminalView = require('expo-libghostty').TerminalView;
  } catch {
    // Expo Go cannot load custom native modules. Mitsuro builds bundle libghostty.
  }
}

interface ToolboxTerminalProps { visible: boolean }
interface NativeTerminalTab {
  id: string;
  label: string;
  connected: boolean;
  error: string | null;
}

const MAX_TERMINAL_TABS = 4;
const TAB_MOUNT_SETTLE_MS = 120;
const MAX_RECONNECT_ATTEMPTS = 8;
const RECONNECT_INITIAL_DELAY_MS = 250;
const RECONNECT_MAX_DELAY_MS = 5_000;
const HEARTBEAT_INTERVAL_MS = 15_000;
const HEARTBEAT_TIMEOUT_MS = 45_000;
const OUTPUT_HIGH_WATERMARK_BYTES = 512 * 1024;

function terminalFontSizeForWidth(width: number): number {
  if (width <= 390) return 9;
  if (width <= 520) return 11;
  if (width <= 760) return 12;
  return 14;
}

// Keep tab identity warm, but freeze native surfaces and PTYs while hidden.
const terminalSession: { tabs: NativeTerminalTab[]; activeTab: string | null } = {
  tabs: [],
  activeTab: null,
};

export function ToolboxTerminal({ visible }: ToolboxTerminalProps) {
  if (Platform.OS === 'web') {
    return <Terminal visible={visible} style={{ flex: 1, height: undefined, borderTopWidth: 0 }} />;
  }
  return <NativeTerminal visible={visible} />;
}

function NativeTerminal({ visible }: { visible: boolean }) {
  const { theme } = useThemeContext();
  const { serverUrl, serverToken } = useConnection();
  const t = theme.colors;
  const [tabs, setTabs] = useState<NativeTerminalTab[]>(terminalSession.tabs);
  const [activeTab, setActiveTab] = useState<string | null>(terminalSession.activeTab);
  const [renderedTabId, setRenderedTabId] = useState<string | null>(null);

  useEffect(() => {
    terminalSession.tabs = tabs;
    terminalSession.activeTab = activeTab;
  }, [activeTab, tabs]);

  const createTab = useCallback(() => {
    if (!serverUrl) return;
    void Haptics.impactAsync(Haptics.ImpactFeedbackStyle.Light);
    setTabs((current) => {
      if (current.length >= MAX_TERMINAL_TABS) {
        setActiveTab(current[current.length - 1]?.id ?? null);
        return current;
      }
      const id = createTerminalId();
      setActiveTab(id);
      return [...current, { id, label: `Terminal ${current.length + 1}`, connected: false, error: null }];
    });
  }, [serverUrl]);

  const closeTab = useCallback((id: string) => {
    setTabs((current) => {
      const index = current.findIndex((tab) => tab.id === id);
      const next = current.filter((tab) => tab.id !== id);
      setActiveTab((selected) => selected === id
        ? next[Math.max(0, index - 1)]?.id ?? next[0]?.id ?? null
        : selected);
      return next;
    });
  }, []);

  const updateTab = useCallback((id: string, patch: Partial<Pick<NativeTerminalTab, 'connected' | 'error'>>) => {
    setTabs((current) => current.map((tab) => tab.id === id ? { ...tab, ...patch } : tab));
  }, []);

  useEffect(() => {
    if (visible && tabs.length === 0 && serverUrl) createTab();
  }, [createTab, serverUrl, tabs.length, visible]);

  useEffect(() => {
    if (!visible || !activeTab) {
      setRenderedTabId(null);
      return;
    }
    const timer = setTimeout(() => setRenderedTabId(activeTab), TAB_MOUNT_SETTLE_MS);
    return () => clearTimeout(timer);
  }, [activeTab, visible]);

  return (
    <View style={[styles.container, { backgroundColor: t.background }]}>
      <View style={[styles.tabBar, { borderBottomColor: t.border }]}>
        {tabs.map((tab) => {
          const selected = activeTab === tab.id;
          const statusColor = tab.connected ? t.success : tab.error ? t.error : t.warning;
          return (
            <View key={tab.id} style={[styles.tab, selected && { backgroundColor: `${t.userMessage}18` }]}>
              <Pressable style={styles.tabPressable} onPress={() => setActiveTab(tab.id)}>
                <View style={[styles.statusDot, { backgroundColor: statusColor }]} />
                <Text style={[styles.tabLabel, { color: selected ? t.foreground : t.mutedForeground }]} numberOfLines={1}>
                  {tab.label}
                </Text>
              </Pressable>
              <Pressable onPress={() => closeTab(tab.id)} hitSlop={8}>
                <X size={12} color={t.mutedForeground} strokeWidth={2} />
              </Pressable>
            </View>
          );
        })}
        <Pressable onPress={createTab} style={styles.addBtn} disabled={!serverUrl}>
          <Plus size={16} color={t.mutedForeground} strokeWidth={2} />
        </Pressable>
      </View>

      <View style={styles.terminalArea}>
        {GhosttyTerminalView && visible && renderedTabId && serverUrl
          ? tabs.filter((tab) => tab.id === renderedTabId).map((tab) => (
              <NativeGhosttyPane
                key={tab.id}
                serverToken={serverToken}
                serverUrl={serverUrl}
                tab={tab}
                themeColors={t}
                onStatusChange={updateTab}
              />
            ))
          : null}
        {!activeTab || !visible || !GhosttyTerminalView || !serverUrl ? (
          <View style={styles.empty}>
            <TerminalSquare size={32} color={t.mutedForeground} strokeWidth={1.5} />
            <Text style={[styles.emptyText, { color: t.mutedForeground }]}>
              {!GhosttyTerminalView
                ? 'Native Ghostty requires a Mitsuro development or release build.'
                : !serverUrl
                  ? 'Connect to Honey to open a terminal.'
                  : visible ? 'No terminal open' : ''}
            </Text>
          </View>
        ) : null}
      </View>
    </View>
  );
}

function NativeGhosttyPane({ serverToken, serverUrl, tab, themeColors, onStatusChange }: {
  serverToken: string | null;
  serverUrl: string;
  tab: NativeTerminalTab;
  themeColors: ReturnType<typeof useThemeContext>['theme']['colors'];
  onStatusChange: (id: string, patch: Partial<Pick<NativeTerminalTab, 'connected' | 'error'>>) => void;
}) {
  const terminalRef = useRef<TerminalViewRef>(null);
  const { width: windowWidth } = useWindowDimensions();
  const socketRef = useRef<WebSocket | null>(null);
  const reconnectTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const heartbeatIntervalRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const heartbeatTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const reconnectAttemptsRef = useRef(0);
  const lastSizeRef = useRef({ cols: 80, rows: 24 });
  const manualCloseRef = useRef(false);
  const ticketRequestRef = useRef<AbortController | null>(null);
  const outputQueueRef = useRef<Array<{ data: string; base64: boolean }>>([]);
  const outputQueueBytesRef = useRef(0);
  const outputWriteActiveRef = useRef(false);
  const mountedRef = useRef(true);

  const drainOutput = useCallback(async () => {
    if (outputWriteActiveRef.current) return;
    outputWriteActiveRef.current = true;
    try {
      while (mountedRef.current && outputQueueRef.current.length > 0) {
        const chunk = outputQueueRef.current.shift();
        if (!chunk) break;
        outputQueueBytesRef.current = Math.max(0, outputQueueBytesRef.current - chunk.data.length);
        if (chunk.base64) await terminalRef.current?.write(chunk.data);
        else await terminalRef.current?.writeText(chunk.data);
      }
    } catch {
      onStatusChange(tab.id, { connected: false, error: 'Ghostty could not render terminal output.' });
      socketRef.current?.close(1011, 'terminal renderer failed');
    } finally {
      outputWriteActiveRef.current = false;
    }
  }, [onStatusChange, tab.id]);

  const enqueueOutput = useCallback((data: string, base64: boolean) => {
    if (outputQueueBytesRef.current + data.length > OUTPUT_HIGH_WATERMARK_BYTES) {
      outputQueueRef.current = [];
      outputQueueBytesRef.current = 0;
      onStatusChange(tab.id, {
        connected: false,
        error: 'Terminal output exceeded the safe buffer. Reconnect to continue.',
      });
      socketRef.current?.close(1008, 'terminal output buffer exceeded');
      return;
    }
    outputQueueRef.current.push({ data, base64 });
    outputQueueBytesRef.current += data.length;
    void drainOutput();
  }, [drainOutput, onStatusChange, tab.id]);

  const clearTimers = useCallback(() => {
    if (reconnectTimerRef.current) clearTimeout(reconnectTimerRef.current);
    if (heartbeatIntervalRef.current) clearInterval(heartbeatIntervalRef.current);
    if (heartbeatTimeoutRef.current) clearTimeout(heartbeatTimeoutRef.current);
    reconnectTimerRef.current = null;
    heartbeatIntervalRef.current = null;
    heartbeatTimeoutRef.current = null;
  }, []);

  const touchHeartbeat = useCallback((socket: WebSocket) => {
    if (heartbeatTimeoutRef.current) clearTimeout(heartbeatTimeoutRef.current);
    heartbeatTimeoutRef.current = setTimeout(() => {
      if (socketRef.current === socket && socket.readyState === WebSocket.OPEN) socket.close(4000, 'heartbeat_timeout');
    }, HEARTBEAT_TIMEOUT_MS);
  }, []);

  const connect = useCallback(async () => {
    const current = socketRef.current;
    if (current?.readyState === WebSocket.OPEN || current?.readyState === WebSocket.CONNECTING) return;
    clearTimers();
    manualCloseRef.current = false;
    onStatusChange(tab.id, { connected: false, error: null });
    ticketRequestRef.current?.abort();
    const ticketRequest = new AbortController(); ticketRequestRef.current = ticketRequest;
    let url: string;
    try { url = await authorizedTerminalWebSocketUrl(serverUrl, serverToken, ticketRequest.signal); }
    catch { if (!ticketRequest.signal.aborted) onStatusChange(tab.id, { connected: false, error: 'Stream authorization failed. Retry connection.' }); return; }
    if (ticketRequest.signal.aborted || !mountedRef.current) return;
    const socket = new WebSocket(url);
    socketRef.current = socket;

    socket.onopen = () => {
      reconnectAttemptsRef.current = 0;
      onStatusChange(tab.id, { connected: true, error: null });
      socket.send(JSON.stringify({ type: 'hello', output_encoding: 'base64' }));
      socket.send(JSON.stringify({ type: 'resize', ...lastSizeRef.current }));
      touchHeartbeat(socket);
      heartbeatIntervalRef.current = setInterval(() => {
        if (socketRef.current === socket && socket.readyState === WebSocket.OPEN) {
          socket.send(JSON.stringify({ type: 'ping' }));
        }
      }, HEARTBEAT_INTERVAL_MS);
    };

    socket.onmessage = (event) => {
      touchHeartbeat(socket);
      if (typeof event.data !== 'string') return;
      try {
        const message = JSON.parse(event.data);
        if (message.type === 'output_base64' && typeof message.data === 'string') {
          enqueueOutput(message.data, true);
        } else if (message.type === 'output' && typeof message.data === 'string') {
          enqueueOutput(message.data, false);
        } else if (message.type === 'error' && typeof message.error === 'string') {
          onStatusChange(tab.id, { connected: false, error: message.error });
        }
      } catch {
        enqueueOutput(String(event.data), false);
      }
    };
    socket.onerror = () => onStatusChange(tab.id, { connected: false, error: 'Terminal connection failed.' });
    socket.onclose = () => {
      if (socketRef.current === socket) socketRef.current = null;
      clearTimers();
      onStatusChange(tab.id, { connected: false });
      if (manualCloseRef.current) return;
      const attempt = reconnectAttemptsRef.current;
      if (attempt >= MAX_RECONNECT_ATTEMPTS) {
        onStatusChange(tab.id, { connected: false, error: 'Reconnect limit reached.' });
        return;
      }
      reconnectAttemptsRef.current = attempt + 1;
      onStatusChange(tab.id, { connected: false, error: `Reconnecting (${attempt + 1}/${MAX_RECONNECT_ATTEMPTS})…` });
      reconnectTimerRef.current = setTimeout(connect, Math.min(RECONNECT_INITIAL_DELAY_MS * 2 ** attempt, RECONNECT_MAX_DELAY_MS));
    };
  }, [clearTimers, enqueueOutput, onStatusChange, serverToken, serverUrl, tab.id, touchHeartbeat]);

  useEffect(() => {
    mountedRef.current = true;
    connect();
    return () => {
      mountedRef.current = false;
      ticketRequestRef.current?.abort();
      manualCloseRef.current = true;
      clearTimers();
      outputQueueRef.current = [];
      outputQueueBytesRef.current = 0;
      socketRef.current?.close();
      socketRef.current = null;
      void terminalRef.current?.finish(0);
    };
  }, [clearTimers, connect]);

  const handleInput = useCallback((event: { nativeEvent: TerminalInputEvent }) => {
    const socket = socketRef.current;
    if (socket?.readyState === WebSocket.OPEN) {
      socket.send(JSON.stringify({ type: 'input_base64', data: event.nativeEvent.data }));
    }
  }, []);

  const handleResize = useCallback((event: { nativeEvent: TerminalResizeEvent }) => {
    const { cols, rows } = event.nativeEvent;
    lastSizeRef.current = { cols, rows };
    const socket = socketRef.current;
    if (socket?.readyState === WebSocket.OPEN) socket.send(JSON.stringify({ type: 'resize', cols, rows }));
  }, []);

  const sendQuickInput = useCallback((data: string) => {
    const socket = socketRef.current;
    if (socket?.readyState === WebSocket.OPEN) {
      socket.send(JSON.stringify({ type: 'input', data }));
    }
  }, []);

  const pasteClipboard = useCallback(async () => {
    const text = await Clipboard.getStringAsync();
    if (text) sendQuickInput(text);
  }, [sendQuickInput]);

  const NativeView = GhosttyTerminalView;
  if (!NativeView) return null;
  return (
    <View style={styles.ghosttyHost}>
      <NativeView
        ref={terminalRef}
        style={styles.ghosttyHost}
        fontSize={terminalFontSizeForWidth(windowWidth)}
        theme={{
          background: themeColors.background,
          foreground: themeColors.foreground,
          cursorColor: themeColors.userMessage,
          selectionBackground: `${themeColors.userMessage}55`,
        }}
        onInput={handleInput}
        onResize={handleResize}
      />
      <TerminalQuickBar
        disabled={!tab.connected}
        onInput={sendQuickInput}
        onPaste={pasteClipboard}
      />
      {!tab.connected ? (
        <View style={[styles.overlay, { backgroundColor: `${themeColors.background}E8` }]}>
          {tab.error && !tab.error.startsWith('Reconnecting') ? (
            <>
              <AlertCircle size={17} color={themeColors.error} strokeWidth={1.8} />
              <Text style={[styles.overlayText, { color: themeColors.error }]}>{tab.error}</Text>
              <Pressable onPress={() => { reconnectAttemptsRef.current = 0; connect(); }} style={[styles.retryButton, { borderColor: themeColors.border }]}>
                <RefreshCw size={15} color={themeColors.foreground} strokeWidth={1.8} />
                <Text style={[styles.retryText, { color: themeColors.foreground }]}>Reconnect</Text>
              </Pressable>
            </>
          ) : (
            <>
              <ActivityIndicator color={themeColors.userMessage} size="small" />
              <Text style={[styles.overlayText, { color: themeColors.mutedForeground }]}>{tab.error ?? 'Connecting…'}</Text>
            </>
          )}
        </View>
      ) : null}
    </View>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1 },
  tabBar: { flexDirection: 'row', alignItems: 'center', gap: 4, paddingHorizontal: 12, paddingVertical: 6, borderBottomWidth: StyleSheet.hairlineWidth },
  tab: { flexDirection: 'row', alignItems: 'center', gap: 6, paddingHorizontal: 8, paddingVertical: 5, borderRadius: 6 },
  tabPressable: { flexDirection: 'row', alignItems: 'center', gap: 6 },
  statusDot: { width: 6, height: 6, borderRadius: 3 },
  tabLabel: { fontSize: 12, fontWeight: '500', maxWidth: 100 },
  addBtn: { padding: 4 },
  terminalArea: { flex: 1 },
  ghosttyHost: { flex: 1 },
  empty: { flex: 1, justifyContent: 'center', alignItems: 'center', gap: 12, padding: 24 },
  emptyText: { fontSize: 14, textAlign: 'center' },
  overlay: { ...StyleSheet.absoluteFillObject, alignItems: 'center', justifyContent: 'center', gap: 10, padding: 24 },
  overlayText: { fontSize: 13, textAlign: 'center' },
  retryButton: { minHeight: 40, flexDirection: 'row', alignItems: 'center', gap: 8, paddingHorizontal: 14, borderRadius: 10, borderWidth: StyleSheet.hairlineWidth },
  retryText: { fontSize: 13, fontWeight: '600' },
});

function createTerminalId(): string {
  if (typeof crypto !== 'undefined' && 'randomUUID' in crypto) return crypto.randomUUID();
  return `term-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
}

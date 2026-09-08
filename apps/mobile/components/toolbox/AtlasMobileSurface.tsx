import { platformFetch } from '../../platform/fetch';
import { authorizeDeviceStream } from '@mitsuro/api';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import type { CSSProperties, PointerEvent as ReactPointerEvent, ReactNode } from 'react';
import {
  ActivityIndicator,
  Platform,
  Pressable,
  ScrollView,
  StyleSheet,
  Text,
  TextInput,
  View,
} from 'react-native';
import type {
  BrowserAction,
  BrowserControlLease,
  BrowserSession,
  PortEntry,
} from '@mitsuro/api';
import type { WebViewProps } from 'react-native-webview';
import {
  ArrowLeft,
  ArrowRight,
  ChevronDown,
  Globe2,
  Monitor,
  Play,
  Plus,
  RefreshCw,
  Smartphone,
  X,
} from 'lucide-react-native';

import { colors } from '@mitsuro/ui';
import { useConnection } from '../../hooks/useConnection';
import { useThemeContext } from '../../hooks/useTheme';
import { withBrowserStreamAuthQuery } from '../../platform/browser-stream-auth';

const FALLBACK_BROWSER_URL = 'https://example.com';

function createBrowserClientId(): string {
  return `mitsuro-mobile-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`;
}

function currentWebPort(): number | null {
  if (Platform.OS !== 'web' || typeof window === 'undefined') return null;
  return Number(window.location.port || (window.location.protocol === 'https:' ? 443 : 80));
}

let NativeWebView: React.ComponentType<WebViewProps> | null = null;
if (Platform.OS !== 'web') {
  try {
    NativeWebView = require('react-native-webview').default as React.ComponentType<WebViewProps>;
  } catch {
    NativeWebView = null;
  }
}

function defaultBrowserAddress(serverUrl: string | null): string {
  if (Platform.OS === 'web' && typeof window !== 'undefined' && window.location.origin) {
    return window.location.origin;
  }
  if (serverUrl) {
    try {
      const url = new URL(serverUrl);
      url.port = '5173';
      url.pathname = '/';
      url.search = '';
      url.hash = '';
      return url.toString().replace(/\/$/, '');
    } catch {
      // Fall through to a public page when the saved server address is malformed.
    }
  }
  return FALLBACK_BROWSER_URL;
}

function addressForPort(port: number, serverUrl: string | null): string {
  if (Platform.OS === 'web' && typeof window !== 'undefined') {
    if (currentWebPort() === port) return window.location.origin;
  }

  if (serverUrl) {
    try {
      const server = new URL(serverUrl);
      const serverPort = Number(server.port || (server.protocol === 'https:' ? 443 : 80));
      if (serverPort === port) return server.origin;
    } catch {
      // A discovered previewable port is still reachable from server-side Chromium.
    }
  }

  return `http://127.0.0.1:${port}`;
}

function normalizeBrowserAddress(raw: string, fallback: string, serverUrl: string | null): string {
  const input = raw.trim();
  if (!input) return fallback;
  if (/^https?:\/\//i.test(input)) return input;
  const localPort = input.match(/^:(\d+)(\/.*)?$/);
  if (localPort) return `${addressForPort(Number(localPort[1]), serverUrl)}${localPort[2] ?? ''}`;
  if (/^(?:localhost|127\.0\.0\.1)(?::\d+)?(?:\/|$)/i.test(input)) return `http://${input}`;
  return `https://${input}`;
}

function browserTabLabel(session: BrowserSession): string {
  const title = session.title?.trim();
  if (title && title.toLowerCase() !== 'browser') return title;
  if (!session.url || session.url === 'about:blank') return 'Browser';
  try {
    const url = new URL(session.url);
    if (url.hostname === '127.0.0.1' || url.hostname === 'localhost') {
      return url.port ? `Local :${url.port}` : 'Local';
    }
    return url.port ? `${url.hostname}:${url.port}` : url.hostname;
  } catch {
    return title || 'Browser';
  }
}

function browserErrorMessage(cause: unknown, fallback: string): string {
  const message = cause instanceof Error ? cause.message : fallback;
  if (
    message.includes("Unexpected token '<'") ||
    message.includes('404') ||
    message.toLowerCase().includes('not found')
  ) {
    return 'Browser service is waiting for the updated Mitsuro runtime.';
  }
  return message;
}

function streamDocument(url: string, interactive: boolean): string {
  const encodedUrl = JSON.stringify(url).replace(/</g, '\\u003c');
  const encodedInteractive = interactive ? 'true' : 'false';
  return `<!doctype html>
<html><head><meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no">
<style>
html,body,#surface{margin:0;width:100%;height:100%;overflow:hidden;background:#0e0e11;touch-action:none}
#frame{width:100%;height:100%;object-fit:contain;object-position:center top;display:block;user-select:none;-webkit-user-select:none}
#status{position:fixed;inset:0;display:grid;place-items:center;color:#9aa7b5;font:13px system-ui;pointer-events:none}
#keyboard{position:fixed;left:-1000px;top:-1000px;width:1px;height:1px;opacity:0}
</style></head><body><div id="surface"><img id="frame"><div id="status">Connecting to Browser…</div><textarea id="keyboard"></textarea></div>
<script>
const interactive=${encodedInteractive};const socket=new WebSocket(${encodedUrl});const frame=document.getElementById('frame');const status=document.getElementById('status');const surface=document.getElementById('surface');const keyboard=document.getElementById('keyboard');let latest=null,meta=null;
const send=(value)=>socket.readyState===1&&socket.send(JSON.stringify(value));
const mutate=(value)=>interactive&&send(value);
socket.onopen=()=>{status.style.display='none';send({type:'config',pacing:'ack',maxFps:24})};
socket.onerror=()=>{status.textContent='Browser stream unavailable';status.style.display='grid'};
socket.onmessage=(event)=>{try{const value=JSON.parse(event.data);if(value.type==='frame'){latest=value;meta=value.metadata||null;frame.src='data:image/jpeg;base64,'+value.data}}catch{}};
frame.onload=()=>latest&&send({type:'ack',seq:latest.seq});
const point=(touch)=>{const box=surface.getBoundingClientRect();return{x:(touch.clientX-box.left)/box.width*(meta?.deviceWidth||box.width),y:(touch.clientY-box.top)/box.height*(meta?.deviceHeight||box.height)}};
surface.addEventListener('touchstart',(event)=>{if(!interactive)return;event.preventDefault();keyboard.focus();mutate({type:'input_touch',eventType:'touchStart',touchPoints:[...event.touches].map(point)})},{passive:false});
surface.addEventListener('touchmove',(event)=>{if(!interactive)return;event.preventDefault();mutate({type:'input_touch',eventType:'touchMove',touchPoints:[...event.touches].map(point)})},{passive:false});
surface.addEventListener('touchend',(event)=>{if(!interactive)return;event.preventDefault();mutate({type:'input_touch',eventType:'touchEnd',touchPoints:[...event.touches].map(point)})},{passive:false});
keyboard.addEventListener('beforeinput',(event)=>{if(interactive&&event.data){for(const key of event.data){mutate({type:'input_keyboard',eventType:'keyDown',key,text:key});mutate({type:'input_keyboard',eventType:'keyUp',key})}keyboard.value=''}});
keyboard.addEventListener('keydown',(event)=>{if(interactive&&event.key.length>1){event.preventDefault();mutate({type:'input_keyboard',eventType:'keyDown',key:event.key,code:event.code});mutate({type:'input_keyboard',eventType:'keyUp',key:event.key,code:event.code})}});
const mousePoint=(event)=>{const box=surface.getBoundingClientRect();return{x:(event.clientX-box.left)/box.width*(meta?.deviceWidth||box.width),y:(event.clientY-box.top)/box.height*(meta?.deviceHeight||box.height)}};
surface.addEventListener('pointerdown',(event)=>{if(!interactive||event.pointerType==='touch')return;event.preventDefault();keyboard.focus();const point=mousePoint(event);mutate({type:'input_mouse',eventType:'mousePressed',...point,button:event.button===2?'right':event.button===1?'middle':'left',clickCount:1})});
surface.addEventListener('pointermove',(event)=>{if(!interactive||event.pointerType==='touch')return;const point=mousePoint(event);mutate({type:'input_mouse',eventType:'mouseMoved',...point,button:'none'})});
surface.addEventListener('pointerup',(event)=>{if(!interactive||event.pointerType==='touch')return;const point=mousePoint(event);mutate({type:'input_mouse',eventType:'mouseReleased',...point,button:event.button===2?'right':event.button===1?'middle':'left',clickCount:1})});
surface.addEventListener('wheel',(event)=>{if(!interactive)return;event.preventDefault();const point=mousePoint(event);mutate({type:'input_mouse',eventType:'mouseWheel',...point,deltaX:event.deltaX,deltaY:event.deltaY})},{passive:false});
</script></body></html>`;
}

function AtlasStream({ html, serverUrl }: { html: string; serverUrl: string | null }) {
  if (Platform.OS === 'web') {
    return (
      <div style={{ width: '100%', height: '100%' }}>
        <iframe
          srcDoc={html}
          sandbox="allow-scripts"
          title="Mitsuro browser"
          style={{ width: '100%', height: '100%', border: 'none', display: 'block' }}
        />
      </div>
    );
  }

  if (!NativeWebView) return null;
  return (
    <NativeWebView
      source={{ html, baseUrl: serverUrl ?? undefined }}
      style={styles.webview}
      javaScriptEnabled
      domStorageEnabled={false}
      originWhitelist={['*']}
      allowsInlineMediaPlayback
    />
  );
}

function DraggableTabStrip({ children }: { children: ReactNode }) {
  const stripRef = useRef<HTMLDivElement | null>(null);
  const dragRef = useRef({
    active: false,
    captured: false,
    moved: false,
    pointerId: -1,
    startX: 0,
    scrollLeft: 0,
  });

  if (Platform.OS === 'web') {
    const beginDrag = (event: ReactPointerEvent<HTMLDivElement>) => {
      if (event.pointerType === 'touch' || event.button !== 0 || !stripRef.current) return;
      dragRef.current = {
        active: true,
        captured: false,
        moved: false,
        pointerId: event.pointerId,
        startX: event.clientX,
        scrollLeft: stripRef.current.scrollLeft,
      };
    };
    const moveDrag = (event: ReactPointerEvent<HTMLDivElement>) => {
      if (!dragRef.current.active || !stripRef.current) return;
      const delta = event.clientX - dragRef.current.startX;
      if (Math.abs(delta) <= 4 && !dragRef.current.moved) return;
      if (!dragRef.current.captured) {
        event.currentTarget.setPointerCapture(event.pointerId);
        dragRef.current.captured = true;
      }
      dragRef.current.moved = true;
      event.preventDefault();
      stripRef.current.scrollLeft = dragRef.current.scrollLeft - delta;
    };
    const endDrag = (event: ReactPointerEvent<HTMLDivElement>) => {
      dragRef.current.active = false;
      if (dragRef.current.captured && event.currentTarget.hasPointerCapture(event.pointerId)) {
        event.currentTarget.releasePointerCapture(event.pointerId);
      }
      dragRef.current.captured = false;
    };
    const stripStyle: CSSProperties = {
      flex: 1,
      minWidth: 0,
      display: 'flex',
      alignItems: 'center',
      gap: 5,
      overflowX: 'auto',
      overflowY: 'hidden',
      scrollbarWidth: 'none',
      touchAction: 'pan-x',
      cursor: 'grab',
      paddingRight: 5,
    };
    return (
      <div
        ref={stripRef}
        style={stripStyle}
        onPointerDown={beginDrag}
        onPointerMove={moveDrag}
        onPointerUp={endDrag}
        onPointerCancel={endDrag}
        onClickCapture={(event) => {
          if (!dragRef.current.moved) return;
          event.preventDefault();
          event.stopPropagation();
          dragRef.current.moved = false;
        }}
      >
        {children}
      </div>
    );
  }

  return (
    <ScrollView
      horizontal
      style={styles.tabScroller}
      showsHorizontalScrollIndicator={false}
      contentContainerStyle={styles.tabs}
    >
      {children}
    </ScrollView>
  );
}

export function AtlasMobileSurface({ visible }: { visible: boolean }) {
  const { theme } = useThemeContext();
  const { client, serverUrl, serverToken } = useConnection();
  const t = theme.colors;
  const browserClientId = useRef(createBrowserClientId()).current;
  const controlLeases = useRef(new Map<string, BrowserControlLease>());
  const [controlRevision, setControlRevision] = useState(0);
  const [sessions, setSessions] = useState<BrowserSession[]>([]);
  const [ports, setPorts] = useState<PortEntry[]>([]);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [address, setAddress] = useState(() => defaultBrowserAddress(serverUrl));
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [loaded, setLoaded] = useState(false);
  const [available, setAvailable] = useState(false);
  const [portsOpen, setPortsOpen] = useState(false);
  const autoOpenedRef = useRef(false);

  const setControlLease = useCallback((sessionId: string, lease: BrowserControlLease | null) => {
    const current = controlLeases.current.get(sessionId);
    if (lease) {
      controlLeases.current.set(sessionId, lease);
    } else {
      controlLeases.current.delete(sessionId);
    }
    if (current !== lease) setControlRevision((revision) => revision + 1);
  }, []);

  const selected = sessions.find((session) => session.id === selectedId) ?? sessions[0] ?? null;
  const selectedLease = selected ? controlLeases.current.get(selected.id) ?? null : null;
  const hasSelectedControl = Boolean(
    selectedLease &&
      selectedLease.client_id === browserClientId &&
      selectedLease.resource_id === selected?.id &&
      (!selected?.controller || selected.controller.client_id === browserClientId),
  );

  const refresh = useCallback(async () => {
    if (!client) return;
    const portsRequest = client.getPorts().catch(() => null);
    try {
      const response = await client.listBrowserSessions();
      const liveSessions = new Map(response.sessions.map((session) => [session.id, session]));
      let leasesChanged = false;
      for (const [sessionId, lease] of controlLeases.current) {
        const session = liveSessions.get(sessionId);
        if (
          !session ||
          lease.client_id !== browserClientId ||
          (session.controller && session.controller.client_id !== browserClientId)
        ) {
          controlLeases.current.delete(sessionId);
          leasesChanged = true;
        }
      }
      if (leasesChanged) setControlRevision((revision) => revision + 1);
      setSessions(response.sessions);
      setSelectedId((current) => current ?? response.sessions[0]?.id ?? null);
      setAvailable(response.capability.available);
      setError(response.capability.available ? null : response.capability.reason ?? 'Browser is unavailable.');
    } catch (cause) {
      setError(browserErrorMessage(cause, 'Could not load Browser sessions.'));
      setAvailable(false);
    } finally {
      const portResponse = await portsRequest;
      setPorts(
        portResponse?.ports.filter(
          (port) => port.active && (port.is_previewable_http || currentWebPort() === port.port),
        ) ?? [],
      );
      setLoaded(true);
    }
  }, [browserClientId, client]);

  useEffect(() => {
    if (!visible) return;
    void refresh();
  }, [refresh, visible]);

  const takeControl = useCallback(async (session: BrowserSession) => {
    if (!client) return null;
    const response = await client.takeBrowserControl(session.id, browserClientId);
    // Lease tokens live only in this mounted surface's in-memory map. They are
    // never written to storage, diagnostics, URLs other than the scoped stream,
    // or error messages.
    setControlLease(session.id, response.lease);
    const updated = await client.getBrowserSession(session.id);
    setSessions((current) =>
      current.map((item) => (item.id === updated.id ? updated : item)),
    );
    return response.lease;
  }, [browserClientId, client, setControlLease]);

  const takeSelectedControl = useCallback(async () => {
    if (!selected || !client) return;
    setBusy(true);
    setError(null);
    try {
      await takeControl(selected);
    } catch (cause) {
      setControlLease(selected.id, null);
      setError(browserErrorMessage(cause, 'Could not take Browser control.'));
    } finally {
      setBusy(false);
    }
  }, [client, selected, setControlLease, takeControl]);

  const navigateTo = useCallback(async (nextAddress: string, newTab = false) => {
    if (!client) return;
    setBusy(true);
    setError(null);
    try {
      const url = normalizeBrowserAddress(nextAddress, defaultBrowserAddress(serverUrl), serverUrl);
      setAddress(url);
      let session: BrowserSession;
      if (!newTab && selected && selected.status !== 'stopped' && selected.status !== 'error') {
        if (!selectedLease || !hasSelectedControl) {
          throw new Error('Observer mode · choose Take control before navigating.');
        }
        await client.heartbeatBrowserSession(
          selected.id,
          'controller',
          browserClientId,
          selectedLease,
        );
        const response = await client.runBrowserActions(
          selected.id,
          selectedLease,
          [{ type: 'navigate', url }],
        );
        if (!response.ok) throw new Error('Browser could not open that address.');
        session = await client.getBrowserSession(selected.id);
      } else {
        session = await client.createBrowserSession({
          title: 'Browser',
          kind: 'interactive',
          url,
          launch_local: true,
        });
        try {
          const response = await client.takeBrowserControl(session.id, browserClientId);
          setControlLease(session.id, response.lease);
          session = await client.getBrowserSession(session.id);
        } catch (cause) {
          setError(browserErrorMessage(cause, 'Browser opened in observer mode.'));
        }
      }
      setSessions((current) => [session, ...current.filter((item) => item.id !== session.id)]);
      setSelectedId(session.id);
    } catch (cause) {
      setError(browserErrorMessage(cause, 'Could not open that address.'));
    } finally {
      setBusy(false);
    }
  }, [
    browserClientId,
    client,
    hasSelectedControl,
    selected,
    selectedLease,
    serverUrl,
    setControlLease,
  ]);

  const selectTab = useCallback((session: BrowserSession) => {
    setSelectedId(session.id);
    if (session.url) setAddress(session.url);
    setPortsOpen(false);
  }, []);

  const closeTab = useCallback(async (session: BrowserSession) => {
    if (!client) return;
    setError(null);
    try {
      if (session.status !== 'stopped') {
        const lease = controlLeases.current.get(session.id);
        if (!lease || lease.client_id !== browserClientId) {
          throw new Error('Observer mode · take control before ending this Browser session.');
        }
        await client.stopBrowserSession(session.id, lease);
      }
      setControlLease(session.id, null);
      const remaining = sessions.filter((item) => item.id !== session.id);
      const closingSelectedTab = selectedId === session.id;
      setSessions(remaining);
      if (closingSelectedTab) {
        setSelectedId(remaining[0]?.id ?? null);
        setAddress(remaining[0]?.url ?? defaultBrowserAddress(serverUrl));
      }
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : 'Could not close Browser tab.');
      await refresh();
    }
  }, [browserClientId, client, refresh, selectedId, serverUrl, sessions, setControlLease]);

  const openPort = useCallback((port: PortEntry) => {
    setPortsOpen(false);
    void navigateTo(addressForPort(port.port, serverUrl), true);
  }, [navigateTo, serverUrl]);

  const openAddress = useCallback(() => navigateTo(address), [address, navigateTo]);

  useEffect(() => {
    if (!visible || !loaded || !available || sessions.length > 0 || autoOpenedRef.current) return;
    autoOpenedRef.current = true;
    void openAddress();
  }, [available, loaded, openAddress, sessions.length, visible]);

  useEffect(() => {
    if (!visible || !client || !selected?.id) return;
    let disposed = false;
    const sessionId = selected.id;
    const heartbeat = async () => {
      const lease = controlLeases.current.get(sessionId);
      try {
        const session = await client.heartbeatBrowserSession(
          sessionId,
          lease ? 'controller' : 'viewer',
          browserClientId,
          lease,
        );
        if (!disposed) {
          setSessions((current) =>
            current.map((item) => (item.id === session.id ? session : item)),
          );
        }
      } catch (cause) {
        if (!disposed && lease) {
          setControlLease(sessionId, null);
          setError(browserErrorMessage(cause, 'Browser control lease ended.'));
        }
      }
    };
    void heartbeat();
    // Server control leases expire after 15 seconds. Renew well inside that
    // window so ordinary timer jitter or one slow request cannot demote an
    // active controller before its first periodic heartbeat.
    const timer = setInterval(() => void heartbeat(), 5_000);
    return () => {
      disposed = true;
      clearInterval(timer);
    };
  }, [browserClientId, client, controlRevision, selected?.id, setControlLease, visible]);

  const runNavigationAction = useCallback(async (action: BrowserAction) => {
    if (!client || !selected) return;
    setBusy(true);
    setError(null);
    try {
      if (!selectedLease || !hasSelectedControl) {
        throw new Error('Observer mode · choose Take control before changing this Browser.');
      }
      await client.heartbeatBrowserSession(
        selected.id,
        'controller',
        browserClientId,
        selectedLease,
      );
      const response = await client.runBrowserActions(selected.id, selectedLease, [action]);
      if (!response.ok) throw new Error('Browser navigation failed.');
      const session = await client.getBrowserSession(selected.id);
      setSessions((current) => current.map((item) => item.id === session.id ? session : item));
      if (session.url) setAddress(session.url);
    } catch (cause) {
      setError(browserErrorMessage(cause, 'Browser navigation failed.'));
    } finally {
      setBusy(false);
    }
  }, [browserClientId, client, hasSelectedControl, selected, selectedLease]);

  const toggleViewport = useCallback(() => {
    const mode = selected?.viewport_mode === 'desktop' ? 'mobile' : 'desktop';
    void runNavigationAction({ type: 'viewport', mode });
  }, [runNavigationAction, selected?.viewport_mode]);

  const [streamHtml, setStreamHtml] = useState<string | null>(null);
  useEffect(() => {
    const abort = new AbortController(); setStreamHtml(null);
    const load = async () => {
    if (!client || !selected?.stream_url) return;
    try {
      const url = client.browserStreamUrl(
        selected.stream_url,
        hasSelectedControl ? 'controller' : 'viewer',
        browserClientId,
        hasSelectedControl ? selectedLease ?? undefined : undefined,
      );
      const authorized = serverToken?.startsWith('mitsuro_device_')
        ? await authorizeDeviceStream(url, serverToken, abort.signal, platformFetch as typeof fetch | undefined)
        : withBrowserStreamAuthQuery(url, serverToken);
      if (!abort.signal.aborted) setStreamHtml(streamDocument(authorized, hasSelectedControl));
    } catch {
      if (!abort.signal.aborted) setError('Stream authorization failed. Reopen the browser tab to retry.');
    }
    };
    void load(); return () => abort.abort();
  }, [
    browserClientId,
    client,
    controlRevision,
    hasSelectedControl,
    selected?.stream_url,
    selectedLease,
    serverToken,
  ]);

  return (
    <View style={[styles.root, { backgroundColor: t.background }]}>
      <View style={[styles.tabBar, { borderColor: t.border }]}>
        <DraggableTabStrip>
          {sessions.map((session) => {
            const active = session.id === selected?.id;
            return (
              <View
                key={session.id}
                style={[
                  styles.tab,
                  { borderColor: active ? t.userMessage : t.border, backgroundColor: active ? t.card : 'transparent' },
                ]}
              >
                <Pressable
                  accessibilityRole="tab"
                  accessibilityState={{ selected: active }}
                  accessibilityLabel={browserTabLabel(session)}
                  onPress={() => selectTab(session)}
                  style={styles.tabSelect}
                >
                  <Globe2 size={13} color={active ? t.foreground : t.mutedForeground} strokeWidth={1.8} />
                  <Text style={[styles.tabTitle, { color: active ? t.foreground : t.mutedForeground }]} numberOfLines={1}>
                    {browserTabLabel(session)}
                  </Text>
                </Pressable>
                <Pressable
                  accessibilityRole="button"
                  accessibilityLabel={`Close ${browserTabLabel(session)} tab`}
                  hitSlop={6}
                  onPressIn={(event) => event.stopPropagation()}
                  onPress={(event) => {
                    event.stopPropagation();
                    void closeTab(session);
                  }}
                  style={styles.tabClose}
                >
                  <X size={13} color={t.mutedForeground} strokeWidth={1.8} />
                </Pressable>
              </View>
            );
          })}
        </DraggableTabStrip>
        <Pressable
          accessibilityRole="button"
          accessibilityLabel="New browser tab"
          onPress={() => void navigateTo(defaultBrowserAddress(serverUrl), true)}
          disabled={busy || !client}
          style={styles.tabAction}
        >
          <Plus size={16} color={t.mutedForeground} strokeWidth={1.8} />
        </Pressable>
        <Pressable
          accessibilityRole="button"
          accessibilityLabel="Live ports"
          accessibilityState={{ expanded: portsOpen }}
          onPress={() => setPortsOpen((open) => !open)}
          disabled={ports.length === 0}
          style={styles.tabAction}
        >
          <Globe2 size={15} color={ports.length ? t.mutedForeground : t.border} strokeWidth={1.8} />
          <ChevronDown size={11} color={ports.length ? t.mutedForeground : t.border} strokeWidth={2} />
        </Pressable>
      </View>

      <View style={styles.toolbar}>
        <Pressable
          accessibilityRole="button"
          accessibilityLabel="Back"
          onPress={() => void runNavigationAction({ type: 'back' })}
          disabled={busy || !selected || !hasSelectedControl}
          style={styles.iconButton}
        >
          <ArrowLeft size={15} color={selected ? t.mutedForeground : t.border} strokeWidth={1.8} />
        </Pressable>
        <Pressable
          accessibilityRole="button"
          accessibilityLabel="Forward"
          onPress={() => void runNavigationAction({ type: 'forward' })}
          disabled={busy || !selected || !hasSelectedControl}
          style={styles.iconButton}
        >
          <ArrowRight size={15} color={selected ? t.mutedForeground : t.border} strokeWidth={1.8} />
        </Pressable>
        <Pressable
          accessibilityRole="button"
          accessibilityLabel="Reload"
          onPress={() => void runNavigationAction({ type: 'reload' })}
          disabled={busy || !selected || !hasSelectedControl}
          style={styles.iconButton}
        >
          <RefreshCw size={15} color={selected ? t.mutedForeground : t.border} strokeWidth={1.8} />
        </Pressable>
        <TextInput
          value={address}
          onChangeText={setAddress}
          onSubmitEditing={() => void openAddress()}
          returnKeyType="go"
          autoCapitalize="none"
          autoCorrect={false}
          placeholder="https://…"
          placeholderTextColor={t.mutedForeground}
          style={[styles.addressInput, { color: t.foreground, backgroundColor: t.card }]}
        />
        <Pressable
          accessibilityRole="button"
          accessibilityLabel={`Switch to ${selected?.viewport_mode === 'desktop' ? 'mobile' : 'desktop'} viewport`}
          onPress={toggleViewport}
          disabled={busy || !selected || !hasSelectedControl}
          style={styles.iconButton}
        >
          {selected?.viewport_mode === 'desktop' ? (
            <Smartphone size={15} color={t.mutedForeground} strokeWidth={1.8} />
          ) : (
            <Monitor size={16} color={t.mutedForeground} strokeWidth={1.8} />
          )}
        </Pressable>
        <Pressable
          accessibilityRole="button"
          accessibilityLabel="Open address"
          onPress={() => void openAddress()}
          disabled={busy || !client || Boolean(selected && !hasSelectedControl)}
          style={styles.iconButton}
        >
          {busy ? <ActivityIndicator size="small" color={t.userMessage} /> : <Play size={15} color={t.foreground} />}
        </Pressable>
      </View>

      {selected && !hasSelectedControl ? (
        <View style={[styles.controlNotice, { borderColor: t.border }]}>
          <Text style={[styles.controlNoticeText, { color: t.mutedForeground }]} numberOfLines={1}>
            {selected.controller && selected.controller.client_id !== browserClientId
              ? `Observer · ${selected.controller.client_id} controls this Browser`
              : 'Observer · take control to interact'}
          </Text>
          <Pressable
            accessibilityRole="button"
            accessibilityLabel="Take control of Browser"
            onPress={() => void takeSelectedControl()}
            disabled={busy}
            style={[styles.controlButton, { borderColor: t.userMessage }]}
          >
            {busy ? (
              <ActivityIndicator size="small" color={t.userMessage} />
            ) : (
              <Text style={[styles.controlButtonText, { color: t.foreground }]}>Take control</Text>
            )}
          </Pressable>
        </View>
      ) : null}

      {portsOpen ? (
        <View style={[styles.portDrawer, { borderColor: t.border }]}>
          <Text style={[styles.drawerLabel, { color: t.mutedForeground }]}>Live ports</Text>
          {ports.map((port) => (
            <Pressable
              key={port.port}
              accessibilityRole="menuitem"
              accessibilityLabel={`Open ${port.name || `port ${port.port}`} in new tab`}
              onPress={() => openPort(port)}
              disabled={busy}
              style={styles.portRow}
            >
              <View style={[styles.liveDot, { backgroundColor: t.userMessage }]} />
              <Text style={[styles.portName, { color: t.foreground }]} numberOfLines={1}>
                {port.name || `Port ${port.port}`}
              </Text>
              <Text style={{ color: t.mutedForeground }}>:{port.port}</Text>
            </Pressable>
          ))}
        </View>
      ) : null}

      <View style={styles.viewport}>
        {streamHtml && visible ? (
          <AtlasStream
            key={`${selected?.id}:${controlRevision}:${hasSelectedControl ? 'controller' : 'viewer'}`}
            html={streamHtml}
            serverUrl={serverUrl}
          />
        ) : (
          <View style={styles.empty}>
            <Text style={{ color: t.mutedForeground }}>
              {selected?.last_error ??
                error ??
                (available ? 'Starting Browser…' : 'Browser is unavailable.')}
            </Text>
          </View>
        )}
      </View>

      {error && selected ? <Text style={[styles.note, { color: t.destructive ?? '#ef4444' }]}>{error}</Text> : null}
    </View>
  );
}

const styles = StyleSheet.create({
  root: { flex: 1, position: 'relative' },
  tabBar: {
    height: 38,
    borderBottomWidth: StyleSheet.hairlineWidth,
    flexDirection: 'row',
    alignItems: 'center',
    paddingHorizontal: 8,
  },
  tabScroller: { flex: 1, minWidth: 0 },
  tabs: { alignItems: 'center', gap: 5, paddingRight: 5 },
  tab: {
    height: 29,
    minWidth: 92,
    maxWidth: 142,
    flexDirection: 'row',
    alignItems: 'stretch',
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 8,
    overflow: 'hidden',
  },
  tabSelect: {
    flex: 1,
    minWidth: 0,
    flexDirection: 'row',
    alignItems: 'center',
    gap: 6,
    paddingLeft: 8,
  },
  tabClose: { width: 30, alignItems: 'center', justifyContent: 'center' },
  tabTitle: { flex: 1, fontSize: 12, fontWeight: '500' },
  tabAction: {
    width: 34,
    height: 34,
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'center',
    gap: 1,
  },
  toolbar: {
    height: 48,
    flexDirection: 'row',
    alignItems: 'center',
    gap: 2,
    paddingHorizontal: 8,
    borderBottomWidth: StyleSheet.hairlineWidth,
    borderBottomColor: colors.glass.border,
  },
  controlNotice: {
    minHeight: 36,
    borderBottomWidth: StyleSheet.hairlineWidth,
    flexDirection: 'row',
    alignItems: 'center',
    gap: 10,
    paddingHorizontal: 10,
  },
  controlNoticeText: { flex: 1, minWidth: 0, fontSize: 11 },
  controlButton: {
    minWidth: 92,
    height: 26,
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 6,
    alignItems: 'center',
    justifyContent: 'center',
    paddingHorizontal: 10,
  },
  controlButtonText: { fontSize: 11, fontWeight: '600' },
  addressInput: {
    flex: 1,
    minWidth: 60,
    height: 34,
    borderRadius: 8,
    paddingHorizontal: 10,
    fontSize: 12,
  },
  iconButton: {
    width: 32,
    height: 34,
    alignItems: 'center',
    justifyContent: 'center',
    borderRadius: 8,
  },
  portDrawer: {
    position: 'absolute',
    zIndex: 20,
    top: 38,
    right: 8,
    width: 226,
    backgroundColor: colors.surfaceElevated,
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 10,
    paddingVertical: 6,
    shadowColor: '#000',
    shadowOpacity: 0.28,
    shadowRadius: 18,
    shadowOffset: { width: 0, height: 8 },
    elevation: 12,
  },
  drawerLabel: {
    paddingHorizontal: 10,
    paddingVertical: 5,
    fontSize: 10,
    fontWeight: '700',
    textTransform: 'uppercase',
    letterSpacing: 0.7,
  },
  portRow: {
    minHeight: 38,
    flexDirection: 'row',
    alignItems: 'center',
    gap: 8,
    paddingHorizontal: 10,
  },
  liveDot: { width: 6, height: 6, borderRadius: 3 },
  portName: { flex: 1, fontSize: 13 },
  viewport: { flex: 1, backgroundColor: colors.background },
  webview: { flex: 1, backgroundColor: colors.background },
  empty: { flex: 1, alignItems: 'center', justifyContent: 'center', padding: 24 },
  note: { fontSize: 12, lineHeight: 17, paddingHorizontal: 10, paddingBottom: 8 },
});

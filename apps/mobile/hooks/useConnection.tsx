import { AppState } from "react-native";
import {
  createContext,
  type ReactNode,
  useCallback,
  useContext,
  useEffect,
  useRef,
  useState,
} from "react";
import * as SecureStore from "../platform/secure-store";
import { platformFetch } from "../platform/fetch";
import { MitsuroClient } from "@mitsuro/api";
import { recordRequestDiagnostic } from "../diagnostics/mobileDiagnostics";
import {
  deleteConnectionCredentials,
  honorPendingConnectionLogout,
  readConnectionCredentials,
  writeConnectionCredentials,
} from "../platform/identity-storage";
import { connectionFromInjectedGlobals } from "../platform/identity-compatibility";
import { deriveRecoveryConnectionScope } from "../platform/recovery-connection-scope";
import {
  type ConnectionIntent,
  createConnectionIntentCoordinator,
} from "../platform/connection-intent-coordinator";

type ConnectionStatus =
  | "disconnected"
  | "disconnecting"
  | "connecting"
  | "connected"
  | "error";

export type DisconnectResult = "disconnected" | "superseded";

interface ConnectionContextValue {
  client: MitsuroClient | null;
  status: ConnectionStatus;
  isConnected: boolean;
  isConfigured: boolean;
  hasLoadedConnection: boolean;
  serverUrl: string | null;
  serverToken: string | null;
  recoveryConnectionScope: string | null;
  error: string | null;
  connect: (url: string, token: string) => Promise<boolean>;
  disconnect: () => Promise<DisconnectResult>;
  reconnect: () => Promise<void>;
}

const ConnectionContext = createContext<ConnectionContextValue>({
  client: null,
  status: "disconnected",
  isConnected: false,
  isConfigured: false,
  hasLoadedConnection: false,
  serverUrl: null,
  serverToken: null,
  recoveryConnectionScope: null,
  error: null,
  connect: async () => false,
  disconnect: async () => "disconnected",
  reconnect: async () => {},
});

export function ConnectionProvider({ children }: { children: ReactNode }) {
  const [client, setClient] = useState<MitsuroClient | null>(null);
  const [status, setStatus] = useState<ConnectionStatus>("disconnected");
  const [serverUrl, setServerUrl] = useState<string | null>(null);
  const [serverToken, setServerToken] = useState<string | null>(null);
  const [recoveryConnectionScope, setRecoveryConnectionScope] = useState<
    string | null
  >(null);
  const [error, setError] = useState<string | null>(null);
  const [isConfigured, setIsConfigured] = useState(false);
  const [loaded, setLoaded] = useState(false);
  const connectionIntentsRef = useRef<
    ReturnType<
      typeof createConnectionIntentCoordinator
    > | null
  >(null);
  if (!connectionIntentsRef.current) {
    connectionIntentsRef.current = createConnectionIntentCoordinator();
  }
  const connectionIntents = connectionIntentsRef.current;
  // Reserve initial-load ownership during render. A child effect or imperative
  // Connect/Disconnect call can then supersede it before this provider's
  // passive effect runs; initial credential migration never becomes newer by
  // merely starting later in the effect order.
  const initialConnectionIntentRef = useRef<ConnectionIntent | null>(null);
  if (!initialConnectionIntentRef.current) {
    initialConnectionIntentRef.current = connectionIntents.begin();
  }

  const doConnect = useCallback(async (
    url: string,
    token: string,
    intent: ConnectionIntent,
  ): Promise<boolean> => {
    if (!connectionIntents.isCurrent(intent)) return false;
    // A connection attempt is an account handoff. Remove the prior client and
    // recovery namespace before the first network await so its transcript and
    // transport cannot remain interactive while the replacement authenticates.
    setClient(null);
    setServerToken(null);
    setRecoveryConnectionScope(null);
    setServerUrl(url);
    setStatus("connecting");
    setError(null);

    try {
      const newClient = new MitsuroClient({
        baseUrl: url,
        token,
        ...(platformFetch
          ? { fetchImpl: platformFetch as unknown as typeof fetch }
          : {}),
        requestObserver: ({ name, outcome, durationMs, code }) => {
          recordRequestDiagnostic(name, outcome, durationMs, code);
        },
      });

      // Verify connection with health check. Every await is followed by an
      // intent fence so Disconnect or a newer connection always wins.
      const healthy = await newClient.checkHealth();
      if (!connectionIntents.isCurrent(intent)) return false;
      if (!healthy) {
        setClient(null);
        setServerToken(null);
        setRecoveryConnectionScope(null);
        setStatus("error");
        setError("Server not reachable");
        return false;
      }

      // Bootstrap remote auth
      const authed = await newClient.bootstrapAuth();
      if (!connectionIntents.isCurrent(intent)) return false;
      if (!authed) {
        setClient(null);
        setServerToken(null);
        setRecoveryConnectionScope(null);
        setStatus("error");
        setError("Authentication failed — check your token");
        return false;
      }

      const nextRecoveryScope = deriveRecoveryConnectionScope(url, token);
      if (!connectionIntents.isCurrent(intent)) return false;
      setRecoveryConnectionScope(nextRecoveryScope);
      setClient(newClient);
      setServerUrl(url);
      setServerToken(token);
      setStatus("connected");
      return true;
    } catch (err) {
      if (!connectionIntents.isCurrent(intent)) return false;
      setClient(null);
      setServerToken(null);
      setRecoveryConnectionScope(null);
      setStatus("error");
      setError(err instanceof Error ? err.message : "Connection failed");
      return false;
    }
  }, [connectionIntents]);

  // Load saved connection on mount — check for Tauri injected URL first
  useEffect(() => {
    const intent = initialConnectionIntentRef.current;
    if (!intent) return;
    void (async () => {
      try {
        const logout = await connectionIntents.runCurrentCredentialOperation(
          intent,
          () => honorPendingConnectionLogout(SecureStore),
        );
        if (
          logout.status === "stale" ||
          !connectionIntents.isCurrent(intent)
        ) return;
        if (logout.value) {
          setClient(null);
          setServerUrl(null);
          setServerToken(null);
          setRecoveryConnectionScope(null);
          setIsConfigured(false);
          setStatus("disconnected");
          setError(null);
          setLoaded(true);
          return;
        }
      } catch (logoutReadError) {
        if (!connectionIntents.isCurrent(intent)) return;
        setClient(null);
        setServerToken(null);
        setRecoveryConnectionScope(null);
        setStatus("error");
        setError(
          logoutReadError instanceof Error
            ? logoutReadError.message
            : "Stored logout state could not be read",
        );
        setLoaded(true);
        return;
      }

      // Tauri desktop: auto-connect via injected globals
      const tauriConnection = typeof window === "undefined"
        ? null
        : connectionFromInjectedGlobals(
          window as unknown as Record<string, unknown>,
        );
      if (tauriConnection) {
        if (!connectionIntents.isCurrent(intent)) return;
        setIsConfigured(true);
        setServerUrl(tauriConnection.serverUrl);
        await doConnect(
          tauriConnection.serverUrl,
          tauriConnection.token,
          intent,
        );
        if (connectionIntents.isCurrent(intent)) setLoaded(true);
        return;
      }

      try {
        const read = await connectionIntents.runCurrentCredentialOperation(
          intent,
          () => readConnectionCredentials(SecureStore),
        );
        if (
          read.status === "stale" ||
          !connectionIntents.isCurrent(intent)
        ) return;
        const saved = read.value;
        if (saved) {
          setIsConfigured(true);
          setServerUrl(saved.serverUrl);
          await doConnect(saved.serverUrl, saved.token, intent);
        }
      } catch (loadError) {
        if (!connectionIntents.isCurrent(intent)) return;
        setStatus("error");
        setError(
          loadError instanceof Error
            ? loadError.message
            : "Stored connection is invalid",
        );
      }
      if (connectionIntents.isCurrent(intent)) setLoaded(true);
    })();
    return () => {
      // Invalidate every operation owned by the departing provider. The
      // replacement token supports React Strict Mode's effect replay without
      // reviving any request from the prior activation.
      initialConnectionIntentRef.current = connectionIntents.begin();
    };
  }, [connectionIntents, doConnect]);

  const connect = useCallback(
    async (url: string, token: string): Promise<boolean> => {
      const intent = connectionIntents.begin();
      const success = await doConnect(url, token, intent);
      if (!success || !connectionIntents.isCurrent(intent)) {
        if (connectionIntents.isCurrent(intent)) setLoaded(true);
        return false;
      }
      try {
        const persisted = await connectionIntents.runCurrentCredentialOperation(
          intent,
          () =>
            writeConnectionCredentials(SecureStore, {
              serverUrl: url,
              token,
            }),
        );
        if (
          persisted.status === "stale" ||
          !connectionIntents.isCurrent(intent)
        ) {
          return false;
        }
        setIsConfigured(true);
        setLoaded(true);
        return true;
      } catch (persistError) {
        if (!connectionIntents.isCurrent(intent)) return false;
        setClient(null);
        setServerToken(null);
        setRecoveryConnectionScope(null);
        setStatus("error");
        setError(
          persistError instanceof Error
            ? persistError.message
            : "Connection credentials could not be saved",
        );
        return false;
      }
    },
    [connectionIntents, doConnect],
  );

  const disconnect = useCallback(async (): Promise<DisconnectResult> => {
    const intent = connectionIntents.begin();
    setClient(null);
    setStatus("disconnecting");
    setServerToken(null);
    setRecoveryConnectionScope(null);
    setError(null);
    setLoaded(true);
    try {
      // Deletes are never skipped as stale. A newer credential operation is
      // serialized behind this one and therefore still owns the final value.
      await connectionIntents.runCredentialOperation(() =>
        deleteConnectionCredentials(SecureStore)
      );
    } catch (logoutError) {
      if (!connectionIntents.isCurrent(intent)) return "superseded";
      const message = logoutError instanceof Error
        ? logoutError.message
        : "Saved server credentials could not be removed.";
      setStatus("error");
      setError(message);
      setIsConfigured(true);
      throw new Error(message);
    }
    if (!connectionIntents.isCurrent(intent)) return "superseded";
    setServerUrl(null);
    setStatus("disconnected");
    setIsConfigured(false);
    return "disconnected";
  }, [connectionIntents]);

  const reconnect = useCallback(async () => {
    const intent = connectionIntents.begin();
    setClient(null);
    setServerToken(null);
    setRecoveryConnectionScope(null);
    setStatus("connecting");
    setError(null);
    try {
      const read = await connectionIntents.runCurrentCredentialOperation(
        intent,
        () => readConnectionCredentials(SecureStore),
      );
      if (
        read.status === "stale" ||
        !connectionIntents.isCurrent(intent)
      ) return;
      const saved = read.value;
      if (saved) {
        setServerUrl(saved.serverUrl);
        await doConnect(saved.serverUrl, saved.token, intent);
      } else {
        setServerUrl(null);
        setStatus("disconnected");
        setIsConfigured(false);
      }
    } catch (reconnectError) {
      if (!connectionIntents.isCurrent(intent)) return;
      setServerUrl(null);
      setStatus("error");
      setError(
        reconnectError instanceof Error
          ? reconnectError.message
          : "Stored connection is invalid",
      );
    }
    if (connectionIntents.isCurrent(intent)) setLoaded(true);
  }, [connectionIntents, doConnect]);

  useEffect(() => {
    if (
      !client || status !== "connected" ||
      !serverToken?.startsWith("mitsuro_device_")
    ) return;
    const intent = connectionIntents.current();
    let stopped = false;
    let checking = false;
    let request: AbortController | null = null;
    let timer: ReturnType<typeof setTimeout>;
    const check = async () => {
      if (stopped || checking || AppState.currentState !== "active") return;
      checking = true;
      request = new AbortController();
      const abort = request;
      const timeout = setTimeout(() => abort.abort(), 10_000);
      try {
        const healthy = await client.checkHealth(abort.signal);
        if (
          !stopped && AppState.currentState === "active" &&
          connectionIntents.isCurrent(intent) && !healthy
        ) {
          setClient(null);
          setServerToken(null);
          setRecoveryConnectionScope(null);
          setStatus("error");
          setError(
            "Connection lost or device access revoked. Reconnect or pair again.",
          );
        }
      } finally {
        clearTimeout(timeout);
        checking = false;
        if (!stopped) timer = setTimeout(check, 30_000);
      }
    };
    timer = setTimeout(check, 30_000);
    const lifecycle = AppState.addEventListener("change", (state) => {
      clearTimeout(timer);
      if (state === "active") void check();
      else request?.abort();
    });
    return () => {
      stopped = true;
      request?.abort();
      clearTimeout(timer);
      lifecycle.remove();
    };
  }, [client, status, serverToken, connectionIntents]);

  return (
    <ConnectionContext.Provider
      value={{
        client,
        status,
        isConnected: status === "connected",
        isConfigured,
        hasLoadedConnection: loaded,
        serverUrl,
        serverToken,
        recoveryConnectionScope,
        error,
        connect,
        disconnect,
        reconnect,
      }}
    >
      {children}
    </ConnectionContext.Provider>
  );
}

export function useConnection() {
  return useContext(ConnectionContext);
}

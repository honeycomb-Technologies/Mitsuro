import {
  createContext,
  type ReactNode,
  useContext,
  useEffect,
  useState,
  useRef,
} from "react";
import {
  createGitStore,
  createPlanStore,
  createSessionsStore,
  createSessionStore,
  createWorkspaceStore,
} from "@mitsuro/state";
import type {
  GitStoreState,
  MitsuroStorage,
  PlanStoreState,
  SessionsStoreState,
  SessionStoreState,
  WorkspaceStoreState,
} from "@mitsuro/state";
import type { MitsuroClient } from "@mitsuro/api";
import type { SessionType } from "@mitsuro/api";
import { createStorage } from "../platform/mitsuro-storage";
import { IDENTITY_STORAGE_KEYS } from "../platform/identity-storage";
import { guardRecoveryTransport } from "../platform/recovery-transport-guard";
import { useStore } from "zustand";

// Store types
type SessionsStore = ReturnType<typeof createSessionsStore>;
type SessionStore = ReturnType<typeof createSessionStore>;
type WorkspaceStore = ReturnType<typeof createWorkspaceStore>;
type GitStore = ReturnType<typeof createGitStore>;
type PlanStore = ReturnType<typeof createPlanStore>;

interface StoresContextValue {
  sessions: SessionsStore;
  modes: Record<SessionType, ModeStores>;
  /** Compatibility aliases for shared/non-mode-aware surfaces. */
  session: SessionStore;
  workspace: WorkspaceStore;
  git: GitStore;
  plan: PlanStore;
}

interface ModeStores {
  session: SessionStore;
  workspace: WorkspaceStore;
  plan: PlanStore;
}

const StoresContext = createContext<StoresContextValue | null>(null);

interface StoresProviderProps {
  client: MitsuroClient | null;
  recoveryConnectionScope: string | null;
  children: ReactNode;
  onHandoffComplete?: () => void;
}

interface ScopedStoreGraph {
  recoveryConnectionScope: string;
  stores: StoresContextValue;
}

const STORAGE_HYDRATION_KEYS = [
  IDENTITY_STORAGE_KEYS.workspaceCode.canonical,
  IDENTITY_STORAGE_KEYS.workspaceChat.canonical,
  IDENTITY_STORAGE_KEYS.workspaceHive.canonical,
  IDENTITY_STORAGE_KEYS.permissionMode.canonical,
  IDENTITY_STORAGE_KEYS.presenceClientId.canonical,
] as const;

function recoveryGuardedClient(
  client: MitsuroClient,
  storage: MitsuroStorage,
): MitsuroClient {
  if (!storage.ensureDurableRecoveryAuthority) return client;
  return guardRecoveryTransport(
    client,
    storage.ensureDurableRecoveryAuthority.bind(storage),
  );
}

function buildStores(client: MitsuroClient, storage: MitsuroStorage) {
  // Code keeps the original shared workspace slot; the storage adapter upgrades
  // its prior key before the mode stores are constructed.
  const workspaces: Record<SessionType, WorkspaceStore> = {
    chat: createWorkspaceStore(
      storage,
      IDENTITY_STORAGE_KEYS.workspaceChat.canonical,
    ),
    code: createWorkspaceStore(storage),
    hive: createWorkspaceStore(
      storage,
      IDENTITY_STORAGE_KEYS.workspaceHive.canonical,
    ),
  };
  const sessionClient = recoveryGuardedClient(client, storage);
  const sessions = createSessionsStore(sessionClient, workspaces.code);
  const modes = (["chat", "code", "hive"] as const).reduce(
    (result, mode) => {
      const plan = createPlanStore();
      result[mode] = {
        workspace: workspaces[mode],
        plan,
        session: createSessionStore(
          sessionClient,
          storage,
          workspaces[mode],
          sessions,
          plan,
          mode,
        ),
      };
      return result;
    },
    {} as Record<SessionType, ModeStores>,
  );
  const getDirectory = () => workspaces.code.getState().directory;
  const git = createGitStore(client, getDirectory);

  return {
    sessions,
    modes,
    // These aliases keep older shared components working. Mode-aware chat
    // surfaces use the hooks below with an explicit mode.
    session: modes.chat.session,
    workspace: modes.code.workspace,
    plan: modes.chat.plan,
    git,
  };
}

export function StoresProvider({
  client,
  recoveryConnectionScope,
  children,
  onHandoffComplete,
}: StoresProviderProps) {
  const handoffCompleteRef = useRef(onHandoffComplete);
  handoffCompleteRef.current = onHandoffComplete;
  const [scopedStoreGraph, setScopedStoreGraph] = useState<
    ScopedStoreGraph | null
  >(null);
  // React may render new ConnectionContext state before this effect has built
  // its replacement graph. Never expose a prior principal's stores during
  // that async handoff. A same-scope reconnect may retain its existing shell.
  const stores = client && recoveryConnectionScope &&
      scopedStoreGraph?.recoveryConnectionScope === recoveryConnectionScope
    ? scopedStoreGraph.stores
    : null;
  const isConnectionHandoff = Boolean(
    client && recoveryConnectionScope && !stores,
  );

  useEffect(() => {
    if (stores) handoffCompleteRef.current?.();
  }, [stores]);

  useEffect(() => {
    let cancelled = false;

    if (!client || !recoveryConnectionScope) {
      setScopedStoreGraph(null);
      return () => {
        cancelled = true;
      };
    }

    const storage = createStorage(recoveryConnectionScope);
    let initialized = false;
    let rebuildPending = false;
    let rebuildScheduled = false;

    const rebuild = () => {
      if (cancelled) return;
      storage.beginDurableRecoverySnapshot?.();
      const nextStores = buildStores(client, storage);
      storage.acknowledgeDurableRecoverySnapshot?.();
      if (!cancelled) {
        setScopedStoreGraph({
          recoveryConnectionScope,
          stores: nextStores,
        });
      }
    };

    const scheduleRebuild = () => {
      if (cancelled) return;
      if (!initialized) {
        rebuildPending = true;
        return;
      }
      if (rebuildScheduled) return;
      rebuildScheduled = true;
      queueMicrotask(() => {
        rebuildScheduled = false;
        rebuild();
      });
    };

    const unsubscribe = storage.subscribeDurableRecoveryInvalidation?.(
      scheduleRebuild,
    );

    // Keep the previous shell mounted while the next store graph hydrates so
    // reconnect / client swaps do not blank the entire app. A peer web tab is
    // allowed to build a read-only graph; guarded transport and every durable
    // mutation still fail closed until this graph owns the origin-wide lock.
    void (async () => {
      try {
        await storage.activateDurableRecovery?.();
      } catch {
        // Unsupported or unavailable lock authority is surfaced on send.
      }
      if (typeof storage.hydrate === "function") {
        try {
          await storage.hydrate([...STORAGE_HYDRATION_KEYS]);
        } catch {
          // Existing shell state remains available if optional hydration fails.
        }
      }
      if (cancelled) return;
      initialized = true;
      rebuild();
      if (rebuildPending) {
        rebuildPending = false;
        scheduleRebuild();
      }
    })();

    return () => {
      cancelled = true;
      unsubscribe?.();
      storage.disposeDurableRecovery?.();
    };
  }, [client, recoveryConnectionScope]);

  useEffect(() => {
    if (!stores) return;
    void stores.sessions.getState().loadSessions();
  }, [stores]);

  useEffect(() => {
    return () => {
      if (!stores) return;
      for (const mode of ["chat", "code", "hive"] as const) {
        stores.modes[mode].session.getState().cleanup();
      }
    };
  }, [stores]);

  return (
    <StoresContext.Provider value={stores}>
      {isConnectionHandoff ? null : children}
    </StoresContext.Provider>
  );
}

function useStoresContext() {
  return useContext(StoresContext);
}

function useRequiredStoresContext(): StoresContextValue {
  const ctx = useStoresContext();
  if (!ctx) {
    throw new Error("Stores are not ready");
  }
  return ctx;
}

export function useSessionsStore<T>(
  selector: (state: SessionsStoreState) => T,
): T {
  const store = useRequiredStoresContext().sessions;
  return useStore(store, selector);
}

export function useSessionStore<T>(
  selector: (state: SessionStoreState) => T,
  mode: SessionType = "chat",
): T {
  const store = useRequiredStoresContext().modes[mode].session;
  return useStore(store, selector);
}

export function useWorkspaceStore<T>(
  selector: (state: WorkspaceStoreState) => T,
  mode: SessionType = "code",
): T {
  const store = useRequiredStoresContext().modes[mode].workspace;
  return useStore(store, selector);
}

export function useGitStore<T>(selector: (state: GitStoreState) => T): T {
  const store = useRequiredStoresContext().git;
  return useStore(store, selector);
}

export function usePlanStore<T>(
  selector: (state: PlanStoreState) => T,
  mode: SessionType = "chat",
): T {
  const store = useRequiredStoresContext().modes[mode].plan;
  return useStore(store, selector);
}

export function useStores() {
  return useStoresContext();
}

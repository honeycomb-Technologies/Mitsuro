import {
  clearPairingIntent,
  completePairingConnection,
  getPairingIntent,
  receivePairingIntent,
  requestPairingScreen,
  savePairingClaim,
  subscribePairingIntent,
} from "../platform/pairing-intent";
import { platformFetch } from "../platform/fetch";
import { useEffect, useRef, useState, useSyncExternalStore } from "react";
import {
  AppState,
  Platform,
  Pressable,
  ScrollView,
  Text,
  TextInput,
} from "react-native";
import { CameraView, useCameraPermissions } from "expo-camera";
import { useRouter } from "expo-router";
import {
  type PairingClaim,
  pairingRequest,
  type PairingStatus,
} from "@mitsuro/api";
import { useConnection } from "../hooks/useConnection";
import { useThemeContext } from "../hooks/useTheme";

export default function PairScreen() {
  const router = useRouter();
  const { connect, isConfigured } = useConnection();
  const { theme } = useThemeContext();
  const [permission, requestPermission] = useCameraPermissions();
  const [scanning, setScanning] = useState(false);
  const [foreground, setForeground] = useState(
    AppState.currentState === "active",
  );
  const [link, setLink] = useState("");
  const [name, setName] = useState(
    Platform.OS === "ios"
      ? "iPhone / iPad"
      : Platform.OS === "android"
      ? "Android"
      : "Browser",
  );
  const [message, setMessage] = useState("");
  const [busy, setBusy] = useState(false);
  const active = useRef<AbortController | null>(null);
  const mounted = useRef(true);
  const claimBusy = useRef(false);
  const loadCode = (value: string) => {
    try {
      if (!receivePairingIntent(value)) throw new Error("Invalid code");
      active.current?.abort();
      setApproved(false);
      setLink("");
      setScanning(false);
      setMessage("");
    } catch {
      setMessage("This is not a valid Mitsuro pairing code.");
    }
  };
  const intent = useSyncExternalStore(
    subscribePairingIntent,
    getPairingIntent,
    getPairingIntent,
  );
  const [approved, setApproved] = useState(false);
  const [retry, setRetry] = useState(0);
  const destination = intent?.destination ?? null;
  const claim = intent?.claim ?? null;
  useEffect(() => {
    mounted.current = true;
    requestPairingScreen();
    const state = AppState.addEventListener("change", (value) => {
      setForeground(value === "active");
      if (value !== "active") setScanning(false);
    });
    return () => {
      mounted.current = false;
      active.current?.abort();
      state.remove();
    };
  }, []);
  useEffect(() => {
    if (!intent || !claim || !destination || !foreground) return;
    const abort = new AbortController();
    active.current = abort;
    let timer: ReturnType<typeof setTimeout>;
    const poll = async () => {
      try {
        const status = await pairingRequest<PairingStatus>(
          destination.origin,
          "status",
          abort.signal,
          { credential: claim.credential },
          platformFetch as typeof fetch | undefined,
        );
        if (abort.signal.aborted) return;
        if (status.status === "approved") {
          setApproved(true);
          const connected = await completePairingConnection(intent, connect);
          if (connected && !getPairingIntent()) {
            router.replace("/");
            return;
          }
          if (!abort.signal.aborted) {
            if (connected) router.replace("/");
            else {setMessage(
                "Pairing approved, but connection failed. Tap Retry connection.",
              );}
          }
          return;
        }
        if (status.status === "unavailable") {
          throw new Error("Pairing was denied or expired. Create a new code.");
        }
        if (Date.now() >= claim.expires_at * 1000) {
          throw new Error("Pairing expired. Create a new code on your server.");
        }
        timer = setTimeout(poll, 2000);
      } catch (error) {
        if (!abort.signal.aborted) {
          setMessage(
            error instanceof Error ? error.message : "Pairing failed.",
          );
        }
      }
    };
    void poll();
    return () => {
      abort.abort();
      clearTimeout(timer);
    };
  }, [intent, claim, destination, foreground, connect, router, retry]);
  const text = { color: theme.colors.foreground };
  const button = (label: string, action: () => void, disabled = busy) => (
    <Pressable
      accessibilityRole="button"
      disabled={disabled}
      onPress={action}
      style={{ paddingVertical: 14 }}
    >
      <Text style={[text, { fontWeight: "600" }]}>{label}</Text>
    </Pressable>
  );
  return (
    <ScrollView
      contentContainerStyle={{
        padding: 28,
        paddingTop: 64,
        gap: 16,
        flexGrow: 1,
        backgroundColor: theme.colors.background,
      }}
      keyboardShouldPersistTaps="handled"
    >
      <Text style={[text, { fontSize: 28, fontWeight: "600" }]}>
        Pair with your server
      </Text>
      <Text style={text}>
        Open Connection settings on your trusted server and create a QR code.
      </Text>
      {!claim && (
        <>
          {button("Scan QR code", () => {
            void (async () => {
              const access = permission?.granted
                ? permission
                : await requestPermission();
              if (access.granted && mounted.current) setScanning(true);
              else {setMessage(
                  "Camera access is needed to scan. You can also paste a pairing link.",
                );}
            })();
          })}
          {scanning && foreground && permission?.granted && (
            <CameraView
              style={{ height: 300 }}
              barcodeScannerSettings={{ barcodeTypes: ["qr"] }}
              onBarcodeScanned={({ data }) => loadCode(data)}
            />
          )}
          <TextInput
            accessibilityLabel="Pairing link"
            placeholder="Or paste a pairing link"
            value={link}
            onChangeText={setLink}
            autoCapitalize="none"
            autoCorrect={false}
            secureTextEntry
            style={[text, {
              borderColor: theme.colors.border,
              borderWidth: 1,
              padding: 12,
              borderRadius: 8,
            }]}
          />
          {button("Read pairing link", () => loadCode(link))}
        </>
      )}
      {destination && (
        <>
          <Text style={text}>Server: {destination.origin}</Text>
          {!claim
            ? (
              <>
                <TextInput
                  accessibilityLabel="Device name"
                  value={name}
                  onChangeText={setName}
                  maxLength={80}
                  style={[text, {
                    borderColor: theme.colors.border,
                    borderWidth: 1,
                    padding: 12,
                  }]}
                />
                <Text style={text}>
                  Continue only if this is your server. Your current connection
                  changes after the server owner approves.
                </Text>
                {button(busy ? "Requesting…" : "Request pairing", () => {
                  if (claimBusy.current) return;
                  claimBusy.current = true;
                  setBusy(true);
                  setMessage("");
                  const abort = new AbortController();
                  active.current?.abort();
                  active.current = abort;
                  void pairingRequest<PairingClaim>(
                    destination.origin,
                    "claim",
                    abort.signal,
                    { secret: destination.secret, name },
                    platformFetch as typeof fetch | undefined,
                  ).then((value) => {
                    if (!abort.signal.aborted && intent) {
                      savePairingClaim(intent, value);
                    }
                  }).catch((error) => {
                    if (!abort.signal.aborted) {
                      setMessage(
                        error instanceof Error
                          ? error.message
                          : "Pairing failed.",
                      );
                    }
                  }).finally(() => {
                    claimBusy.current = false;
                    if (mounted.current) setBusy(false);
                  });
                })}
              </>
            )
            : (
              <>
                <Text style={[text, { fontSize: 30, fontWeight: "600" }]}>
                  {claim.comparison_code}
                </Text>
                <Text style={text}>
                  Compare this code on your trusted server, then choose “Codes
                  match — allow device” there. Waiting for approval…
                </Text>
              </>
            )}
        </>
      )}
      {approved && !!message && button("Retry connection", () => {
        setMessage("");
        setRetry((value) => value + 1);
      })}
      {!!message && (
        <Text accessibilityRole="alert" style={text}>{message}</Text>
      )}
      {button("Cancel", () => {
        active.current?.abort();
        clearPairingIntent();
        if (router.canGoBack()) router.back();
        else router.replace(isConfigured ? "/" : "/onboarding");
      }, false)}
    </ScrollView>
  );
}

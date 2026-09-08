import { setStringAsync } from "../../platform/clipboard";
import { router } from "expo-router";
import { useEffect, useMemo, useRef, useState } from "react";
import { AppState, Pressable, Text, TextInput, View } from "react-native";
import Svg, { Path, Rect } from "react-native-svg";
import { pairingQrGeometry } from "./pairing-qr";
import {
  MitsuroApiError,
  type PairingInvitation,
  pairingLink,
  pairingOrigin,
  type PairingOverview,
} from "@mitsuro/api";
import { useConnection } from "../../hooks/useConnection";
import { useThemeContext } from "../../hooks/useTheme";
import { GlassCard } from "../ui/GlassCard";

function InvitationQr({ value }: { value: string }) {
  const { size, path } = useMemo(() => pairingQrGeometry(value), [value]);
  return (
    <Svg
      width={256}
      height={256}
      viewBox={`0 0 ${size} ${size}`}
      accessibilityLabel="One-use Mitsuro pairing QR code"
    >
      <Rect width={size} height={size} fill="white" />
      <Path d={path} fill="black" />
    </Svg>
  );
}

/** Mounted only while Connection settings are open. Secrets stay in component memory. */
export function DevicePairingSection() {
  const { client, serverUrl, serverToken } = useConnection();
  const { theme } = useThemeContext();
  const [origin, setOrigin] = useState(serverUrl ?? "");
  const [overview, setOverview] = useState<PairingOverview | null>(null);
  const [invitation, setInvitation] = useState<PairingInvitation | null>(null);
  const [message, setMessage] = useState("");
  const [busy, setBusy] = useState(false);
  const busyRef = useRef(false);
  const [foreground, setForeground] = useState(
    AppState.currentState === "active",
  );
  const [seconds, setSeconds] = useState(0);
  const owner = !!client && !serverToken?.startsWith("mitsuro_device_");
  useEffect(() => {
    if (!owner || !client) return;
    const abort = new AbortController();
    void client.getServerAccess(abort.signal).then((access) => {
      if (abort.signal.aborted || !access.remote_launch_url) return;
      try {
        const suggested = pairingOrigin(access.remote_launch_url);
        setOrigin((current) =>
          current === (serverUrl ?? "") && !current.startsWith("https:")
            ? suggested
            : current
        );
      } catch {
        /* Keep the explicit address field for custom HTTPS gateways. */
      }
    }).catch(() => {});
    return () => abort.abort();
  }, [client, owner, serverUrl]);
  useEffect(() => {
    const subscription = AppState.addEventListener(
      "change",
      (state) => setForeground(state === "active"),
    );
    return () => subscription.remove();
  }, []);
  useEffect(() => {
    if (!owner || !client || !foreground) return;
    const abort = new AbortController();
    let timer: ReturnType<typeof setTimeout>;
    let supported = true;
    async function refresh() {
      try {
        const data = await client!.listPairedDevices(abort.signal);
        if (!abort.signal.aborted) setOverview(data);
      } catch (error) {
        if (
          error instanceof MitsuroApiError &&
          [401, 403, 404].includes(error.status)
        ) supported = false;
        if (!abort.signal.aborted) {
          setMessage(
            "Device pairing requires a server with pairing support and owner access.",
          );
        }
      } finally {
        if (!abort.signal.aborted && supported) {
          timer = setTimeout(refresh, 2500);
        }
      }
    }
    void refresh();
    return () => {
      abort.abort();
      clearTimeout(timer);
    };
  }, [client, owner, foreground]);
  useEffect(() => {
    if (!invitation) return;
    const tick = () => {
      const remaining = Math.max(
        0,
        invitation.expires_at - Math.floor(Date.now() / 1000),
      );
      setSeconds(remaining);
      if (!remaining) setInvitation(null);
    };
    tick();
    const timer = setInterval(tick, 1000);
    return () => clearInterval(timer);
  }, [invitation]);
  const action = async (run: () => Promise<unknown>) => {
    if (busyRef.current) return;
    busyRef.current = true;
    setBusy(true);
    setMessage("");
    try {
      await run();
    } catch (error) {
      setMessage(error instanceof Error ? error.message : "Pairing failed.");
    } finally {
      busyRef.current = false;
      setBusy(false);
    }
  };
  if (!owner) {
    return (
      <Pressable
        accessibilityRole="button"
        onPress={() => router.push("/pair")}
        style={{ padding: 12 }}
      >
        <Text style={{ color: theme.colors.foreground }}>
          Scan a server QR code
        </Text>
      </Pressable>
    );
  }
  const text = { color: theme.colors.foreground };
  const button = (label: string, run: () => Promise<unknown>) => (
    <Pressable
      accessibilityRole="button"
      disabled={busy}
      onPress={() => void action(run)}
      style={{ paddingVertical: 12, opacity: busy ? 0.5 : 1 }}
    >
      <Text style={text}>{label}</Text>
    </Pressable>
  );
  return (
    <GlassCard compact>
      <View style={{ gap: 12 }}>
        <Text style={[text, { fontWeight: "600" }]}>Pair a device</Text>
        <Text style={text}>
          Use the HTTPS address your phone can reach. Scan the code, then
          compare the code on both devices before allowing access.
        </Text>
        <TextInput
          accessibilityLabel="Pairing server address"
          value={origin}
          onChangeText={setOrigin}
          autoCapitalize="none"
          autoCorrect={false}
          placeholder="https://your-server"
          style={[text, {
            padding: 10,
            borderWidth: 1,
            borderColor: theme.colors.border,
            borderRadius: 8,
          }]}
        />
        {button(invitation ? "Replace QR code" : "Create QR code", async () => {
          const destination = pairingOrigin(origin);
          if (invitation) await client!.cancelPairing(invitation.id);
          setInvitation(await client!.createPairingInvitation(destination));
        })}
        {invitation && (
          <View style={{ alignItems: "center", gap: 8 }}>
            <InvitationQr value={pairingLink(invitation)} />
            <Text style={text}>Expires in {seconds}s · one use</Text>
            {button("Copy pairing link", async () => {
              await setStringAsync(pairingLink(invitation));
              setMessage(
                "One-use pairing link copied. It expires with this QR code.",
              );
            })}
            {button("Cancel pairing", async () => {
              await client!.cancelPairing(invitation.id);
              setInvitation(null);
            })}
          </View>
        )}
        {overview?.pending.map((request) => (
          <View key={request.id} style={{ gap: 4 }}>
            <Text style={text}>{request.name}</Text>
            <Text style={[text, { fontSize: 24, fontWeight: "600" }]}>
              {request.comparison_code}
            </Text>
            <Text style={text}>
              Allow only if this matches your phone. This device will be able to
              run code and access your workspace.
            </Text>
            {button("Codes match — allow device", async () => {
              await client!.approvePairing(request.id);
              setInvitation(null);
              setOverview(await client!.listPairedDevices());
            })}
            {button("Deny", async () => {
              await client!.cancelPairing(request.id);
              setOverview(await client!.listPairedDevices());
            })}
          </View>
        ))}
        {overview?.devices.filter((device) => !device.revoked_at).map(
          (device) => (
            <View key={device.id}>
              <Text style={text}>
                {device.name} · expires{" "}
                {new Date(device.expires_at * 1000).toLocaleDateString()}
              </Text>
              {button(`Revoke ${device.name}`, async () => {
                await client!.revokePairedDevice(device.id);
                setOverview(await client!.listPairedDevices());
              })}
            </View>
          ),
        )}
        {!!message && (
          <Text accessibilityRole="alert" style={text}>{message}</Text>
        )}
      </View>
    </GlassCard>
  );
}

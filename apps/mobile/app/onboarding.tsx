import { colors } from '@mitsuro/ui';
import { useState } from 'react';
import {
  View,
  Text,
  TextInput,
  Pressable,
  StyleSheet,
  KeyboardAvoidingView,
  Platform,
  ActivityIndicator,
} from 'react-native';
import { SafeAreaView } from 'react-native-safe-area-context';
import * as Haptics from '../platform/haptics';
import { useThemeContext } from '../hooks/useTheme';
import { useConnection } from '../hooks/useConnection';
import { MitsuroWordmark } from '../components/brand';
import { router } from 'expo-router';

function inferInitialServerUrl(): string {
  if (Platform.OS !== 'web' || typeof window === 'undefined') return '';

  const { origin, hostname, port } = window.location;
  if (!origin || origin === 'null') return '';

  const isExpoDevServer =
    (hostname === 'localhost' || hostname === '127.0.0.1') && port === '5173';
  if (isExpoDevServer) return '';

  return origin.replace(/\/+$/, '');
}

export default function OnboardingScreen() {
  const { theme } = useThemeContext();
  const { connect } = useConnection();
  const [serverUrl, setServerUrl] = useState(inferInitialServerUrl);
  const [token, setToken] = useState('');
  const [isConnecting, setIsConnecting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleConnect = async () => {
    const url = (serverUrl.trim() || inferInitialServerUrl()).replace(/\/+$/, '');
    if (!url || !token.trim()) return;

    setIsConnecting(true);
    setError(null);
    Haptics.impactAsync(Haptics.ImpactFeedbackStyle.Medium);

    const success = await connect(url, token.trim());

    if (success) {
      Haptics.notificationAsync(Haptics.NotificationFeedbackType.Success);
      router.replace('/(tabs)');
    } else {
      Haptics.notificationAsync(Haptics.NotificationFeedbackType.Error);
      setError('Could not connect. Check your URL and token.');
    }

    setIsConnecting(false);
  };

  const t = theme.colors;
  const g = theme.colors.glass;
  const canConnect = Boolean((serverUrl.trim() || inferInitialServerUrl()) && token.trim());

  return (
    <SafeAreaView style={[styles.container, { backgroundColor: t.background }]}>
      <KeyboardAvoidingView
        style={styles.content}
        behavior={Platform.OS === 'ios' ? 'padding' : 'height'}
      >
        <View style={styles.header}>
          <MitsuroWordmark width={200} color={t.foreground} />
          <Text style={[styles.subtitle, { color: t.mutedForeground }]}>
            Connect to your server
          </Text>
        </View>

        <View style={styles.form}>
          <View style={[styles.inputGroup, { backgroundColor: g.background, borderColor: g.border }]}>
            <Text style={[styles.label, { color: t.mutedForeground }]}>Server URL</Text>
            <TextInput
              accessibilityLabel="Server URL"
              style={[styles.input, { color: t.foreground }]}
              value={serverUrl}
              onChangeText={setServerUrl}
              placeholder="https://device.tail123.ts.net:8443"
              placeholderTextColor={t.mutedForeground + '60'}
              autoCapitalize="none"
              autoCorrect={false}
              keyboardType="url"
              textContentType="URL"
            />
          </View>

          <View style={[styles.inputGroup, { backgroundColor: g.background, borderColor: g.border }]}>
            <Text style={[styles.label, { color: t.mutedForeground }]}>Remote Access Token</Text>
            <TextInput
              accessibilityLabel="Remote access token"
              style={[styles.input, { color: t.foreground }]}
              value={token}
              onChangeText={setToken}
              placeholder="mitsuro_remote_..."
              placeholderTextColor={t.mutedForeground + '60'}
              autoCapitalize="none"
              autoCorrect={false}
              secureTextEntry
            />
          </View>

          {error && (
            <Text style={[styles.error, { color: t.error }]}>{error}</Text>
          )}

          <Pressable accessibilityRole="button" onPress={() => router.push('/pair')} style={{ paddingVertical: 16 }}><Text style={{ color: theme.colors.foreground }}>Pair using a QR code</Text></Pressable>
          <Pressable
            accessibilityLabel="Connect to server"
            accessibilityRole="button"
            style={({ pressed }) => [
              styles.button,
              {
                backgroundColor: pressed ? t.userMessage + 'cc' : t.userMessage,
                opacity: isConnecting || !canConnect ? 0.5 : 1,
              },
            ]}
            onPress={handleConnect}
            disabled={isConnecting || !canConnect}
          >
            {isConnecting ? (
              <ActivityIndicator color={t.onAccent} size="small" />
            ) : (
              <Text style={styles.buttonText}>Connect</Text>
            )}
          </Pressable>
        </View>

        <Text style={[styles.hint, { color: t.mutedForeground }]}>
          Start your Mitsuro server with `mitsuro serve` and find your Tailscale URL in Settings → Remote Access.
        </Text>
      </KeyboardAvoidingView>
    </SafeAreaView>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
  },
  content: {
    flex: 1,
    justifyContent: 'center',
    paddingHorizontal: 24,
  },
  header: {
    alignItems: 'center',
    marginBottom: 48,
  },
  subtitle: {
    fontSize: 17,
    marginTop: 12,
  },
  form: {
    gap: 16,
  },
  inputGroup: {
    borderRadius: 16,
    borderWidth: StyleSheet.hairlineWidth,
    padding: 16,
  },
  label: {
    fontSize: 13,
    fontWeight: '500',
    marginBottom: 8,
    textTransform: 'uppercase',
    letterSpacing: 0.5,
  },
  input: {
    fontSize: 17,
    padding: 0,
  },
  error: {
    fontSize: 15,
    textAlign: 'center',
  },
  button: {
    borderRadius: 16,
    paddingVertical: 16,
    alignItems: 'center',
    marginTop: 8,
  },
  buttonText: {
    color: colors.onAccent,
    fontSize: 17,
    fontWeight: '600',
  },
  hint: {
    fontSize: 13,
    textAlign: 'center',
    marginTop: 32,
    lineHeight: 18,
  },
});

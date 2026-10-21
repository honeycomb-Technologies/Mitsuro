import { memo, useEffect, useMemo, useState } from "react";
import {
  ActivityIndicator,
  Pressable,
  Text,
  TextInput,
  View,
} from "react-native";
import {
  Bell,
  ChevronDown,
  ChevronRight,
  Cpu,
  ExternalLink,
  Link,
  LogOut,
  Moon,
  RefreshCw,
  Sun,
  Monitor,
  Play,
  Wifi,
  WifiOff,
  Square,
  Upload,
  X,
} from "lucide-react-native";

import type {
  PortEntry,
  PreviewSettings,
  PreviewSettingsPatch,
  ProviderStatus,
  SkillInfo,
} from "@mitsuro/api";
import type { ColorScheme } from "@mitsuro/ui";

import * as Haptics from "../../platform/haptics";
import { openURL } from "../../platform/linking";
import { useThemeContext } from "../../hooks/useTheme";
import { GlassCard } from "../ui/GlassCard";
import {
  ActiveOAuthFlow,
  MessageBanner,
  NotificationOption,
  Pill,
  PreviewDraftState,
  ProviderFormState,
  SchemeOption,
  previewStatusText,
} from "./shared";
import { styles } from "./styles";
import {
  clampSkillPageStart,
  nextSkillPageStart,
  previousSkillPageStart,
  SKILL_PAGE_SIZE,
} from "./skillWindow";

export function SettingsHeader({ onClose }: { onClose?: () => void }) {
  const { theme } = useThemeContext();
  const t = theme.colors;

  return (
    <View style={styles.header}>
      <Text style={[styles.title, { color: t.foreground }]}>Settings</Text>
      {onClose ? (
        <Pressable
          accessibilityLabel="Close settings"
          accessibilityRole="button"
          hitSlop={10}
          onPress={() => {
            void Haptics.impactAsync(Haptics.ImpactFeedbackStyle.Light);
            onClose();
          }}
          style={[styles.closeBtn, { backgroundColor: `${t.border}40` }]}
        >
          <X size={18} color={t.foreground} strokeWidth={2} />
        </Pressable>
      ) : null}
    </View>
  );
}
export function DiagnosticsSection({
  mode,
  runId,
  eventCount,
  nativePayloadCount,
  approximateBytes,
  uploadState,
  completionPending,
  isConnected,
  onStart,
  onStopAndUpload,
  onUpload,
}: {
  mode: "baseline" | "stress";
  runId: string | null;
  eventCount: number;
  nativePayloadCount: number;
  approximateBytes: number;
  uploadState: "idle" | "pending" | "uploading" | "uploaded" | "failed" | "unavailable";
  completionPending: boolean;
  isConnected: boolean;
  onStart: () => void;
  onStopAndUpload: () => void;
  onUpload: () => void;
}) {
  const { theme } = useThemeContext();
  const t = theme.colors;
  const statusTone = uploadState === "uploaded"
    ? "success"
    : uploadState === "failed" || uploadState === "unavailable"
      ? "error"
      : mode === "stress" || uploadState === "pending"
        ? "warning"
        : "neutral";

  return (
    <>
      <GlassCard compact>
        <View style={styles.stack}>
          <View style={styles.subsectionHeader}>
            <View style={styles.rowContent}>
              <Text style={[styles.rowTitle, { color: t.foreground }]}>Stress capture</Text>
              <Text style={[styles.rowSubtitle, { color: t.mutedForeground }]}>
                {runId ? `Run ${runId.slice(-12)}` : "Recorder starting…"}
              </Text>
            </View>
            <Pill
              label={mode === "stress" ? "Recording" : uploadState}
              tone={statusTone}
            />
          </View>

          <View style={styles.pillRow}>
            <Pill label={`${eventCount} events`} tone="info" />
            {nativePayloadCount > 0 ? (
              <Pill label={`${nativePayloadCount} native reports`} tone="warning" />
            ) : null}
            <Pill label={`${Math.ceil(approximateBytes / 1024)} KB`} />
            <Pill label="No chat or file content" tone="success" />
          </View>

          <View style={styles.actionsWrap}>
            {mode === "stress" ? (
              <Pressable
                accessibilityRole="button"
                accessibilityLabel={isConnected
                  ? "Stop and upload diagnostic capture"
                  : "Stop and save diagnostic capture"}
                onPress={onStopAndUpload}
                style={[styles.smallActionBtn, { borderColor: t.border }]}
              >
                <Square size={14} color={t.warning} strokeWidth={1.8} />
                <Text style={[styles.smallActionText, { color: t.warning }]}>Stop & {isConnected ? "upload" : "save"}</Text>
              </Pressable>
            ) : (
              <Pressable
                accessibilityRole="button"
                accessibilityLabel="Start diagnostic capture"
                onPress={onStart}
                disabled={!runId || completionPending || uploadState === "uploading"}
                style={[styles.smallActionBtn, { borderColor: t.border }]}
              >
                <Play size={14} color={t.userMessage} strokeWidth={1.8} />
                <Text style={[styles.smallActionText, { color: t.userMessage }]}>Start 10-minute capture</Text>
              </Pressable>
            )}
            <Pressable
              accessibilityRole="button"
              accessibilityLabel={mode === "stress"
                ? "Upload diagnostic checkpoint"
                : "Upload diagnostics now"}
              onPress={onUpload}
              disabled={!isConnected || uploadState === "uploading" || (eventCount === 0 && nativePayloadCount === 0)}
              style={[styles.smallActionBtn, { borderColor: t.border }]}
            >
              {uploadState === "uploading" ? (
                <ActivityIndicator color={t.foreground} size="small" />
              ) : (
                <Upload size={14} color={t.foreground} strokeWidth={1.8} />
              )}
              <Text style={[styles.smallActionText, { color: t.foreground }]}>
                {mode === "stress" ? "Upload checkpoint" : "Upload now"}
              </Text>
            </Pressable>
          </View>
        </View>
      </GlassCard>
    </>
  );
}

export function ConnectionSection({
  isConfigured,
  isConnected,
  status,
  serverUrl,
  connectError,
  inputUrl,
  inputToken,
  isConnecting,
  onInputUrlChange,
  onInputTokenChange,
  onConnect,
  onReconnect,
  onDisconnect,
}: {
  isConfigured: boolean;
  isConnected: boolean;
  status: string;
  serverUrl?: string | null;
  connectError: string | null;
  inputUrl: string;
  inputToken: string;
  isConnecting: boolean;
  onInputUrlChange: (value: string) => void;
  onInputTokenChange: (value: string) => void;
  onConnect: () => void;
  onReconnect: () => void;
  onDisconnect: () => void;
}) {
  const { theme } = useThemeContext();
  const t = theme.colors;
  const g = theme.colors.glass;

  return (
    <>
      {isConfigured ? (
        <GlassCard compact>
          <View style={styles.row}>
            {isConnected ? (
              <Wifi size={20} color={t.success} strokeWidth={1.8} />
            ) : (
              <WifiOff size={20} color={t.error} strokeWidth={1.8} />
            )}
            <View style={styles.rowContent}>
              <Text style={[styles.rowTitle, { color: t.foreground }]}>
                {isConnected
                  ? "Connected"
                  : status === "disconnecting"
                    ? "Removing saved connection..."
                    : status === "connecting"
                      ? "Connecting..."
                    : "Disconnected"}
              </Text>
              {serverUrl ? (
                <Text
                  style={[styles.rowSubtitle, { color: t.mutedForeground }]}
                  numberOfLines={1}
                >
                  {serverUrl}
                </Text>
              ) : null}
            </View>
          </View>

          <MessageBanner text={connectError} />

          <View style={[styles.separator, { backgroundColor: t.border }]} />

          <View style={styles.actions}>
            <Pressable
              disabled={isConnecting}
              onPress={() => {
                void Haptics.impactAsync(Haptics.ImpactFeedbackStyle.Light);
                onReconnect();
              }}
              style={[styles.actionBtn, isConnecting && { opacity: 0.5 }]}
            >
              <RefreshCw size={18} color={t.userMessage} strokeWidth={1.8} />
              <Text style={[styles.actionText, { color: t.userMessage }]}>Reconnect</Text>
            </Pressable>

            <Pressable
              disabled={isConnecting}
              onPress={onDisconnect}
              style={[styles.actionBtn, isConnecting && { opacity: 0.5 }]}
            >
              <LogOut size={18} color={t.error} strokeWidth={1.8} />
              <Text style={[styles.actionText, { color: t.error }]}>Disconnect</Text>
            </Pressable>
          </View>
        </GlassCard>
      ) : (
        <GlassCard compact>
          <View style={styles.connectForm}>
            <View style={styles.row}>
              <Link size={20} color={t.mutedForeground} strokeWidth={1.8} />
              <Text style={[styles.rowTitle, { color: t.foreground }]}>Connect to Server</Text>
            </View>

            <View
              style={[
                styles.inputWrap,
                { backgroundColor: g.background, borderColor: g.border },
              ]}
            >
              <Text style={[styles.inputLabel, { color: t.mutedForeground }]}>SERVER URL</Text>
              <TextInput
                style={[styles.input, { color: t.foreground }]}
                value={inputUrl}
                onChangeText={onInputUrlChange}
                placeholder="https://device.tail123.ts.net:8443"
                placeholderTextColor={`${t.mutedForeground}60`}
                autoCapitalize="none"
                autoCorrect={false}
                keyboardType="url"
              />
            </View>

            <View
              style={[
                styles.inputWrap,
                { backgroundColor: g.background, borderColor: g.border },
              ]}
            >
              <Text style={[styles.inputLabel, { color: t.mutedForeground }]}>TOKEN</Text>
              <TextInput
                style={[styles.input, { color: t.foreground }]}
                value={inputToken}
                onChangeText={onInputTokenChange}
                placeholder="mitsuro_remote_..."
                placeholderTextColor={`${t.mutedForeground}60`}
                autoCapitalize="none"
                autoCorrect={false}
                secureTextEntry
              />
            </View>

            <MessageBanner text={connectError} />

            <Pressable
              onPress={onConnect}
              disabled={isConnecting || !inputUrl.trim() || !inputToken.trim()}
              style={[
                styles.connectBtn,
                {
                  backgroundColor: t.userMessage,
                  opacity:
                    isConnecting || !inputUrl.trim() || !inputToken.trim() ? 0.5 : 1,
                },
              ]}
            >
              {isConnecting ? (
                <ActivityIndicator color={t.onAccent} size="small" />
              ) : (
                <Text style={styles.connectBtnText}>Connect</Text>
              )}
            </Pressable>
          </View>
        </GlassCard>
      )}
    </>
  );
}

export function ProvidersSection({
  isConnected,
  providersLoading,
  providers,
  providerForms,
  providerBusyKey,
  providerMessage,
  activeOAuthFlow,
  oauthCode,
  onProviderFormChange,
  onSaveCredential,
  onDeleteCredential,
  onStartOAuth,
  onExchangeOAuthCode,
  onRevokeOAuth,
  onOauthCodeChange,
}: {
  isConnected: boolean;
  providersLoading: boolean;
  providers: ProviderStatus[];
  providerForms: ProviderFormState;
  providerBusyKey: string | null;
  providerMessage: string | null;
  activeOAuthFlow: ActiveOAuthFlow | null;
  oauthCode: string;
  onProviderFormChange: (providerId: string, value: string) => void;
  onSaveCredential: (providerId: string) => void;
  onDeleteCredential: (providerId: string) => void;
  onStartOAuth: (providerId: string) => void;
  onExchangeOAuthCode: () => void;
  onRevokeOAuth: (providerId: string) => void;
  onOauthCodeChange: (value: string) => void;
}) {
  const { theme } = useThemeContext();
  const t = theme.colors;
  const g = theme.colors.glass;
  const [expandedProviderId, setExpandedProviderId] = useState<string | null>(
    null,
  );

  return (
    <>
      <GlassCard compact>
        {!isConnected ? (
          <Text style={[styles.emptyText, { color: t.mutedForeground }]}> 
            Connect to a server to manage provider credentials.
          </Text>
        ) : providersLoading ? (
          <View style={styles.loadingRow}>
            <ActivityIndicator color={t.userMessage} size="small" />
            <Text style={[styles.loadingText, { color: t.mutedForeground }]}>Loading providers…</Text>
          </View>
        ) : (
          <View style={styles.stack}>
            <MessageBanner
              text={providerMessage}
              tone={
                providerMessage?.includes("saved") ||
                providerMessage?.includes("connected") ||
                providerMessage?.includes("revoked")
                  ? "info"
                  : "error"
              }
            />
            {providers.map((provider) => {
              const draft = providerForms[provider.id] ?? "";
              const expanded = expandedProviderId === provider.id;
              const ready = provider.configured || provider.has_oauth;
              return (
                <View key={provider.id} style={[styles.subsection, { borderColor: t.border }]}> 
                  <Pressable
                    accessibilityRole="button"
                    accessibilityState={{ expanded }}
                    onPress={() =>
                      setExpandedProviderId((current) =>
                        current === provider.id ? null : provider.id,
                      )
                    }
                    style={styles.subsectionHeader}
                  >
                    <View style={styles.rowContent}>
                      <Text style={[styles.rowTitle, { color: t.foreground }]}>{provider.name}</Text>
                    </View>
                    <Text
                      style={[
                        styles.rowSubtitle,
                        { color: ready ? t.success : t.mutedForeground },
                      ]}
                    >
                      {ready ? "Ready" : "Not configured"}
                    </Text>
                    {expanded ? (
                      <ChevronDown size={16} color={t.mutedForeground} />
                    ) : (
                      <ChevronRight size={16} color={t.mutedForeground} />
                    )}
                  </Pressable>

                  {expanded ? (
                    <>
                  <View
                    style={[
                      styles.inputWrap,
                      { backgroundColor: g.background, borderColor: g.border },
                    ]}
                  >
                    <Text style={[styles.inputLabel, { color: t.mutedForeground }]}>API KEY</Text>
                    <TextInput
                      style={[styles.input, { color: t.foreground }]}
                      value={draft}
                      onChangeText={(value) => onProviderFormChange(provider.id, value)}
                      placeholder={`Set ${provider.name} key`}
                      placeholderTextColor={`${t.mutedForeground}60`}
                      autoCapitalize="none"
                      autoCorrect={false}
                      secureTextEntry
                    />
                  </View>

                  <View style={styles.actionsWrap}>
                    <Pressable
                      onPress={() => onSaveCredential(provider.id)}
                      disabled={!draft.trim() || providerBusyKey === `save:${provider.id}`}
                      style={[styles.smallActionBtn, { borderColor: t.border }]}
                    >
                      {providerBusyKey === `save:${provider.id}` ? (
                        <ActivityIndicator color={t.userMessage} size="small" />
                      ) : (
                        <Text style={[styles.smallActionText, { color: t.userMessage }]}>Save Key</Text>
                      )}
                    </Pressable>

                    {provider.configured ? (
                      <Pressable
                        onPress={() => onDeleteCredential(provider.id)}
                        disabled={providerBusyKey === `delete:${provider.id}`}
                        style={[styles.smallActionBtn, { borderColor: t.border }]}
                      >
                        <Text style={[styles.smallActionText, { color: t.error }]}>Delete Key</Text>
                      </Pressable>
                    ) : null}

                    {provider.supports_oauth ? (
                      <Pressable
                        onPress={() => onStartOAuth(provider.id)}
                        disabled={providerBusyKey === `oauth:${provider.id}`}
                        style={[styles.smallActionBtn, { borderColor: t.border }]}
                      >
                        <Text style={[styles.smallActionText, { color: t.foreground }]}>Start OAuth</Text>
                      </Pressable>
                    ) : null}

                    {provider.has_oauth ? (
                      <Pressable
                        onPress={() => onRevokeOAuth(provider.id)}
                        disabled={providerBusyKey === `revoke:${provider.id}`}
                        style={[styles.smallActionBtn, { borderColor: t.border }]}
                      >
                        <Text style={[styles.smallActionText, { color: t.warning }]}>Revoke OAuth</Text>
                      </Pressable>
                    ) : null}
                  </View>
                    </>
                  ) : null}
                </View>
              );
            })}

            {activeOAuthFlow ? (
              <View style={[styles.subsection, { borderColor: t.border }]}> 
                <Text style={[styles.rowTitle, { color: t.foreground }]}>OAuth in progress: {activeOAuthFlow.provider}</Text>
                <Text style={[styles.rowSubtitle, { color: t.mutedForeground }]}> 
                  Flow: {activeOAuthFlow.flowType.replace(/_/g, " ")}
                </Text>
                {activeOAuthFlow.userCode ? (
                  <Text style={[styles.oauthCodeHint, { color: t.foreground }]}>Device code: {activeOAuthFlow.userCode}</Text>
                ) : null}
                {activeOAuthFlow.pasteCode ? (
                  <>
                    <TextInput
                      style={[
                        styles.input,
                        styles.inlineInput,
                        {
                          color: t.foreground,
                          borderColor: g.border,
                          backgroundColor: g.background,
                        },
                      ]}
                      value={oauthCode}
                      onChangeText={onOauthCodeChange}
                      placeholder="Paste OAuth code"
                      placeholderTextColor={`${t.mutedForeground}60`}
                      autoCapitalize="none"
                      autoCorrect={false}
                    />
                    <View style={styles.actionsWrap}>
                      <Pressable
                        onPress={onExchangeOAuthCode}
                        disabled={
                          !oauthCode.trim() ||
                          providerBusyKey === `exchange:${activeOAuthFlow.provider}`
                        }
                        style={[styles.smallActionBtn, { borderColor: t.border }]}
                      >
                        <Text style={[styles.smallActionText, { color: t.userMessage }]}>Exchange Code</Text>
                      </Pressable>
                      <Pressable
                        onPress={() =>
                          void openURL(
                            activeOAuthFlow.verificationUriComplete ?? activeOAuthFlow.authUrl,
                          )
                        }
                        style={[styles.smallActionBtn, { borderColor: t.border }]}
                      >
                        <ExternalLink size={14} color={t.foreground} strokeWidth={1.8} />
                        <Text style={[styles.smallActionText, { color: t.foreground }]}>Open Auth</Text>
                      </Pressable>
                    </View>
                  </>
                ) : (
                  <Text style={[styles.rowSubtitle, { color: t.mutedForeground }]}> 
                    Finish the sign-in flow in the browser. This panel will refresh provider status automatically.
                  </Text>
                )}
              </View>
            ) : null}
          </View>
        )}
      </GlassCard>
    </>
  );
}

export function SkillsSection({
  isConnected,
  loading,
  skills,
  message,
  pageStart,
  onPageStartChange,
}: {
  isConnected: boolean;
  loading: boolean;
  skills: SkillInfo[];
  message: string | null;
  pageStart: number;
  onPageStartChange: (nextStart: number) => void;
}) {
  const { theme } = useThemeContext();
  const t = theme.colors;
  const clampedSkillPageStart = clampSkillPageStart(
    pageStart,
    skills.length,
  );
  const visibleSkills = useMemo(
    () =>
      skills.slice(
        clampedSkillPageStart,
        clampedSkillPageStart + SKILL_PAGE_SIZE,
      ),
    [clampedSkillPageStart, skills],
  );
  const skillPageEnd = clampedSkillPageStart + visibleSkills.length;
  const hasPreviousSkillPage = clampedSkillPageStart > 0;
  const hasNextSkillPage = skillPageEnd < skills.length;

  return (
    <>
      <GlassCard compact>
        {!isConnected ? (
          <Text style={[styles.emptyText, { color: t.mutedForeground }]}>Connect to a server to inspect loaded skills.</Text>
        ) : loading ? (
          <View style={styles.loadingRow}>
            <ActivityIndicator color={t.userMessage} size="small" />
            <Text style={[styles.loadingText, { color: t.mutedForeground }]}>Loading skills…</Text>
          </View>
        ) : (
          <View style={styles.stack}>
            <MessageBanner text={message} />
            {skills.length === 0 ? (
              <Text style={[styles.emptyText, { color: t.mutedForeground }]}>No skills reported by the current server.</Text>
            ) : (
              visibleSkills.map((skill) => (
                <SkillRow
                  key={`${skill.source}:${skill.name}`}
                  skill={skill}
                />
              ))
            )}
            {hasPreviousSkillPage || hasNextSkillPage ? (
              <View style={styles.actionsWrap}>
                {hasPreviousSkillPage ? (
                  <Pressable
                    accessibilityRole="button"
                    accessibilityLabel="Show previous skills"
                    onPress={() =>
                      onPageStartChange(
                        previousSkillPageStart(clampedSkillPageStart),
                      )
                    }
                    style={[styles.smallActionBtn, { borderColor: t.border }]}
                  >
                    <Text style={[styles.smallActionText, { color: t.userMessage }]}>
                      Previous
                    </Text>
                  </Pressable>
                ) : null}
                <Pill
                  label={`${clampedSkillPageStart + 1}-${skillPageEnd} of ${skills.length}`}
                  tone="info"
                />
                {hasNextSkillPage ? (
                  <Pressable
                    accessibilityRole="button"
                    accessibilityLabel="Show next skills"
                    onPress={() =>
                      onPageStartChange(
                        nextSkillPageStart(
                          clampedSkillPageStart,
                          skills.length,
                        ),
                      )
                    }
                    style={[styles.smallActionBtn, { borderColor: t.border }]}
                  >
                    <Text style={[styles.smallActionText, { color: t.userMessage }]}>
                      Next
                    </Text>
                  </Pressable>
                ) : null}
              </View>
            ) : null}
          </View>
        )}
      </GlassCard>
    </>
  );
}

const SkillRow = memo(function SkillRow({ skill }: { skill: SkillInfo }) {
  const { theme } = useThemeContext();
  const t = theme.colors;

  return (
    <View style={[styles.subsection, { borderColor: t.border }]}>
      <View style={styles.subsectionHeader}>
        <View style={styles.rowContent}>
          <Text style={[styles.rowTitle, { color: t.foreground }]}>{skill.name}</Text>
          <Text style={[styles.rowSubtitle, { color: t.mutedForeground }]}>
            {skill.description}
          </Text>
        </View>
        <Pill
          label={skill.source}
          tone={skill.source === "project" ? "info" : "neutral"}
        />
      </View>

      <View style={styles.pillRow}>
        {skill.version ? <Pill label={`v${skill.version}`} /> : null}
        {skill.author ? <Pill label={skill.author} /> : null}
        {skill.tags.slice(0, 4).map((tag) => (
          <Pill key={tag} label={`#${tag}`} tone="info" />
        ))}
      </View>
    </View>
  );
});

export function PreviewSection({
  isConnected,
  loading,
  previewSettings,
  previewPorts,
  previewDraft,
  busyKey,
  message,
  onToggle,
  onSaveNumbers,
  onDraftChange,
  onRefresh,
  onTogglePinnedPort,
  onHidePort,
}: {
  isConnected: boolean;
  loading: boolean;
  previewSettings: PreviewSettings | null;
  previewPorts: PortEntry[];
  previewDraft: PreviewDraftState;
  busyKey: string | null;
  message: string | null;
  onToggle: (patch: PreviewSettingsPatch) => void;
  onSaveNumbers: () => void;
  onDraftChange: (draft: PreviewDraftState) => void;
  onRefresh: () => void;
  onTogglePinnedPort: (port: PortEntry) => void;
  onHidePort: (port: PortEntry) => void;
}) {
  const { theme } = useThemeContext();
  const t = theme.colors;
  const g = theme.colors.glass;

  return (
    <>
      <GlassCard compact>
        {!isConnected ? (
          <Text style={[styles.emptyText, { color: t.mutedForeground }]}>Connect to a server to manage preview settings.</Text>
        ) : loading ? (
          <View style={styles.loadingRow}>
            <ActivityIndicator color={t.userMessage} size="small" />
            <Text style={[styles.loadingText, { color: t.mutedForeground }]}>Loading preview settings…</Text>
          </View>
        ) : !previewSettings ? (
          <Text style={[styles.emptyText, { color: t.mutedForeground }]}>Preview settings are unavailable.</Text>
        ) : (
          <View style={styles.stack}>
            <MessageBanner
              text={message}
              tone={message?.includes("failed") ? "error" : "info"}
            />

            <View style={[styles.subsection, { borderColor: t.border }]}> 
              <View style={styles.subsectionHeader}>
                <View style={styles.rowContent}>
                  <Text style={[styles.rowTitle, { color: t.foreground }]}>Forwarding enabled</Text>
                  <Text style={[styles.rowSubtitle, { color: t.mutedForeground }]}>Detect and proxy local development servers.</Text>
                </View>
                <Pressable
                  onPress={() => onToggle({ enabled: !previewSettings.enabled })}
                  disabled={busyKey === "toggle"}
                  style={[
                    styles.toggle,
                    { backgroundColor: previewSettings.enabled ? t.userMessage : t.border },
                  ]}
                >
                  <View
                    style={[
                      styles.toggleKnob,
                      { alignSelf: previewSettings.enabled ? "flex-end" : "flex-start" },
                    ]}
                  />
                </Pressable>
              </View>
            </View>

            <View style={[styles.subsection, { borderColor: t.border }]}> 
              <View style={styles.subsectionHeader}>
                <View style={styles.rowContent}>
                  <Text style={[styles.rowTitle, { color: t.foreground }]}>HTTP-like only</Text>
                  <Text style={[styles.rowSubtitle, { color: t.mutedForeground }]}>Filter the preview list to ports that look like web servers.</Text>
                </View>
                <Pressable
                  onPress={() => onToggle({ show_only_http_like: !previewSettings.show_only_http_like })}
                  style={[
                    styles.toggle,
                    {
                      backgroundColor: previewSettings.show_only_http_like
                        ? t.userMessage
                        : t.border,
                    },
                  ]}
                >
                  <View
                    style={[
                      styles.toggleKnob,
                      {
                        alignSelf: previewSettings.show_only_http_like
                          ? "flex-end"
                          : "flex-start",
                      },
                    ]}
                  />
                </Pressable>
              </View>

              <View style={[styles.subsectionHeader, styles.compactHeader]}>
                <View style={styles.rowContent}>
                  <Text style={[styles.rowTitle, { color: t.foreground }]}>Allow non-HTTP embed</Text>
                  <Text style={[styles.rowSubtitle, { color: t.mutedForeground }]}>Keep non-HTTP listeners available to advanced preview flows.</Text>
                </View>
                <Pressable
                  onPress={() =>
                    onToggle({
                      allow_force_open_non_http: !previewSettings.allow_force_open_non_http,
                    })
                  }
                  style={[
                    styles.toggle,
                    {
                      backgroundColor: previewSettings.allow_force_open_non_http
                        ? t.userMessage
                        : t.border,
                    },
                  ]}
                >
                  <View
                    style={[
                      styles.toggleKnob,
                      {
                        alignSelf: previewSettings.allow_force_open_non_http
                          ? "flex-end"
                          : "flex-start",
                      },
                    ]}
                  />
                </Pressable>
              </View>
            </View>

            <View style={[styles.subsection, { borderColor: t.border }]}> 
              <Text style={[styles.rowTitle, { color: t.foreground }]}>Probe timing</Text>
              <View style={styles.twoCol}>
                <View
                  style={[
                    styles.inputWrap,
                    { flex: 1, backgroundColor: g.background, borderColor: g.border },
                  ]}
                >
                  <Text style={[styles.inputLabel, { color: t.mutedForeground }]}>AUTO REFRESH (s)</Text>
                  <TextInput
                    style={[styles.input, { color: t.foreground }]}
                    value={previewDraft.autoRefreshSecs}
                    onChangeText={(value) =>
                      onDraftChange({ ...previewDraft, autoRefreshSecs: value })
                    }
                    keyboardType="number-pad"
                  />
                </View>
                <View
                  style={[
                    styles.inputWrap,
                    { flex: 1, backgroundColor: g.background, borderColor: g.border },
                  ]}
                >
                  <Text style={[styles.inputLabel, { color: t.mutedForeground }]}>PROBE TIMEOUT (ms)</Text>
                  <TextInput
                    style={[styles.input, { color: t.foreground }]}
                    value={previewDraft.probeTimeoutMs}
                    onChangeText={(value) =>
                      onDraftChange({ ...previewDraft, probeTimeoutMs: value })
                    }
                    keyboardType="number-pad"
                  />
                </View>
              </View>

              <View style={styles.actionsWrap}>
                <Pressable
                  onPress={onSaveNumbers}
                  disabled={busyKey === "numbers"}
                  style={[styles.smallActionBtn, { borderColor: t.border }]}
                >
                  <Text style={[styles.smallActionText, { color: t.userMessage }]}>Save Timing</Text>
                </Pressable>
                <Pressable
                  onPress={onRefresh}
                  style={[styles.smallActionBtn, { borderColor: t.border }]}
                >
                  <RefreshCw size={14} color={t.foreground} strokeWidth={1.8} />
                  <Text style={[styles.smallActionText, { color: t.foreground }]}>Refresh Ports</Text>
                </Pressable>
              </View>
            </View>

            <View style={[styles.subsection, { borderColor: t.border }]}> 
              <Text style={[styles.rowTitle, { color: t.foreground }]}>Visible ports</Text>
              {previewPorts.length === 0 ? (
                <Text style={[styles.emptyText, { color: t.mutedForeground }]}>No forwardable ports are currently visible.</Text>
              ) : (
                previewPorts.map((port) => (
                  <View key={port.port} style={[styles.portRow, { borderColor: t.border }]}> 
                    <View style={styles.rowContent}>
                      <View style={styles.pillRow}>
                        <Text style={[styles.rowTitle, { color: t.foreground }]}>{port.name}</Text>
                        <Pill label={`:${port.port}`} tone="info" />
                        {port.pinned ? <Pill label="Pinned" tone="success" /> : null}
                      </View>
                      <Text style={[styles.rowSubtitle, { color: t.mutedForeground }]}> 
                        {previewStatusText(port)}
                      </Text>
                    </View>

                    <View style={styles.actionsWrap}>
                      <Pressable
                        onPress={() => onTogglePinnedPort(port)}
                        disabled={busyKey === `pin:${port.port}`}
                        style={[styles.smallActionBtn, { borderColor: t.border }]}
                      >
                        <Text style={[styles.smallActionText, { color: t.userMessage }]}> 
                          {port.pinned ? "Unpin" : "Pin"}
                        </Text>
                      </Pressable>
                      <Pressable
                        onPress={() => onHidePort(port)}
                        disabled={busyKey === `hide:${port.port}`}
                        style={[styles.smallActionBtn, { borderColor: t.border }]}
                      >
                        <Text style={[styles.smallActionText, { color: t.warning }]}>Hide</Text>
                      </Pressable>
                    </View>
                  </View>
                ))
              )}
            </View>
          </View>
        )}
      </GlassCard>
    </>
  );
}

export function AppearanceSection({
  colorScheme,
  schemeOptions,
  onSelect,
}: {
  colorScheme: ColorScheme;
  schemeOptions: SchemeOption[];
  onSelect: (scheme: ColorScheme) => void;
}) {
  const { theme } = useThemeContext();
  const t = theme.colors;

  return (
    <>
      <GlassCard compact>
        <View style={styles.schemeRow}>
          {schemeOptions.map((opt) => {
            const Icon = opt.icon;
            const selected = colorScheme === opt.key;
            return (
              <Pressable
                key={opt.key}
                onPress={() => {
                  void Haptics.impactAsync(Haptics.ImpactFeedbackStyle.Light);
                  onSelect(opt.key);
                }}
                style={[
                  styles.schemeBtn,
                  {
                    backgroundColor: selected ? `${t.userMessage}20` : "transparent",
                    borderColor: selected ? t.userMessage : t.border,
                  },
                ]}
              >
                <Icon
                  size={18}
                  color={selected ? t.userMessage : t.mutedForeground}
                  strokeWidth={1.8}
                />
                <Text
                  style={[
                    styles.schemeBtnText,
                    { color: selected ? t.userMessage : t.mutedForeground },
                  ]}
                >
                  {opt.label}
                </Text>
              </Pressable>
            );
          })}
        </View>
      </GlassCard>
    </>
  );
}

export function NotificationsSection({
  notificationLevel,
  registrationState,
  lastRegistrationError,
  pendingActionCount,
  notifOptions,
  onSelect,
}: {
  notificationLevel: string;
  registrationState: string;
  lastRegistrationError: string | null;
  pendingActionCount: number;
  notifOptions: NotificationOption[];
  onSelect: (key: NotificationOption["key"]) => void;
}) {
  const { theme } = useThemeContext();
  const t = theme.colors;

  return (
    <>
      <GlassCard compact>
        <View style={styles.row}>
          <Bell size={20} color={t.mutedForeground} strokeWidth={1.8} />
          <View style={styles.rowContent}>
            <Text style={[styles.rowTitle, { color: t.foreground }]}>Delivery level</Text>
            <Text style={[styles.rowSubtitle, { color: t.mutedForeground }]}>
              Delivery: {registrationState.replaceAll("_", " ")}
              {pendingActionCount > 0 ? ` · ${pendingActionCount} pending action` : ""}
            </Text>
            {lastRegistrationError ? (
              <Text style={[styles.rowSubtitle, { color: t.error }]}>
                {lastRegistrationError}
              </Text>
            ) : null}
          </View>
        </View>

        <View style={[styles.separator, { backgroundColor: t.border }]} />

        <View style={styles.schemeRow}>
          {notifOptions.map((opt) => {
            const Icon = opt.icon;
            const selected = notificationLevel === opt.key;
            return (
              <Pressable
                key={opt.key}
                onPress={() => {
                  void Haptics.impactAsync(Haptics.ImpactFeedbackStyle.Light);
                  onSelect(opt.key);
                }}
                style={[
                  styles.schemeBtn,
                  {
                    backgroundColor: selected ? `${t.userMessage}20` : "transparent",
                    borderColor: selected ? t.userMessage : t.border,
                  },
                ]}
              >
                <Icon
                  size={18}
                  color={selected ? t.userMessage : t.mutedForeground}
                  strokeWidth={1.8}
                />
                <Text
                  style={[
                    styles.schemeBtnText,
                    { color: selected ? t.userMessage : t.mutedForeground },
                  ]}
                >
                  {opt.label}
                </Text>
              </Pressable>
            );
          })}
        </View>
      </GlassCard>
    </>
  );
}

export function AboutSection() {
  const { theme } = useThemeContext();
  const t = theme.colors;

  return (
    <>
      <GlassCard compact>
        <View style={styles.row}>
          <Cpu size={20} color={t.mutedForeground} strokeWidth={1.8} />
          <View style={styles.rowContent}>
            <Text style={[styles.rowTitle, { color: t.foreground }]}>Mitsuro</Text>
            <Text style={[styles.rowSubtitle, { color: t.mutedForeground }]}>
              By Honeycomb Technologies
            </Text>
          </View>
        </View>
      </GlassCard>
    </>
  );
}

import { DevicePairingSection } from "./DevicePairingSection";
import { type ReactNode, useCallback, useEffect, useMemo, useState } from "react";
import { Alert, Pressable, ScrollView, Text, View } from "react-native";
import {
	Bell,
	BellOff,
	BellRing,
	ChevronDown,
	ChevronRight,
	Monitor,
	Moon,
	Sun,
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
import { useConnection } from "../../hooks/useConnection";
import { useNotifications } from "../../hooks/useNotifications";
import { useThemeContext } from "../../hooks/useTheme";
import {
	AboutSection,
	AppearanceSection,
	ConnectionSection,
	DiagnosticsSection,
	NotificationsSection,
	PreviewSection,
	ProvidersSection,
	SettingsHeader,
	SkillsSection,
} from "./sections";
import {
	type ActiveOAuthFlow,
	type NotificationOption,
	type PreviewDraftState,
	type ProviderFormState,
	type SchemeOption,
	pollOAuthUntilDone,
	toErrorMessage,
} from "./shared";
import { styles } from "./styles";
import { useMobileDiagnostics } from "../../diagnostics/MobileDiagnosticsProvider";

interface SettingsPanelProps {
	active?: boolean;
	onClose?: () => void;
	showHeader?: boolean;
}

function SettingsDisclosure({
	title,
	summary,
	expanded,
	onPress,
	children,
}: {
	title: string;
	summary?: string;
	expanded: boolean;
	onPress: () => void;
	children: ReactNode;
}) {
	const { theme } = useThemeContext();
	const t = theme.colors;
	return (
		<View style={[styles.disclosure, { borderColor: t.border }]}>
			<Pressable
				accessibilityRole="button"
				accessibilityState={{ expanded }}
				onPress={onPress}
				style={styles.disclosureHeader}
			>
				<Text style={[styles.disclosureTitle, { color: t.foreground }]}>
					{title}
				</Text>
				{summary ? (
					<Text
						numberOfLines={1}
						style={[styles.disclosureSummary, { color: t.mutedForeground }]}
					>
						{summary}
					</Text>
				) : null}
				{expanded ? (
					<ChevronDown size={17} color={t.mutedForeground} />
				) : (
					<ChevronRight size={17} color={t.mutedForeground} />
				)}
			</Pressable>
			{expanded ? (
				<View style={[styles.disclosureBody, { borderTopColor: t.border }]}>
					{children}
				</View>
			) : null}
		</View>
	);
}

function DevelopmentDiagnosticsDisclosure({
	isConnected,
	expanded,
	onPress,
}: {
	isConnected: boolean;
	expanded: boolean;
	onPress: () => void;
}) {
	const diagnostics = useMobileDiagnostics();

	return (
		<SettingsDisclosure
			title="Diagnostics"
			summary={diagnostics.mode === "stress" ? "Recording" : "Idle"}
			expanded={expanded}
			onPress={onPress}
		>
			<DiagnosticsSection
				mode={diagnostics.mode}
				runId={diagnostics.runId}
				eventCount={diagnostics.eventCount}
				nativePayloadCount={diagnostics.nativePayloadCount}
				approximateBytes={diagnostics.approximateBytes}
				uploadState={diagnostics.uploadState}
				completionPending={diagnostics.completionPending}
				isConnected={isConnected}
				onStart={() => diagnostics.startStressRun(10 * 60 * 1000)}
				onStopAndUpload={() => void diagnostics.stopStressRun()}
				onUpload={() => void diagnostics.flush(false)}
			/>
		</SettingsDisclosure>
	);
}

export function SettingsPanel({
	active = true,
	onClose,
	showHeader = true,
}: SettingsPanelProps) {
	const { colorScheme, setColorScheme } = useThemeContext();
	const {
		client,
		isConnected,
		isConfigured,
		serverUrl,
		status,
		error: connectionError,
		connect,
		disconnect,
		reconnect,
	} = useConnection();
	const {
		notificationLevel,
		changeNotificationLevel,
		registrationState,
		lastRegistrationError,
		pendingActionCount,
	} = useNotifications();

	const [inputUrl, setInputUrl] = useState("");
	const [inputToken, setInputToken] = useState("");
	const [isConnecting, setIsConnecting] = useState(false);
	const [connectError, setConnectError] = useState<string | null>(null);

	const [providers, setProviders] = useState<ProviderStatus[]>([]);
	const [providerForms, setProviderForms] = useState<ProviderFormState>({});
	const [providersLoading, setProvidersLoading] = useState(false);
	const [providerBusyKey, setProviderBusyKey] = useState<string | null>(null);
	const [providerMessage, setProviderMessage] = useState<string | null>(null);
	const [activeOAuthFlow, setActiveOAuthFlow] =
		useState<ActiveOAuthFlow | null>(null);
	const [oauthCode, setOauthCode] = useState("");
	const [skills, setSkills] = useState<SkillInfo[]>([]);
	const [skillsLoading, setSkillsLoading] = useState(false);
	const [skillsMessage, setSkillsMessage] = useState<string | null>(null);
	const [skillPageStart, setSkillPageStart] = useState(0);

	const [previewSettings, setPreviewSettings] =
		useState<PreviewSettings | null>(null);
	const [previewPorts, setPreviewPorts] = useState<PortEntry[]>([]);
	const [previewDraft, setPreviewDraft] = useState<PreviewDraftState>({
		autoRefreshSecs: "",
		probeTimeoutMs: "",
	});
	const [previewLoading, setPreviewLoading] = useState(false);
	const [previewBusyKey, setPreviewBusyKey] = useState<string | null>(null);
	const [previewMessage, setPreviewMessage] = useState<string | null>(null);
	const [openSection, setOpenSection] = useState<string | null>("connection");

	const schemeOptions: SchemeOption[] = useMemo(
		() => [
			{ key: "dark", label: "Dark", icon: Moon },
			{ key: "light", label: "Light", icon: Sun },
			{ key: "system", label: "System", icon: Monitor },
		],
		[],
	);

	const notifOptions: NotificationOption[] = useMemo(
		() => [
			{ key: "all", label: "All", icon: BellRing },
			{ key: "important", label: "Important", icon: Bell },
			{ key: "silent", label: "Silent", icon: BellOff },
		],
		[],
	);

	const loadProviders = useCallback(async () => {
		if (!client) {
			setProviders([]);
			return;
		}

		setProvidersLoading(true);
		try {
			const nextProviders = await client.getCredentials();
			setProviders(nextProviders);
			setProviderMessage(null);
		} catch (err) {
			setProviderMessage(
				toErrorMessage(err, "Failed to load provider settings."),
			);
		} finally {
			setProvidersLoading(false);
		}
	}, [client]);

	const loadSkills = useCallback(async () => {
		if (!client) {
			setSkills([]);
			return;
		}

		setSkillsLoading(true);
		try {
			const nextSkills = await client.getSkills();
			setSkills(nextSkills);
			setSkillsMessage(null);
		} catch (err) {
			setSkillsMessage(toErrorMessage(err, "Failed to load skills."));
		} finally {
			setSkillsLoading(false);
		}
	}, [client]);

	const loadPreview = useCallback(async () => {
		if (!client) {
			setPreviewSettings(null);
			setPreviewPorts([]);
			return;
		}

		setPreviewLoading(true);
		try {
			const response = await client.getPorts();
			setPreviewSettings(response.settings);
			setPreviewPorts(response.ports);
			setPreviewDraft({
				autoRefreshSecs: String(response.settings.auto_refresh_secs),
				probeTimeoutMs: String(response.settings.probe_timeout_ms),
			});
			setPreviewMessage(response.discovery_error ?? null);
		} catch (err) {
			setPreviewMessage(
				toErrorMessage(err, "Failed to load preview settings."),
			);
		} finally {
			setPreviewLoading(false);
		}
	}, [client]);

	const refreshOperationalState = useCallback(async () => {
		if (!client || !isConnected) return;
		await Promise.all([
			loadProviders(),
			loadSkills(),
			loadPreview(),
		]);
	}, [
		client,
		isConnected,
		loadPreview,
		loadProviders,
		loadSkills,
	]);

	useEffect(() => {
		if (!active || !client || !isConnected) return;
		void refreshOperationalState();
	}, [active, client, isConnected, refreshOperationalState]);

	const handleConnect = useCallback(async () => {
		if (!inputUrl.trim() || !inputToken.trim()) return;

		setIsConnecting(true);
		setConnectError(null);
		await Haptics.impactAsync(Haptics.ImpactFeedbackStyle.Medium);

		const url = inputUrl.trim().replace(/\/+$/, "");
		const success = await connect(url, inputToken.trim());

		if (success) {
			await Haptics.notificationAsync(Haptics.NotificationFeedbackType.Success);
			setInputUrl("");
			setInputToken("");
		} else {
			await Haptics.notificationAsync(Haptics.NotificationFeedbackType.Error);
			setConnectError("Connection failed. Check URL and token.");
		}

		setIsConnecting(false);
	}, [connect, inputToken, inputUrl]);

	const handleDisconnect = useCallback(() => {
		Alert.alert("Disconnect", "Remove saved server connection?", [
			{ text: "Cancel", style: "cancel" },
			{
				text: "Disconnect",
				style: "destructive",
				onPress: () => {
					void (async () => {
						setIsConnecting(true);
						setConnectError(null);
						void Haptics.impactAsync(Haptics.ImpactFeedbackStyle.Heavy);
						try {
							await disconnect();
						} catch (logoutError) {
							setConnectError(
								toErrorMessage(
									logoutError,
									"Saved server credentials could not be removed. Try Disconnect again.",
								),
							);
							await Haptics.notificationAsync(
								Haptics.NotificationFeedbackType.Error,
							);
						} finally {
							setIsConnecting(false);
						}
					})();
				},
			},
		]);
	}, [disconnect]);

	const updateProviderForm = useCallback(
		(providerId: string, value: string) => {
			setProviderForms((current) => ({ ...current, [providerId]: value }));
		},
		[],
	);

	const handleSaveCredential = useCallback(
		async (providerId: string) => {
			if (!client) return;
			const apiKey = providerForms[providerId]?.trim();
			if (!apiKey) return;

			setProviderBusyKey(`save:${providerId}`);
			try {
				await client.setCredential(providerId, apiKey);
				setProviderMessage(`${providerId} API key saved.`);
				setProviderForms((current) => ({ ...current, [providerId]: "" }));
				await loadProviders();
			} catch (err) {
				setProviderMessage(
					toErrorMessage(err, `Failed to save ${providerId} credential.`),
				);
			} finally {
				setProviderBusyKey(null);
			}
		},
		[client, loadProviders, providerForms],
	);

	const handleDeleteCredential = useCallback(
		async (providerId: string) => {
			if (!client) return;

			setProviderBusyKey(`delete:${providerId}`);
			try {
				await client.deleteCredential(providerId);
				setProviderMessage(`${providerId} API key removed.`);
				await loadProviders();
			} catch (err) {
				setProviderMessage(
					toErrorMessage(err, `Failed to delete ${providerId} credential.`),
				);
			} finally {
				setProviderBusyKey(null);
			}
		},
		[client, loadProviders],
	);

	const handleStartOAuth = useCallback(
		async (providerId: string) => {
			if (!client) return;

			setProviderBusyKey(`oauth:${providerId}`);
			setProviderMessage(null);
			setOauthCode("");

			try {
				const flow = await client.startOAuth(providerId);
				setActiveOAuthFlow({
					provider: flow.provider,
					flowType: flow.flow_type,
					authUrl: flow.auth_url,
					pasteCode: flow.paste_code,
					userCode: flow.device_code?.user_code ?? null,
					verificationUriComplete:
						flow.device_code?.verification_uri_complete ?? null,
				});

				const authUrl =
					flow.device_code?.verification_uri_complete ?? flow.auth_url;
				if (authUrl.trim().length > 0) {
					await openURL(authUrl);
				}

				if (!flow.paste_code) {
					const status = await pollOAuthUntilDone(
						flow.provider,
						client.getOAuthStatus.bind(client),
					);
					if (status.has_token) {
						setProviderMessage(`${flow.provider} OAuth connected.`);
						setActiveOAuthFlow(null);
						await loadProviders();
					} else {
						setProviderMessage(`${flow.provider} OAuth is still pending.`);
					}
				}
			} catch (err) {
				setProviderMessage(
					toErrorMessage(err, `Failed to start ${providerId} OAuth.`),
				);
			} finally {
				setProviderBusyKey(null);
			}
		},
		[client, loadProviders],
	);

	const handleExchangeOAuthCode = useCallback(async () => {
		if (!client || !activeOAuthFlow || !oauthCode.trim()) return;

		setProviderBusyKey(`exchange:${activeOAuthFlow.provider}`);
		try {
			await client.exchangeOAuthCode(
				activeOAuthFlow.provider,
				oauthCode.trim(),
			);
			setProviderMessage(`${activeOAuthFlow.provider} OAuth connected.`);
			setActiveOAuthFlow(null);
			setOauthCode("");
			await loadProviders();
		} catch (err) {
			setProviderMessage(toErrorMessage(err, "Failed to exchange OAuth code."));
		} finally {
			setProviderBusyKey(null);
		}
	}, [activeOAuthFlow, client, loadProviders, oauthCode]);

	const handleRevokeOAuth = useCallback(
		async (providerId: string) => {
			if (!client) return;

			setProviderBusyKey(`revoke:${providerId}`);
			try {
				await client.revokeOAuth(providerId);
				setProviderMessage(`${providerId} OAuth revoked.`);
				if (activeOAuthFlow?.provider === providerId) {
					setActiveOAuthFlow(null);
					setOauthCode("");
				}
				await loadProviders();
			} catch (err) {
				setProviderMessage(
					toErrorMessage(err, `Failed to revoke ${providerId} OAuth.`),
				);
			} finally {
				setProviderBusyKey(null);
			}
		},
		[activeOAuthFlow?.provider, client, loadProviders],
	);

	const handleUpdatePreviewToggle = useCallback(
		async (patch: PreviewSettingsPatch) => {
			if (!client || !previewSettings) return;

			setPreviewBusyKey("toggle");
			try {
				const nextSettings = await client.updatePreviewSettings(patch);
				setPreviewSettings(nextSettings);
				await loadPreview();
			} catch (err) {
				setPreviewMessage(
					toErrorMessage(err, "Failed to update preview settings."),
				);
			} finally {
				setPreviewBusyKey(null);
			}
		},
		[client, loadPreview, previewSettings],
	);

	const handleSavePreviewNumbers = useCallback(async () => {
		if (!client) return;

		const autoRefreshSecs = Number.parseInt(previewDraft.autoRefreshSecs, 10);
		const probeTimeoutMs = Number.parseInt(previewDraft.probeTimeoutMs, 10);
		if (!Number.isFinite(autoRefreshSecs) || !Number.isFinite(probeTimeoutMs)) {
			setPreviewMessage(
				"Auto refresh and probe timeout must be valid numbers.",
			);
			return;
		}

		setPreviewBusyKey("numbers");
		try {
			const nextSettings = await client.updatePreviewSettings({
				auto_refresh_secs: autoRefreshSecs,
				probe_timeout_ms: probeTimeoutMs,
			});
			setPreviewSettings(nextSettings);
			await loadPreview();
			setPreviewMessage(null);
		} catch (err) {
			setPreviewMessage(
				toErrorMessage(err, "Failed to save preview timing settings."),
			);
		} finally {
			setPreviewBusyKey(null);
		}
	}, [
		client,
		loadPreview,
		previewDraft.autoRefreshSecs,
		previewDraft.probeTimeoutMs,
	]);

	const handleTogglePinnedPort = useCallback(
		async (port: PortEntry) => {
			if (!client) return;

			setPreviewBusyKey(`pin:${port.port}`);
			try {
				if (port.pinned) {
					await client.removePinnedPort(port.port);
				} else {
					await client.addPinnedPort(port.port);
				}
				await loadPreview();
			} catch (err) {
				setPreviewMessage(
					toErrorMessage(err, `Failed to update port ${port.port}.`),
				);
			} finally {
				setPreviewBusyKey(null);
			}
		},
		[client, loadPreview],
	);

	const handleHidePort = useCallback(
		async (port: PortEntry) => {
			if (!client) return;

			setPreviewBusyKey(`hide:${port.port}`);
			try {
				await client.addHiddenPort(port.port);
				await loadPreview();
			} catch (err) {
				setPreviewMessage(
					toErrorMessage(err, `Failed to hide port ${port.port}.`),
				);
			} finally {
				setPreviewBusyKey(null);
			}
		},
		[client, loadPreview],
	);

	const providersReady = providers.filter(
		(provider) => provider.configured || provider.has_oauth,
	).length;
	const toggleDisclosure = (section: string) => {
		void Haptics.impactAsync(Haptics.ImpactFeedbackStyle.Light);
		setOpenSection((current) => (current === section ? null : section));
	};

	return (
		<ScrollView
			contentContainerStyle={styles.content}
			keyboardShouldPersistTaps="handled"
			showsVerticalScrollIndicator={false}
		>
			{showHeader ? <SettingsHeader onClose={onClose} /> : null}

			<SettingsDisclosure
				title="Connection"
				summary={isConnected ? "Connected" : "Disconnected"}
				expanded={openSection === "connection"}
				onPress={() => toggleDisclosure("connection")}
			>
				<ConnectionSection
					isConfigured={isConfigured}
					isConnected={isConnected}
					status={status}
					serverUrl={serverUrl}
					connectError={connectError ?? connectionError}
					inputUrl={inputUrl}
					inputToken={inputToken}
					isConnecting={isConnecting}
					onInputUrlChange={setInputUrl}
					onInputTokenChange={setInputToken}
					onConnect={() => void handleConnect()}
					onReconnect={() => void reconnect()}
					onDisconnect={handleDisconnect}
				/>
				{active && openSection === "connection" && <DevicePairingSection />}
			</SettingsDisclosure>

			<SettingsDisclosure
				title="Providers"
				summary={`${providersReady} ready`}
				expanded={openSection === "providers"}
				onPress={() => toggleDisclosure("providers")}
			>
				<ProvidersSection
					isConnected={isConnected}
					providersLoading={providersLoading}
					providers={providers}
					providerForms={providerForms}
					providerBusyKey={providerBusyKey}
					providerMessage={providerMessage}
					activeOAuthFlow={activeOAuthFlow}
					oauthCode={oauthCode}
					onProviderFormChange={updateProviderForm}
					onSaveCredential={(providerId) => void handleSaveCredential(providerId)}
					onDeleteCredential={(providerId) =>
						void handleDeleteCredential(providerId)
					}
					onStartOAuth={(providerId) => void handleStartOAuth(providerId)}
					onExchangeOAuthCode={() => void handleExchangeOAuthCode()}
					onRevokeOAuth={(providerId) => void handleRevokeOAuth(providerId)}
					onOauthCodeChange={setOauthCode}
				/>
			</SettingsDisclosure>

			<SettingsDisclosure
				title="Skills"
				summary={skillsLoading ? "Loading…" : `${skills.length} loaded`}
				expanded={openSection === "skills"}
				onPress={() => toggleDisclosure("skills")}
			>
				<SkillsSection
					isConnected={isConnected}
					loading={skillsLoading}
					skills={skills}
					message={skillsMessage}
					pageStart={skillPageStart}
					onPageStartChange={setSkillPageStart}
				/>
			</SettingsDisclosure>

			<SettingsDisclosure
				title="Preview & ports"
				summary={previewSettings?.enabled ? "On" : "Off"}
				expanded={openSection === "preview"}
				onPress={() => toggleDisclosure("preview")}
			>
				<PreviewSection
					isConnected={isConnected}
					loading={previewLoading}
					previewSettings={previewSettings}
					previewPorts={previewPorts}
					previewDraft={previewDraft}
					busyKey={previewBusyKey}
					message={previewMessage}
					onToggle={(patch) => void handleUpdatePreviewToggle(patch)}
					onSaveNumbers={() => void handleSavePreviewNumbers()}
					onDraftChange={setPreviewDraft}
					onRefresh={() => void loadPreview()}
					onTogglePinnedPort={(port) => void handleTogglePinnedPort(port)}
					onHidePort={(port) => void handleHidePort(port)}
				/>
			</SettingsDisclosure>

			<SettingsDisclosure
				title="Appearance"
				summary={colorScheme}
				expanded={openSection === "appearance"}
				onPress={() => toggleDisclosure("appearance")}
			>
				<AppearanceSection
					colorScheme={colorScheme as ColorScheme}
					schemeOptions={schemeOptions}
					onSelect={setColorScheme}
				/>
			</SettingsDisclosure>

			<SettingsDisclosure
				title="Notifications"
				summary={notificationLevel}
				expanded={openSection === "notifications"}
				onPress={() => toggleDisclosure("notifications")}
			>
				<NotificationsSection
					notificationLevel={notificationLevel}
					registrationState={registrationState}
					lastRegistrationError={lastRegistrationError}
					pendingActionCount={pendingActionCount}
					notifOptions={notifOptions}
					onSelect={(level) => void changeNotificationLevel(level)}
				/>
			</SettingsDisclosure>

			{__DEV__ ? (
				<DevelopmentDiagnosticsDisclosure
					isConnected={isConnected}
					expanded={openSection === "diagnostics"}
					onPress={() => toggleDisclosure("diagnostics")}
				/>
			) : null}

			<SettingsDisclosure
				title="About"
				expanded={openSection === "about"}
				onPress={() => toggleDisclosure("about")}
			>
				<AboutSection />
			</SettingsDisclosure>
		</ScrollView>
	);
}

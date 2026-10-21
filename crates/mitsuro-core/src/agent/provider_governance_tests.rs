use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc as std_mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use anyhow::Result;
use chrono::{TimeZone, Utc};

use crate::ai::client::{AiClient, AiClientConfig, CallOptions, RemoteAttemptPolicy};
use crate::ai::models::{ApiFormat, ModelCatalogSource, ModelKey, ModelMetadata};
use crate::ai::providers::{AuthHeader, ProviderId};
use crate::ai::types::{Content, ModelMessage, Role, Usage};
use crate::storage::{
    BeginWorkerProviderCall, BeginWorkerProviderCallResult, FinishWorkerProviderCall,
    FinishWorkerProviderCallResult, FrozenModelPriceSnapshot, ProviderCallRemoteAcceptance,
    ProviderCallTerminalState, WorkerConversationLane, WorkerGovernorDailyUsage,
    WorkerGovernorDecision, WorkerGovernorDisposition, WorkerGovernorGateReason,
    WorkerGovernorIdleProjection, WorkerProviderCall, WorkerProviderCallOutcome, WorkerRunOrigin,
};
use crate::tools::registry::PermissionMode;
use tiny_http::{Header, Response, Server};

use super::{
    estimate_worker_provider_call_cost, freeze_worker_model_pricing, WorkerProviderAdmission,
    WorkerProviderCallGovernor, WorkerProviderCallKind, WorkerProviderCallSlot,
    WorkerProviderCompletion, WorkerProviderGovernorBinding, WorkerProviderLedger,
    WorkerProviderTerminalOutcome,
};

#[derive(Clone)]
enum FakeBegin {
    Started,
    AlreadyStarted,
    Gated(Box<WorkerGovernorDecision>),
}

struct FakeLedger {
    begin: FakeBegin,
    begins: Mutex<Vec<BeginWorkerProviderCall>>,
    finishes: Mutex<Vec<FinishWorkerProviderCall>>,
}

impl FakeLedger {
    fn new(begin: FakeBegin) -> Arc<Self> {
        Arc::new(Self {
            begin,
            begins: Mutex::new(Vec::new()),
            finishes: Mutex::new(Vec::new()),
        })
    }
}

impl WorkerProviderLedger for FakeLedger {
    fn begin(&self, input: &BeginWorkerProviderCall) -> Result<BeginWorkerProviderCallResult> {
        self.begins.lock().unwrap().push(input.clone());
        Ok(match &self.begin {
            FakeBegin::Started => BeginWorkerProviderCallResult::Started(started_call(input)),
            FakeBegin::AlreadyStarted => {
                BeginWorkerProviderCallResult::AlreadyStarted(started_call(input))
            }
            FakeBegin::Gated(decision) => {
                BeginWorkerProviderCallResult::Gated(decision.as_ref().clone())
            }
        })
    }

    fn finish(&self, input: &FinishWorkerProviderCall) -> Result<FinishWorkerProviderCallResult> {
        self.finishes.lock().unwrap().push(input.clone());
        Ok(FinishWorkerProviderCallResult::Inserted(
            WorkerProviderCallOutcome {
                provider_call_id: input.provider_call_id.clone(),
                state: input.state,
                outcome: input.outcome.clone(),
                remote_acceptance: input.remote_acceptance,
                usage: input.usage.clone(),
                usage_total_tokens: input
                    .usage
                    .as_ref()
                    .map(|usage| usage.logical_total_tokens() as u64),
                estimated_cost_microunits: input.estimated_cost_microunits,
                unknown_reason: input.unknown_reason.clone(),
                finished_at: input.finished_at.to_rfc3339(),
            },
        ))
    }
}

fn binding(lease_token: &str) -> WorkerProviderGovernorBinding {
    WorkerProviderGovernorBinding {
        db_path: "/tmp/mitsuro-provider-governor-test.db".into(),
        worker_id: "worker-1".into(),
        worker_revision: 7,
        owner_user_id: Some("user-1".into()),
        session_id: "session-1".into(),
        conversation_lane: WorkerConversationLane::DirectMessage,
        run_id: "run-1".into(),
        run_lease_token: lease_token.into(),
        run_lease_epoch: 12,
        model_key: ModelKey::new(ProviderId::Grok, "grok-test", ApiFormat::OpenAI),
        model_catalog_revision: Some("catalog-1".into()),
        permission_mode: PermissionMode::Autonomous,
        origin: WorkerRunOrigin::UserDm,
        workflow_goal_id: None,
        workflow_attempt_id: None,
        pricing: None,
        override_grant_id: None,
    }
}

fn now() -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 8, 25, 12, 0, 0)
        .single()
        .unwrap()
}

fn started_call(input: &BeginWorkerProviderCall) -> WorkerProviderCall {
    WorkerProviderCall {
        provider_call_id: input.provider_call_id.clone(),
        worker_id: input.worker_id.clone(),
        worker_revision: input.expected_worker_revision,
        owner_user_id: input.owner_user_id.clone(),
        session_id: input.session_id.clone(),
        group_id: None,
        run_id: input.run_id.clone(),
        run_lease_token: input.run_lease_token.clone(),
        run_lease_epoch: input.run_lease_epoch,
        run_lease_expires_at: "2026-08-25T12:05:00Z".into(),
        workflow_goal_id: input.workflow_goal_id.clone(),
        workflow_attempt_id: input.workflow_attempt_id.clone(),
        origin: input.origin,
        lane_key: input.lane_key.clone(),
        call_kind: input.call_kind.clone(),
        provider_id: input.expected_model_key.provider.storage_key().into(),
        model_id: input.expected_model_key.model_id.clone(),
        model_key_json: serde_json::to_string(&input.expected_model_key).unwrap(),
        model_key_fingerprint: "fingerprint".into(),
        model_catalog_revision: input.expected_model_catalog_revision.clone(),
        permission_mode: input.expected_permission_mode,
        pricing: input.pricing.clone(),
        policy_revision: 3,
        timezone: "UTC".into(),
        local_day: "2026-08-25".into(),
        reserved_tokens: input.reserved_tokens,
        override_grant_id: input.override_grant_id.clone(),
        started_at: input.started_at.to_rfc3339(),
    }
}

fn gated_decision() -> WorkerGovernorDecision {
    WorkerGovernorDecision {
        disposition: WorkerGovernorDisposition::Defer,
        primary_reason: Some(WorkerGovernorGateReason::QuietHours),
        reasons: vec![WorkerGovernorGateReason::QuietHours],
        evaluated_at: now().to_rfc3339(),
        next_eligible_at: Some("2026-08-25T13:00:00Z".into()),
        policy_revision: 3,
        tracking_started_at: "2026-08-01T00:00:00Z".into(),
        daily: WorkerGovernorDailyUsage {
            local_day: "2026-08-25".into(),
            timezone: "UTC".into(),
            starts_at: "2026-08-25T00:00:00Z".into(),
            resets_at: "2026-08-26T00:00:00Z".into(),
            calls_used: 0,
            calls_limit: 128,
            tokens_used_or_reserved: 0,
            tokens_limit: 1_000_000,
        },
        idle: WorkerGovernorIdleProjection {
            lane_key: "dm".into(),
            idle_streak: 0,
            not_before: None,
            last_material_at: None,
            last_outcome_run_id: None,
        },
        override_grant_id: None,
    }
}

#[test]
fn deterministic_ids_are_stable_per_attempt_and_distinct_for_children() {
    let ledger = FakeLedger::new(FakeBegin::Started);
    let governor = WorkerProviderCallGovernor::with_ledger(binding("lease-a"), ledger).unwrap();
    let first =
        WorkerProviderCallSlot::child(WorkerProviderCallKind::DelegatedAgentTurn, 2, 1, "task-a")
            .unwrap();
    let sibling =
        WorkerProviderCallSlot::child(WorkerProviderCallKind::DelegatedAgentTurn, 2, 1, "task-b")
            .unwrap();

    assert_eq!(
        governor.provider_call_id(&first).unwrap(),
        governor.provider_call_id(&first).unwrap()
    );
    assert_ne!(
        governor.provider_call_id(&first).unwrap(),
        governor.provider_call_id(&sibling).unwrap()
    );

    let next_attempt = WorkerProviderCallGovernor::with_ledger(
        binding("lease-b"),
        FakeLedger::new(FakeBegin::Started),
    )
    .unwrap();
    assert_ne!(
        governor.provider_call_id(&first).unwrap(),
        next_attempt.provider_call_id(&first).unwrap()
    );
}

#[test]
fn started_is_persisted_before_the_fake_remote_boundary_and_completion_reuses_id() {
    let ledger = FakeLedger::new(FakeBegin::Started);
    let governor =
        WorkerProviderCallGovernor::with_ledger(binding("lease-a"), ledger.clone()).unwrap();
    let network_calls = AtomicUsize::new(0);
    let admission = governor
        .admit_at(
            WorkerProviderCallSlot::new(WorkerProviderCallKind::AgentTurn, 1, 0),
            2_000,
            now(),
        )
        .unwrap();
    assert_eq!(ledger.begins.lock().unwrap().len(), 1);

    let WorkerProviderAdmission::Allowed(permit) = admission else {
        panic!("expected permit")
    };
    network_calls.fetch_add(1, Ordering::SeqCst);
    permit
        .complete_at(
            WorkerProviderCompletion::acknowledged(WorkerProviderTerminalOutcome::Completed, None),
            now(),
        )
        .unwrap();

    assert_eq!(network_calls.load(Ordering::SeqCst), 1);
    let begins = ledger.begins.lock().unwrap();
    let finishes = ledger.finishes.lock().unwrap();
    assert_eq!(finishes.len(), 1);
    assert_eq!(finishes[0].provider_call_id, begins[0].provider_call_id);
    assert_eq!(finishes[0].state, ProviderCallTerminalState::Completed);
    assert_eq!(
        finishes[0].remote_acceptance,
        ProviderCallRemoteAcceptance::Acknowledged
    );
}

#[tokio::test]
async fn ambiguous_governed_transport_failure_leaves_one_started_slot_without_resend() {
    let ledger = FakeLedger::new(FakeBegin::Started);
    let governor =
        WorkerProviderCallGovernor::with_ledger(binding("lease-ambiguous"), ledger.clone())
            .unwrap();
    let WorkerProviderAdmission::Allowed(permit) = governor
        .admit_at(
            WorkerProviderCallSlot::new(WorkerProviderCallKind::AgentTurn, 1, 0),
            2_000,
            now(),
        )
        .unwrap()
    else {
        panic!("expected provider permit")
    };

    let server = Server::http("127.0.0.1:0").expect("test server should bind");
    let url = format!("http://{}", server.server_addr());
    let (request_tx, request_rx) = std_mpsc::channel();
    let server_thread = thread::spawn(move || {
        let request = server.recv().expect("one request should arrive");
        request_tx.send(()).expect("request should be counted");
        request
            .respond(
                Response::from_string("capacity")
                    .with_status_code(429)
                    .with_header(
                        Header::from_bytes("Retry-After", "0")
                            .expect("retry header should be valid"),
                    ),
            )
            .expect("429 should be sent");

        if let Some(retry) = server
            .recv_timeout(Duration::from_millis(1_500))
            .expect("retry observation should succeed")
        {
            request_tx.send(()).expect("retry should be counted");
            retry
                .respond(Response::from_string(
                    "data: {\"choices\":[{\"delta\":{\"content\":\"unexpected retry\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n",
                ))
                .expect("unexpected retry should be answered");
        }
    });

    let client = AiClient::new(
        AiClientConfig {
            model: "grok-test".to_string(),
            max_tokens: 128,
            base_url: Some(url),
            auth_header: AuthHeader::Bearer,
            provider_id: ProviderId::Grok,
            api_format: ApiFormat::OpenAI,
            custom_headers: Default::default(),
        },
        "test-key".to_string(),
    );
    let messages = vec![ModelMessage {
        role: Role::User,
        content: vec![Content::text("perform one governed attempt".to_string())],
    }];

    client
        .call_streaming_with_attempt_policy(
            messages,
            &CallOptions::default(),
            RemoteAttemptPolicy::GovernedSingleAttempt,
        )
        .await
        .expect_err("first ambiguous failure must surface without resend");
    drop(permit);

    server_thread.join().expect("server thread should finish");
    assert_eq!(ledger.begins.lock().unwrap().len(), 1);
    assert!(ledger.finishes.lock().unwrap().is_empty());
    request_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("one request should be recorded");
    assert!(request_rx.try_recv().is_err());
}

#[test]
fn gate_and_replay_never_grant_a_remote_call() {
    for begin in [
        FakeBegin::Gated(Box::new(gated_decision())),
        FakeBegin::AlreadyStarted,
    ] {
        let ledger = FakeLedger::new(begin);
        let governor =
            WorkerProviderCallGovernor::with_ledger(binding("lease-a"), ledger.clone()).unwrap();
        let network_calls = AtomicUsize::new(0);
        let admission = governor
            .admit_at(
                WorkerProviderCallSlot::new(WorkerProviderCallKind::AgentTurn, 1, 0),
                2_000,
                now(),
            )
            .unwrap();
        if let WorkerProviderAdmission::Allowed(_) = admission {
            network_calls.fetch_add(1, Ordering::SeqCst);
        }
        assert_eq!(network_calls.load(Ordering::SeqCst), 0);
        assert!(ledger.finishes.lock().unwrap().is_empty());
    }
}

#[test]
fn dropping_an_ambiguous_permit_does_not_manufacture_a_terminal_outcome() {
    let ledger = FakeLedger::new(FakeBegin::Started);
    let governor =
        WorkerProviderCallGovernor::with_ledger(binding("lease-a"), ledger.clone()).unwrap();
    let admission = governor
        .admit_at(
            WorkerProviderCallSlot::new(WorkerProviderCallKind::AgentTurn, 1, 0),
            2_000,
            now(),
        )
        .unwrap();
    assert!(matches!(admission, WorkerProviderAdmission::Allowed(_)));
    drop(admission);
    assert_eq!(ledger.begins.lock().unwrap().len(), 1);
    assert!(ledger.finishes.lock().unwrap().is_empty());
}

#[test]
fn exact_catalog_pricing_is_frozen_and_completion_cost_is_derived_centrally() {
    let mut metadata = ModelMetadata::new("grok-test", "Priced Grok", ProviderId::Grok)
        .with_transport(ApiFormat::OpenAI)
        .with_catalog_provenance(ModelCatalogSource::LiveDynamic, Some("catalog-1".into()));
    metadata.input_price = Some(1.25);
    metadata.output_price = Some(2.5);
    let runtime = metadata.resolve_runtime();
    let pricing = freeze_worker_model_pricing(
        &ModelKey::new(ProviderId::Grok, "grok-test", ApiFormat::OpenAI),
        &runtime,
    )
    .unwrap()
    .expect("known prices must freeze");
    assert_eq!(pricing.currency.as_deref(), Some("USD"));
    assert_eq!(pricing.input_microunits_per_million, Some(1_250_000));
    assert_eq!(pricing.output_microunits_per_million, Some(2_500_000));
    assert_eq!(pricing.catalog_source, "live_dynamic");
    assert_eq!(pricing.catalog_revision.as_deref(), Some("catalog-1"));

    let ledger = FakeLedger::new(FakeBegin::Started);
    let mut exact_binding = binding("lease-priced");
    exact_binding.pricing = Some(pricing.clone());
    let governor = WorkerProviderCallGovernor::with_ledger(exact_binding, ledger.clone()).unwrap();
    let WorkerProviderAdmission::Allowed(permit) = governor
        .admit_at(
            WorkerProviderCallSlot::new(WorkerProviderCallKind::AgentTurn, 1, 0),
            2_000_000,
            now(),
        )
        .unwrap()
    else {
        panic!("expected priced permit")
    };
    permit
        .complete_at(
            WorkerProviderCompletion::acknowledged(
                WorkerProviderTerminalOutcome::Completed,
                Some(Usage {
                    prompt_tokens: 1_000_000,
                    completion_tokens: 500_000,
                    reasoning_tokens: 0,
                    total_tokens: 1_500_000,
                    cache_creation_input_tokens: 0,
                    cache_read_input_tokens: 0,
                }),
            ),
            now(),
        )
        .unwrap();

    assert_eq!(ledger.begins.lock().unwrap()[0].pricing, Some(pricing));
    assert_eq!(
        ledger.finishes.lock().unwrap()[0].estimated_cost_microunits,
        Some(2_500_000)
    );
}

#[test]
fn unknown_or_incompletely_classified_usage_remains_unpriced() {
    let runtime = ModelMetadata::new("grok-test", "Unpriced Grok", ProviderId::Grok)
        .with_transport(ApiFormat::OpenAI)
        .with_catalog_provenance(ModelCatalogSource::LiveDynamic, Some("catalog-1".into()))
        .resolve_runtime();
    assert_eq!(
        freeze_worker_model_pricing(
            &ModelKey::new(ProviderId::Grok, "grok-test", ApiFormat::OpenAI),
            &runtime,
        )
        .unwrap(),
        None
    );

    let priced = FrozenModelPriceSnapshot {
        currency: Some("USD".into()),
        input_microunits_per_million: Some(1_000_000),
        output_microunits_per_million: Some(2_000_000),
        cache_creation_microunits_per_million: None,
        cache_read_microunits_per_million: None,
        catalog_source: "live_dynamic".into(),
        catalog_revision: Some("catalog-1".into()),
    };
    let cache_usage = Usage {
        prompt_tokens: 10,
        completion_tokens: 5,
        reasoning_tokens: 0,
        total_tokens: 22,
        cache_creation_input_tokens: 0,
        cache_read_input_tokens: 7,
    };
    assert_eq!(
        estimate_worker_provider_call_cost(Some(&priced), Some(&cache_usage)).unwrap(),
        None
    );

    let unclassified_usage = Usage {
        prompt_tokens: 10,
        completion_tokens: 5,
        reasoning_tokens: 0,
        total_tokens: 20,
        cache_creation_input_tokens: 0,
        cache_read_input_tokens: 0,
    };
    assert_eq!(
        estimate_worker_provider_call_cost(Some(&priced), Some(&unclassified_usage)).unwrap(),
        None
    );
    assert_eq!(
        estimate_worker_provider_call_cost(Some(&priced), None).unwrap(),
        None
    );
}

#[test]
fn pricing_identity_rounding_and_overflow_fail_safely() {
    let mut metadata = ModelMetadata::new("grok-test", "Priced Grok", ProviderId::Grok)
        .with_transport(ApiFormat::OpenAI)
        .with_catalog_provenance(ModelCatalogSource::LiveDynamic, Some("catalog-1".into()));
    metadata.input_price = Some(0.000_000_1);
    metadata.output_price = Some(0.0);
    let runtime = metadata.resolve_runtime();
    let expected_key = ModelKey::new(ProviderId::Grok, "grok-test", ApiFormat::OpenAI);
    let pricing = freeze_worker_model_pricing(&expected_key, &runtime)
        .unwrap()
        .expect("sub-micro pricing must round conservatively");
    assert_eq!(pricing.input_microunits_per_million, Some(1));

    let one_token = Usage {
        prompt_tokens: 1,
        completion_tokens: 0,
        reasoning_tokens: 0,
        total_tokens: 1,
        cache_creation_input_tokens: 0,
        cache_read_input_tokens: 0,
    };
    assert_eq!(
        estimate_worker_provider_call_cost(Some(&pricing), Some(&one_token)).unwrap(),
        Some(1)
    );

    let wrong_key = ModelKey::new(ProviderId::Grok, "other-model", ApiFormat::OpenAI);
    assert!(freeze_worker_model_pricing(&wrong_key, &runtime).is_err());

    let mut refreshed_runtime = runtime.clone();
    refreshed_runtime.catalog_revision = Some("catalog-2".into());
    refreshed_runtime.input_price = Some(3.0);
    let refreshed = freeze_worker_model_pricing(&expected_key, &refreshed_runtime)
        .unwrap()
        .expect("same exact key remains executable after a catalog refresh");
    assert_eq!(refreshed.catalog_revision.as_deref(), Some("catalog-2"));
    assert_eq!(refreshed.input_microunits_per_million, Some(3_000_000));

    let overflow_pricing = FrozenModelPriceSnapshot {
        currency: Some("USD".into()),
        input_microunits_per_million: Some(i64::MAX as u64),
        output_microunits_per_million: Some(0),
        cache_creation_microunits_per_million: None,
        cache_read_microunits_per_million: None,
        catalog_source: "live_dynamic".into(),
        catalog_revision: Some("catalog-1".into()),
    };
    let overflow_usage = Usage {
        prompt_tokens: 1_000_001,
        ..Usage::default()
    };
    assert!(
        estimate_worker_provider_call_cost(Some(&overflow_pricing), Some(&overflow_usage)).is_err()
    );

    let mut invalid_runtime = runtime;
    invalid_runtime.input_price = Some(f64::NAN);
    assert!(freeze_worker_model_pricing(&expected_key, &invalid_runtime).is_err());
}

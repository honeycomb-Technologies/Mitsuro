//! Repeatable provider-free request preparation benchmark. The retained
//! reference is the pre-optimization implementation, so comparisons use the
//! same compiler, process, workload, and resulting metric contract.

use std::hint::black_box;
use std::time::Instant;

use serde_json::json;
use sha2::{Digest, Sha256};

use super::{estimate_tokens, request_component_metrics, RequestComponentMetrics};
use crate::ai::model_profile::{build_system_prompt_sections, SystemPromptSections};
use crate::ai::models::ApiFormat;
use crate::ai::providers::ProviderId;
use crate::ai::types::{AiTool, Citation, Content, ModelMessage, Role};

fn reference_request_component_metrics(
    sections: &SystemPromptSections,
    messages: &[ModelMessage],
    tools: Option<&[AiTool]>,
) -> RequestComponentMetrics {
    let base_bytes = sections.base_prompt.len();
    let identity_bytes = sections.identity_context.len();
    let project_bytes = sections.project_context.len();
    let session_bytes = sections.session_context.len();
    let system_message_count = messages
        .iter()
        .filter(|message| message.role == Role::System)
        .count();
    let history_messages = messages
        .iter()
        .filter(|message| message.role != Role::System)
        .collect::<Vec<_>>();
    let history_bytes = history_messages
        .iter()
        .map(|message| serde_json::to_vec(message).map_or(0, |value| value.len()))
        .sum();

    let mut sorted_tools = tools.unwrap_or_default().iter().collect::<Vec<_>>();
    sorted_tools.sort_by(|a, b| a.name.cmp(&b.name));
    let serialized_tools = sorted_tools
        .iter()
        .map(|tool| serde_json::to_vec(tool).unwrap_or_default())
        .collect::<Vec<_>>();
    let tool_schema_bytes = serialized_tools.iter().map(Vec::len).sum();

    // This hash identifies the stable request contract without exposing its
    // prompt or schemas in logs. Dynamic conversation history and session
    // context are deliberately excluded so callers can detect stable-prefix
    // drift across warm turns.
    let mut hasher = Sha256::new();
    for component in [
        sections.base_prompt.as_bytes(),
        sections.identity_context.as_bytes(),
        sections.project_context.as_bytes(),
    ] {
        hasher.update(component.len().to_le_bytes());
        hasher.update(component);
    }
    for tool in &serialized_tools {
        hasher.update(tool.len().to_le_bytes());
        hasher.update(tool);
    }
    let request_shape_fingerprint = format!("{:x}", hasher.finalize());

    let request_content_bytes = base_bytes
        .saturating_add(identity_bytes)
        .saturating_add(project_bytes)
        .saturating_add(session_bytes)
        .saturating_add(tool_schema_bytes)
        .saturating_add(history_bytes);

    RequestComponentMetrics {
        base_bytes,
        identity_bytes,
        project_bytes,
        session_bytes,
        tool_schema_bytes,
        history_bytes,
        system_message_count,
        history_message_count: history_messages.len(),
        tool_count: sorted_tools.len(),
        request_content_bytes,
        estimated_tokens: estimate_tokens(request_content_bytes),
        request_shape_fingerprint,
    }
}

struct Workload {
    messages: Vec<ModelMessage>,
    tools: Vec<AiTool>,
    sections: SystemPromptSections,
}

fn workload(history_count: usize, revision: usize) -> Workload {
    let mut messages = vec![
        ModelMessage { role: Role::System, content: vec![Content::text(format!(
            "[PROJECT INSTRUCTIONS - AGENTS.md]\n{}", "Preserve module boundaries and verify changes.\n".repeat(160)
        ))] },
        ModelMessage { role: Role::System, content: vec![Content::text(format!(
            "[DURABLE GOAL - request metrics]\nRevision: {revision}\nCurrent step: verify exact output without changing the conversation."
        ))] },
    ];
    for index in 0..history_count {
        let call_id = format!("call-{}", index / 4);
        let message = match index % 4 {
            0 => ModelMessage { role: Role::User, content: vec![Content::text(format!(
                "Inspect component {index}. Preserve Unicode 🦀, quoted \"values\" and paths C:\\example.\n{}",
                "Check the implementation and report verified evidence.\n".repeat(20)
            ))] },
            1 => ModelMessage { role: Role::Assistant, content: vec![
                Content::Thinking { thinking: "Compare the current interfaces and evidence. ".repeat(16), signature: "synthetic-signature".into() },
                Content::Text { text: "The relevant subsystem is isolated. ".repeat(16), citations: vec![Citation {
                    url: "https://example.com/docs".into(), title: "Synthetic source".into(), cited_text: "The relevant subsystem is isolated.".into(),
                }] },
                Content::ToolUse { id: call_id, name: "read".into(), input: json!({"file_path":format!("src/component_{index}.rs"),"offset":1,"limit":100}) },
            ] },
            2 => ModelMessage { role: Role::User, content: vec![Content::ToolResult {
                tool_use_id: call_id, output: json!({"ok":true,"data":{"path":format!("src/component_{index}.rs"),"content":"fn sample() { let quoted = \"value\"; /* 🦀 */ }\n".repeat(160)},"warnings":[],"metadata":{"truncated":false}}), is_error: None,
            }] },
            _ => ModelMessage { role: Role::Assistant, content: vec![Content::text(
                "The implementation preserves the required contract; the focused check passed.\n".repeat(16)
            )] },
        };
        messages.push(message);
    }
    let tools = ["workflow_read", "workflow_update", "task_complete", "task_start", "tool_search", "read", "apply_patch", "bash"]
        .into_iter().map(|name| AiTool {
            name: name.into(), description: format!("{name}: governed operation with a precise bounded result."),
            input_schema: json!({"type":"object","properties":{"path":{"type":"string"},"limit":{"type":"integer","minimum":1},"options":{"type":"object","properties":{"validate":{"type":"boolean"}}}},"additionalProperties":false}), prompt: None,
        }).collect();
    let sections = build_system_prompt_sections(
        ProviderId::OpenAI,
        ApiFormat::OpenAIResponses,
        "gpt-5.5",
        &messages,
        None,
        &[],
    );
    Workload {
        messages,
        tools,
        sections,
    }
}

#[test]
fn request_metrics_match_reference_for_representative_histories() {
    for history_count in [10, 100, 1_000] {
        for revision in [1, 10_000] {
            let workload = workload(history_count, revision);
            let expected = reference_request_component_metrics(
                &workload.sections,
                &workload.messages,
                Some(&workload.tools),
            );
            let actual = request_component_metrics(
                &workload.sections,
                &workload.messages,
                Some(&workload.tools),
            );
            assert_eq!(actual, expected);
            assert_eq!(actual.history_message_count, history_count);
        }
    }
}

type MetricsFunction =
    fn(&SystemPromptSections, &[ModelMessage], Option<&[AiTool]>) -> RequestComponentMetrics;

fn timed(function: MetricsFunction, workload: &Workload) -> (u128, RequestComponentMetrics) {
    let start = Instant::now();
    let metrics = black_box(function(
        black_box(&workload.sections),
        black_box(&workload.messages),
        Some(black_box(&workload.tools)),
    ));
    (start.elapsed().as_nanos(), metrics)
}

fn median(samples: &mut [u128]) -> u128 {
    samples.sort_unstable();
    samples[samples.len() / 2]
}

/// Run with the actual optimized core build; never compare Debug to Release.
/// "first_call" is the first call after fixture construction, not a claim
/// about cold filesystem/CPU caches. Warm samples reuse the exact fixture.
#[test]
#[ignore = "manual Release performance measurement; no provider or filesystem I/O"]
fn benchmark_request_component_metrics() {
    assert!(
        !cfg!(debug_assertions),
        "run this measurement with cargo test --release"
    );
    const SAMPLES: usize = 51;
    println!(
        "{}",
        json!({"benchmark":"request_component_metrics","workload_version":1,"build":"release","warm_samples":SAMPLES,"first_call_definition":"first invocation after fixture allocation; not cold filesystem or CPU cache","scope":"metrics calculation only; fixture and prompt assembly are outside timing"})
    );
    for history_count in [10, 100, 1_000] {
        for revision in [1, 10_000] {
            let workload = workload(history_count, revision);
            let (first_current, actual) = timed(request_component_metrics, &workload);
            let (first_reference, expected) = timed(reference_request_component_metrics, &workload);
            assert_eq!(actual, expected);
            let fixture_hash = format!(
                "{:x}",
                Sha256::digest(serde_json::to_vec(&workload.messages).unwrap())
            );
            for _ in 0..5 {
                black_box(timed(request_component_metrics, &workload));
                black_box(timed(reference_request_component_metrics, &workload));
            }
            let mut current = Vec::with_capacity(SAMPLES);
            let mut reference = Vec::with_capacity(SAMPLES);
            for sample in 0..SAMPLES {
                // Alternate call order to reduce systematic warm-cache bias.
                let (current_sample, reference_sample) = if sample % 2 == 0 {
                    (
                        timed(request_component_metrics, &workload),
                        timed(reference_request_component_metrics, &workload),
                    )
                } else {
                    let reference = timed(reference_request_component_metrics, &workload);
                    (timed(request_component_metrics, &workload), reference)
                };
                assert_eq!(current_sample.1, reference_sample.1);
                current.push(current_sample.0);
                reference.push(reference_sample.0);
            }
            let current_median = median(&mut current);
            let reference_median = median(&mut reference);
            println!(
                "{}",
                json!({
                    "benchmark":"request_component_metrics", "workload_version":1,
                    "history_messages":history_count, "session_revision":revision, "fixture_sha256":fixture_hash,
                    "history_bytes":actual.history_bytes, "request_content_bytes":actual.request_content_bytes,
                    "request_shape_fingerprint":actual.request_shape_fingerprint, "metrics_equal":true,
                    "first_call_current_ns":first_current, "first_call_reference_ns":first_reference,
                    "warm_current_median_ns":current_median, "warm_reference_median_ns":reference_median,
                    "warm_current_p90_ns":current[(SAMPLES * 9) / 10], "warm_reference_p90_ns":reference[(SAMPLES * 9) / 10]
                })
            );
        }
    }
}

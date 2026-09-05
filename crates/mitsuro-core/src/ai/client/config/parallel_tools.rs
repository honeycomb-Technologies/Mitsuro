use serde_json::Value;

use super::CallOptions;

/// The request transport, rather than the model slug, owns legacy defaults.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ParallelToolCallTransport {
    Responses,
    ChatGptCodex,
    Uncontrolled,
}

/// Resolve the exact wire control without changing either transport's default.
/// `has_tools_field` follows request shape: regular Responses omits this control
/// when no tool array is present, while Codex has always sent it explicitly.
pub(crate) fn resolve_parallel_tool_calls(
    options: Option<&CallOptions>,
    transport: ParallelToolCallTransport,
    has_tools_field: bool,
) -> Option<bool> {
    let requested = options.and_then(|options| options.parallel_tool_calls);
    match transport {
        ParallelToolCallTransport::Responses if has_tools_field => Some(requested.unwrap_or(true)),
        ParallelToolCallTransport::ChatGptCodex => {
            Some(requested.unwrap_or_else(|| {
                options.is_some_and(|options| options.codex_parallel_tool_calls)
            }))
        }
        _ => None,
    }
}

/// Apply shared policy after assembling tools, including provider-hosted tools.
pub(crate) fn apply_parallel_tool_call_policy(
    body: &mut Value,
    options: Option<&CallOptions>,
    transport: ParallelToolCallTransport,
) {
    if let Some(enabled) = resolve_parallel_tool_calls(
        options,
        transport,
        body.get("tools").is_some_and(Value::is_array),
    ) {
        body["parallel_tool_calls"] = Value::Bool(enabled);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parallel_policy_preserves_transport_defaults_and_explicit_overrides() {
        for legacy in [false, true] {
            for requested in [None, Some(false), Some(true)] {
                let options = CallOptions {
                    parallel_tool_calls: requested,
                    codex_parallel_tool_calls: legacy,
                    ..Default::default()
                };
                for (transport, expected) in [
                    (
                        ParallelToolCallTransport::Responses,
                        Some(requested.unwrap_or(true)),
                    ),
                    (
                        ParallelToolCallTransport::ChatGptCodex,
                        Some(requested.unwrap_or(legacy)),
                    ),
                    (ParallelToolCallTransport::Uncontrolled, None),
                ] {
                    let mut body = json!({"tools": [{"type": "function", "name": "read"}]});
                    apply_parallel_tool_call_policy(&mut body, Some(&options), transport);
                    assert_eq!(
                        body.get("parallel_tool_calls").and_then(Value::as_bool),
                        expected
                    );
                }
            }
        }
    }

    #[test]
    fn parallel_policy_preserves_tool_free_and_hosted_tool_shapes() {
        let mut tool_free = json!({"model": "example"});
        apply_parallel_tool_call_policy(&mut tool_free, None, ParallelToolCallTransport::Responses);
        assert!(tool_free.get("parallel_tool_calls").is_none());

        let mut hosted = json!({"tools": [{"type": "web_search"}]});
        apply_parallel_tool_call_policy(
            &mut hosted,
            Some(&CallOptions {
                parallel_tool_calls: Some(false),
                ..Default::default()
            }),
            ParallelToolCallTransport::Responses,
        );
        assert_eq!(hosted["parallel_tool_calls"], false);

        apply_parallel_tool_call_policy(
            &mut tool_free,
            None,
            ParallelToolCallTransport::ChatGptCodex,
        );
        assert_eq!(tool_free["parallel_tool_calls"], false);
    }
}

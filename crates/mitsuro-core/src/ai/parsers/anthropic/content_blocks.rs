use serde_json::Value;

use super::AnthropicParser;
use crate::ai::sse::{ServerToolAccumulator, SseEvent, ThinkingAccumulator, ToolCallAccumulator};

impl AnthropicParser {
    pub(super) fn parse_content_block_start(&self, json: &Value) -> anyhow::Result<SseEvent> {
        let index = json.get("index").and_then(|i| i.as_u64()).unwrap_or(0) as usize;

        if let Some(content_block) = json.get("content_block") {
            let block_type = content_block.get("type").and_then(|t| t.as_str());

            match block_type {
                Some("tool_use") => {
                    let id = content_block
                        .get("id")
                        .and_then(|i| i.as_str())
                        .unwrap_or("")
                        .to_string();
                    let name = content_block
                        .get("name")
                        .and_then(|n| n.as_str())
                        .unwrap_or("")
                        .to_string();

                    let mut accumulators = self.lock_tool_accumulators()?;
                    accumulators.insert(index, ToolCallAccumulator::new(id.clone(), name.clone()));

                    return Ok(SseEvent::ToolCallStart { id, name });
                }
                Some("server_tool_use") => {
                    let id = content_block
                        .get("id")
                        .and_then(|i| i.as_str())
                        .unwrap_or("")
                        .to_string();
                    let name = content_block
                        .get("name")
                        .and_then(|n| n.as_str())
                        .unwrap_or("")
                        .to_string();

                    let mut accumulators = self.lock_server_tool_accumulators()?;
                    accumulators
                        .insert(index, ServerToolAccumulator::new(id.clone(), name.clone()));

                    return Ok(SseEvent::ServerToolStart { id, name });
                }
                Some("web_search_tool_result") => {
                    let tool_use_id = content_block
                        .get("tool_use_id")
                        .and_then(|i| i.as_str())
                        .unwrap_or("")
                        .to_string();

                    let results = self.parse_search_results(content_block);
                    return Ok(SseEvent::WebSearchResults {
                        tool_use_id,
                        results,
                    });
                }
                Some("web_fetch_tool_result") => {
                    let tool_use_id = content_block
                        .get("tool_use_id")
                        .and_then(|i| i.as_str())
                        .unwrap_or("")
                        .to_string();

                    if let Some(content) = self.parse_fetch_result(content_block) {
                        return Ok(SseEvent::WebFetchResult {
                            tool_use_id,
                            content,
                        });
                    }

                    if let Some(err_content) = content_block.get("content") {
                        if let Some(err_type) = err_content.get("type").and_then(|t| t.as_str()) {
                            if err_type == "web_fetch_tool_error"
                                || err_type == "web_search_tool_result_error"
                            {
                                let error_code = err_content
                                    .get("error_code")
                                    .and_then(|e| e.as_str())
                                    .unwrap_or("unknown")
                                    .to_string();
                                return Ok(SseEvent::ServerToolError {
                                    tool_use_id,
                                    error_code,
                                });
                            }
                        }
                    }
                }
                Some("thinking") => {
                    let mut accumulators = self.lock_thinking_accumulators()?;
                    accumulators.insert(index, ThinkingAccumulator::new());
                    return Ok(SseEvent::ThinkingStart { index });
                }
                _ => {}
            }
        }
        Ok(SseEvent::Skip)
    }

    pub(super) fn parse_content_block_delta(&self, json: &Value) -> anyhow::Result<SseEvent> {
        let index = json.get("index").and_then(|i| i.as_u64()).unwrap_or(0) as usize;

        if let Some(delta) = json.get("delta") {
            let delta_type = delta.get("type").and_then(|t| t.as_str());

            match delta_type {
                Some("citations_delta") => {
                    let citations = delta
                        .get("citation")
                        .map(|citation| self.parse_citations(std::slice::from_ref(citation)))
                        .unwrap_or_default();
                    return Ok(if citations.is_empty() {
                        SseEvent::Skip
                    } else {
                        SseEvent::TextDeltaWithCitations {
                            text: String::new(),
                            citations,
                        }
                    });
                }
                Some("text_delta") => {
                    let text = delta
                        .get("text")
                        .and_then(|t| t.as_str())
                        .unwrap_or("")
                        .to_string();

                    if let Some(citations_arr) = delta.get("citations").and_then(|c| c.as_array()) {
                        let citations = self.parse_citations(citations_arr);
                        if !citations.is_empty() {
                            return Ok(SseEvent::TextDeltaWithCitations { text, citations });
                        }
                    }
                    return Ok(SseEvent::TextDelta(text));
                }
                Some("input_json_delta") => {
                    let partial_json = delta
                        .get("partial_json")
                        .and_then(|p| p.as_str())
                        .unwrap_or("")
                        .to_string();

                    {
                        let mut accumulators = self.lock_server_tool_accumulators()?;
                        if let Some(acc) = accumulators.get_mut(&index) {
                            acc.add_input(&partial_json);
                            return Ok(SseEvent::ServerToolDelta {
                                id: acc.id.clone(),
                                delta: partial_json,
                            });
                        }
                    }

                    let mut accumulators = self.lock_tool_accumulators()?;
                    if let Some(acc) = accumulators.get_mut(&index) {
                        acc.add_arguments(&partial_json);
                        return Ok(SseEvent::ToolCallDelta {
                            id: acc.id.clone(),
                            delta: partial_json,
                        });
                    }
                }
                Some("thinking_delta") => {
                    let thinking = delta
                        .get("thinking")
                        .and_then(|t| t.as_str())
                        .unwrap_or("")
                        .to_string();

                    let mut accumulators = self.lock_thinking_accumulators()?;
                    if let Some(acc) = accumulators.get_mut(&index) {
                        acc.add_thinking(&thinking);
                    }
                    return Ok(SseEvent::ThinkingDelta { index, thinking });
                }
                Some("signature_delta") => {
                    let signature = delta
                        .get("signature")
                        .and_then(|s| s.as_str())
                        .unwrap_or("")
                        .to_string();

                    let mut accumulators = self.lock_thinking_accumulators()?;
                    if let Some(acc) = accumulators.get_mut(&index) {
                        acc.add_signature(&signature);
                    }
                    return Ok(SseEvent::SignatureDelta { index, signature });
                }
                _ => {}
            }
        }
        Ok(SseEvent::Skip)
    }

    pub(super) fn parse_content_block_stop(&self, json: &Value) -> anyhow::Result<SseEvent> {
        let index = json.get("index").and_then(|i| i.as_u64()).unwrap_or(0) as usize;

        {
            let mut accumulators = self.lock_server_tool_accumulators()?;
            if let Some(mut acc) = accumulators.remove(&index) {
                let input = acc.complete();
                return Ok(SseEvent::ServerToolComplete {
                    id: acc.id,
                    name: acc.name,
                    input,
                });
            }
        }

        {
            let mut accumulators = self.lock_tool_accumulators()?;
            if let Some(mut acc) = accumulators.remove(&index) {
                if let Some(tool_call) = acc.try_complete() {
                    return Ok(SseEvent::ToolCallComplete(tool_call));
                } else {
                    return Ok(SseEvent::ToolCallComplete(acc.force_complete()));
                }
            }
        }

        {
            let mut accumulators = self.lock_thinking_accumulators()?;
            if let Some(mut acc) = accumulators.remove(&index) {
                let (thinking, signature) = acc.complete();
                return Ok(SseEvent::ThinkingComplete {
                    index,
                    thinking,
                    signature,
                });
            }
        }

        Ok(SseEvent::Skip)
    }
}

#[cfg(test)]
mod citation_tests {
    use super::*;
    use crate::ai::sse::SseParser;
    use serde_json::json;

    #[tokio::test]
    async fn separate_citation_delta_preserves_annotation_without_inventing_text() {
        let event = AnthropicParser::new().parse_event(&json!({
            "type": "content_block_delta", "index": 1,
            "delta": {"type": "citations_delta", "citation": {
                "type": "web_search_result_location", "url": "https://example.org/source",
                "title": "Source", "cited_text": "A cited fact", "encrypted_index": "opaque-provider-reference"
            }}
        })).await.unwrap();
        match event {
            SseEvent::TextDeltaWithCitations { text, citations } => {
                assert!(text.is_empty());
                assert_eq!(citations.len(), 1);
                assert_eq!(citations[0].url, "https://example.org/source");
                assert_eq!(citations[0].cited_text, "A cited fact");
            }
            _ => panic!("expected a normalized citation event"),
        }
    }
}

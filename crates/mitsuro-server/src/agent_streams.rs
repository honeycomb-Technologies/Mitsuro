//! Bounded server-owned live/replay streams for interactive Code Agents.

use std::collections::{HashMap, VecDeque};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use serde::Serialize;
use tokio::sync::broadcast;

use crate::types::AgenticEvent;

const LIVE_CHANNEL_CAPACITY: usize = 256;
const REPLAY_CAPACITY: usize = 512;
const MAX_SESSION_STREAMS: usize = 4_096;

#[derive(Clone, Debug, Serialize)]
pub(crate) struct AgentStreamEnvelope {
    pub generation: u64,
    pub sequence: u64,
    pub event: AgenticEvent,
}

pub(crate) struct AgentStreamSubscription {
    pub replay: Vec<AgentStreamEnvelope>,
    pub receiver: broadcast::Receiver<AgentStreamEnvelope>,
}

struct SessionAgentStream {
    generation: u64,
    sequence: u64,
    replay: VecDeque<AgentStreamEnvelope>,
    sender: broadcast::Sender<AgentStreamEnvelope>,
    last_activity: Instant,
}

impl SessionAgentStream {
    fn new() -> Self {
        let (sender, _) = broadcast::channel(LIVE_CHANNEL_CAPACITY);
        Self {
            generation: 0,
            sequence: 0,
            replay: VecDeque::with_capacity(REPLAY_CAPACITY),
            sender,
            last_activity: Instant::now(),
        }
    }
}

#[derive(Default)]
pub(crate) struct AgentStreamHub {
    sessions: HashMap<String, SessionAgentStream>,
}

impl AgentStreamHub {
    pub(crate) fn cursor(&self, session_id: &str) -> (u64, u64) {
        self.sessions
            .get(session_id)
            .map(|stream| (stream.generation, stream.sequence))
            .unwrap_or_default()
    }

    pub(crate) fn subscribe(
        &mut self,
        session_id: &str,
        after_generation: u64,
        after_sequence: u64,
    ) -> AgentStreamSubscription {
        let stream = self.ensure_session(session_id);
        stream.last_activity = Instant::now();
        // Subscribe while holding the hub lock, before cloning replay. A
        // publisher therefore cannot land in the gap between the two.
        let receiver = stream.sender.subscribe();
        let mut replay: Vec<AgentStreamEnvelope> = if after_generation == stream.generation {
            stream
                .replay
                .iter()
                .filter(|event| event.sequence > after_sequence)
                .cloned()
                .collect()
        } else if after_generation < stream.generation {
            stream.replay.iter().cloned().collect()
        } else {
            Vec::new()
        };
        let expected_sequence = if after_generation == stream.generation {
            after_sequence.saturating_add(1)
        } else {
            1
        };
        if let Some(first_sequence) = replay.first().map(|event| event.sequence) {
            if first_sequence > expected_sequence {
                let skipped = first_sequence.saturating_sub(expected_sequence);
                replay.insert(
                    0,
                    AgentStreamEnvelope {
                        generation: stream.generation,
                        sequence: first_sequence.saturating_sub(1),
                        event: AgenticEvent::Lagged {
                            skipped: skipped.try_into().unwrap_or(usize::MAX),
                        },
                    },
                );
            }
        }
        AgentStreamSubscription { replay, receiver }
    }

    pub(crate) fn begin_generation(&mut self, session_id: &str) -> u64 {
        let stream = self.ensure_session(session_id);
        let wall_clock_generation = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX);
        stream.generation = stream
            .generation
            .saturating_add(1)
            .max(wall_clock_generation)
            .max(1);
        stream.sequence = 0;
        stream.replay.clear();
        stream.last_activity = Instant::now();
        stream.generation
    }

    pub(crate) fn publish(&mut self, session_id: &str, generation: u64, event: AgenticEvent) {
        let Some(stream) = self.sessions.get_mut(session_id) else {
            return;
        };
        if stream.generation != generation {
            return;
        }
        stream.sequence = stream.sequence.saturating_add(1);
        stream.last_activity = Instant::now();
        let envelope = AgentStreamEnvelope {
            generation,
            sequence: stream.sequence,
            event,
        };
        stream.replay.push_back(envelope.clone());
        while stream.replay.len() > REPLAY_CAPACITY {
            stream.replay.pop_front();
        }
        // No live receivers is normal; replay remains authoritative for the
        // next attaching surface.
        let _ = stream.sender.send(envelope);
    }

    fn ensure_session(&mut self, session_id: &str) -> &mut SessionAgentStream {
        if !self.sessions.contains_key(session_id) && self.sessions.len() >= MAX_SESSION_STREAMS {
            let eviction = self
                .sessions
                .iter()
                .filter(|(_, stream)| stream.sender.receiver_count() == 0)
                .min_by_key(|(_, stream)| stream.last_activity)
                .map(|(id, _)| id.clone())
                .or_else(|| {
                    self.sessions
                        .iter()
                        .min_by_key(|(_, stream)| stream.last_activity)
                        .map(|(id, _)| id.clone())
                });
            if let Some(eviction) = eviction {
                self.sessions.remove(&eviction);
            }
        }
        self.sessions
            .entry(session_id.to_owned())
            .or_insert_with(SessionAgentStream::new)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn two_clients_observe_delta_approval_and_finish_exactly_once() {
        let mut hub = AgentStreamHub::default();
        let mut first = hub.subscribe("code-1", 0, 0);
        let mut second = hub.subscribe("code-1", 0, 0);
        let generation = hub.begin_generation("code-1");
        hub.publish(
            "code-1",
            generation,
            AgenticEvent::TextDelta { delta: "hi".into() },
        );
        hub.publish(
            "code-1",
            generation,
            AgenticEvent::ToolApprovalRequired {
                id: "tool-1".into(),
                name: "bash".into(),
                arguments: serde_json::json!({"command": "true"}),
            },
        );
        hub.publish(
            "code-1",
            generation,
            AgenticEvent::Finish {
                session_id: "code-1".into(),
                stop_reason: "complete".into(),
            },
        );

        for client in [&mut first, &mut second] {
            let events = (0..3)
                .map(|_| client.receiver.try_recv().expect("live event"))
                .collect::<Vec<_>>();
            assert_eq!(
                events
                    .iter()
                    .map(|event| event.sequence)
                    .collect::<Vec<_>>(),
                vec![1, 2, 3]
            );
            assert!(matches!(events[0].event, AgenticEvent::TextDelta { .. }));
            assert!(matches!(
                events[1].event,
                AgenticEvent::ToolApprovalRequired { .. }
            ));
            assert!(matches!(events[2].event, AgenticEvent::Finish { .. }));
            assert!(client.receiver.try_recv().is_err());
        }

        let replay = hub.subscribe("code-1", generation, 1).replay;
        assert_eq!(
            replay
                .iter()
                .map(|event| event.sequence)
                .collect::<Vec<_>>(),
            vec![2, 3]
        );
    }

    #[test]
    fn replay_overflow_is_explicitly_signaled_before_the_contiguous_tail() {
        let mut hub = AgentStreamHub::default();
        let generation = hub.begin_generation("code-1");
        for index in 0..(REPLAY_CAPACITY + 2) {
            hub.publish(
                "code-1",
                generation,
                AgenticEvent::TextDelta {
                    delta: index.to_string(),
                },
            );
        }
        let replay = hub.subscribe("code-1", generation, 0).replay;
        assert_eq!(replay.len(), REPLAY_CAPACITY + 1);
        assert!(matches!(
            replay[0].event,
            AgenticEvent::Lagged { skipped: 2 }
        ));
        assert_eq!(replay[0].sequence, 2);
        assert_eq!(replay[1].sequence, 3);
    }
}

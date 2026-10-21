//! Manual Release workload through the production PreviewApp event handler and
//! renderer. The terminal backend is in memory; OS/compositor/TTY write latency
//! is deliberately outside this measurement.

use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use crossterm::event::KeyEvent;
use mitsuro_core::agent::loop_events::LoopStopReason;
use ratatui::{backend::TestBackend, Terminal};
use serde_json::json;

use super::*;
use crate::tui_v2::{motion::preference::MotionPreference, test_support::BufferSnapshot};

const INPUT_INTERVAL: Duration = Duration::from_millis(2);
const MAX_INPUT_EVENTS: usize = 128;
const MILESTONE_INPUT_EVENTS: usize = 32;

#[derive(Clone, Copy)]
enum DrawPolicy {
    OneEvent,
    Batched,
}

impl DrawPolicy {
    fn name(self) -> &'static str {
        match self {
            Self::OneEvent => "one_event_per_draw",
            Self::Batched => "bounded_batch",
        }
    }
}

#[derive(Clone, Copy)]
enum InputSource {
    Timed,
    Milestone,
}

fn input_sample(source: InputSource) -> (Instant, Event, InputSource) {
    (
        Instant::now(),
        Event::Key(KeyEvent::new(KeyCode::Char('x'), KeyModifiers::NONE)),
        source,
    )
}

fn mixed_events(count: usize) -> Vec<LoopEvent> {
    (0..count)
        .map(|index| {
            let turn = index / 100;
            let id = format!("burst-tool-{turn}");
            if index == count - 1 {
                return LoopEvent::Finished {
                    session_id: "preview-session".to_owned(),
                    stop_reason: LoopStopReason::Completed,
                };
            }
            match index % 100 {
                0 => LoopEvent::ThinkingDelta {
                    thinking: "Inspecting the fixture.".to_owned(),
                },
                1 => LoopEvent::ThinkingComplete {
                    thinking: "Inspecting the fixture.".to_owned(),
                    signature: String::new(),
                },
                2 => LoopEvent::TextDelta {
                    delta: format!("Checking turn {turn}. "),
                },
                3 => LoopEvent::ToolCallStart {
                    id,
                    name: "read".to_owned(),
                },
                4 => LoopEvent::ToolCallComplete {
                    id,
                    name: "read".to_owned(),
                    arguments: json!({ "path": "src/fixture.rs" }),
                },
                5 => LoopEvent::ToolExecuting {
                    id,
                    name: "read".to_owned(),
                },
                6..=9 => LoopEvent::ToolOutputDelta {
                    id,
                    delta: "progress\n".to_owned(),
                },
                10 => LoopEvent::ToolResult {
                    id,
                    output: "fixture ready".to_owned(),
                    is_error: false,
                },
                98 => LoopEvent::TextDelta {
                    delta: format!("Turn {turn} complete."),
                },
                99 => LoopEvent::TurnComplete {
                    turn,
                    has_more: true,
                },
                _ => LoopEvent::TextDelta {
                    delta: "word ".to_owned(),
                },
            }
        })
        .collect()
}

fn fixture_app() -> PreviewApp {
    let mut app = PreviewApp::preview();
    app.state.capability = CapabilityProfile::from_environment(
        Some("xterm-256color"),
        Some("truecolor"),
        false,
        false,
    );
    app.state.route = AppRoute::Conversation {
        session_id: SessionId::from_canonical("preview-session"),
    };
    app.state.focus = FocusTarget::Composer;
    app.state.appearance.motion.preference = MotionPreference::Off;
    app.state.agent_run = state::AgentRunState::Running;
    app.conversation.push_user_prompt(
        "burst-user",
        "Inspect this fixture.".to_owned(),
        Vec::new(),
        false,
    );
    app
}

fn percentile(samples: &[Duration], percentage: usize) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let rank = (percentage * sorted.len()).div_ceil(100).saturating_sub(1);
    sorted[rank.min(sorted.len() - 1)].as_secs_f64() * 1_000.0
}

#[test]
#[ignore = "manual Release throughput/input baseline; run with --release --ignored --nocapture"]
fn release_stream_burst_baseline() {
    assert!(
        !cfg!(debug_assertions),
        "this measurement requires a Release build"
    );
    for count in [100, 1_000, 10_000] {
        for sample in 0..3 {
            measure_burst(count, sample, DrawPolicy::OneEvent);
        }
    }
}

#[test]
#[ignore = "manual Release paired comparison; run with --release --ignored --nocapture"]
fn release_stream_burst_comparison() {
    assert!(
        !cfg!(debug_assertions),
        "this measurement requires a Release build"
    );
    for count in [100, 1_000, 10_000] {
        for sample in 0..3 {
            let (baseline, batched) = if sample % 2 == 0 {
                (
                    measure_burst(count, sample, DrawPolicy::OneEvent),
                    measure_burst(count, sample, DrawPolicy::Batched),
                )
            } else {
                let batched = measure_burst(count, sample, DrawPolicy::Batched);
                (measure_burst(count, sample, DrawPolicy::OneEvent), batched)
            };
            assert_eq!(baseline, batched, "batching must preserve the final frame");
        }
    }
}

fn measure_burst(count: usize, sample: usize, policy: DrawPolicy) -> BufferSnapshot {
    let mut app = fixture_app();
    let empty_composer = app.state.composer.clone();
    let mut terminal = Terminal::new(TestBackend::new(120, 36)).expect("test terminal");
    terminal
        .draw(|frame| app.render(frame))
        .expect("warm render");
    let (event_tx, event_rx) = mpsc::unbounded_channel();
    app.loop_events = Some(event_rx);
    let mut event_enqueued_at = VecDeque::with_capacity(count);
    for event in mixed_events(count) {
        event_enqueued_at.push_back(Instant::now());
        event_tx.send(event).expect("enqueue burst");
    }
    drop(event_tx);

    // Match the production biased-select order for these two always-ready
    // sources. Real key events arrive independently while the event burst drains.
    let (input_tx, mut input_rx) = mpsc::unbounded_channel();
    let stop_input = Arc::new(AtomicBool::new(false));
    let processed_count = Arc::new(AtomicUsize::new(0));
    let producer_stop = Arc::clone(&stop_input);
    let timed_input_tx = input_tx.clone();
    let input_producer = std::thread::spawn(move || {
        for _ in 0..MAX_INPUT_EVENTS {
            if producer_stop.load(Ordering::Acquire) {
                break;
            }
            if timed_input_tx
                .send(input_sample(InputSource::Timed))
                .is_err()
            {
                break;
            }
            std::thread::sleep(INPUT_INTERVAL);
        }
    });
    // Sparse independent input spans the entire backlog, including the larger
    // transcript near the end after the initial timed-input sample has ended.
    let milestone_progress = Arc::clone(&processed_count);
    let milestone_producer = std::thread::spawn(move || {
        for milestone in 1..=MILESTONE_INPUT_EVENTS {
            let threshold = count * milestone / (MILESTONE_INPUT_EVENTS + 1);
            while milestone_progress.load(Ordering::Acquire) < threshold {
                std::thread::sleep(Duration::from_micros(200));
            }
            if input_tx.send(input_sample(InputSource::Milestone)).is_err() {
                return;
            }
        }
    });
    let mut event_latency = Vec::with_capacity(count);
    let mut input_latency = Vec::new();
    let mut timed_input_latency = Vec::new();
    let mut milestone_input_latency = Vec::new();
    let mut draws = 0usize;
    let mut processed = 0usize;
    let mut max_batch_events = 0usize;
    let mut handling = Duration::ZERO;
    let mut rendering = Duration::ZERO;
    let started = Instant::now();

    while processed < count {
        if let Ok((queued, event, source)) = input_rx.try_recv() {
            let redraw = app.handle_event(event);
            assert!(
                redraw.handled(),
                "composer input must remain actionable under stream pressure"
            );
            let rendering_started = Instant::now();
            match redraw {
                Redraw::Light => terminal.draw(|frame| app.render_light(frame)),
                _ => terminal.draw(|frame| app.render(frame)),
            }
            .expect("input render");
            rendering += rendering_started.elapsed();
            draws += 1;
            let latency = queued.elapsed();
            input_latency.push(latency);
            match source {
                InputSource::Timed => timed_input_latency.push(latency),
                InputSource::Milestone => milestone_input_latency.push(latency),
            }
            continue;
        }
        let event = app
            .loop_events
            .as_mut()
            .expect("attached stream")
            .try_recv()
            .expect("queued loop event");
        let batch_events = match policy {
            DrawPolicy::OneEvent => {
                let queued = event_enqueued_at.pop_front().expect("enqueue timestamp");
                event_latency.push(queued.elapsed());
                let handling_started = Instant::now();
                app.handle_loop_event(event);
                handling += handling_started.elapsed();
                1
            }
            DrawPolicy::Batched => {
                let handling_started = Instant::now();
                let batch_events = app.handle_loop_event_batch(event, || {
                    let queued = event_enqueued_at.pop_front().expect("enqueue timestamp");
                    event_latency.push(queued.elapsed());
                });
                handling += handling_started.elapsed();
                batch_events
            }
        };
        max_batch_events = max_batch_events.max(batch_events);
        let rendering_started = Instant::now();
        terminal
            .draw(|frame| app.render(frame))
            .expect("stream render");
        rendering += rendering_started.elapsed();
        draws += 1;
        processed += batch_events;
        processed_count.store(processed, Ordering::Release);
    }
    stop_input.store(true, Ordering::Release);
    input_producer.join().expect("input producer");
    milestone_producer.join().expect("milestone input producer");
    while let Ok((queued, event, source)) = input_rx.try_recv() {
        assert!(app.handle_event(event).handled());
        let rendering_started = Instant::now();
        terminal
            .draw(|frame| app.render(frame))
            .expect("last input render");
        rendering += rendering_started.elapsed();
        draws += 1;
        let latency = queued.elapsed();
        input_latency.push(latency);
        match source {
            InputSource::Timed => timed_input_latency.push(latency),
            InputSource::Milestone => milestone_input_latency.push(latency),
        }
    }
    let elapsed = started.elapsed();
    assert_eq!(processed, count);
    assert_eq!(event_latency.len(), count);
    assert!(event_enqueued_at.is_empty());
    assert_eq!(milestone_input_latency.len(), MILESTONE_INPUT_EVENTS);
    assert_eq!(
        app.state.composer.buffer.content(),
        "x".repeat(input_latency.len())
    );
    assert!(matches!(app.state.agent_run, state::AgentRunState::Idle));
    assert!(app
        .loop_events
        .as_ref()
        .expect("stream receiver")
        .is_empty());

    // Compare the final transcript to direct ordered replay. Input remains in
    // its own composer and must never alter the provider's answer or tools.
    let mut reference = fixture_app();
    for event in mixed_events(count) {
        reference.handle_loop_event(event);
    }
    assert_eq!(
        app.conversation.presentation(),
        reference.conversation.presentation(),
        "the entire transcript must match direct ordered replay"
    );
    reference.state.composer = app.state.composer.clone();
    let mut reference_terminal =
        Terminal::new(TestBackend::new(120, 36)).expect("reference terminal");
    reference_terminal
        .draw(|frame| reference.render(frame))
        .expect("reference render");
    let final_buffer = BufferSnapshot::capture(terminal.backend().buffer(), None);
    let reference_buffer = BufferSnapshot::capture(reference_terminal.backend().buffer(), None);
    assert_eq!(
        final_buffer, reference_buffer,
        "every event must preserve the final rendered output"
    );
    // Faster runs naturally receive fewer timed keystrokes. After validating
    // every input above, normalize only the composer outside the timed region
    // so baseline/batched frames can be compared without that confounder.
    app.state.composer = empty_composer.clone();
    reference.state.composer = empty_composer;
    terminal
        .draw(|frame| app.render(frame))
        .expect("normalized render");
    reference_terminal
        .draw(|frame| reference.render(frame))
        .expect("normalized reference render");
    let normalized_buffer = BufferSnapshot::capture(terminal.backend().buffer(), None);
    assert_eq!(
        normalized_buffer,
        BufferSnapshot::capture(reference_terminal.backend().buffer(), None),
        "normalized transcript must match ordered replay"
    );
    eprintln!(
        "TUI_STREAM_BURST {}",
        json!({
            "policy": policy.name(), "width": 120, "height": 36,
            "events": count, "sample": sample, "draws": draws,
            "stream_draws": draws - input_latency.len(), "max_batch_events": max_batch_events,
            "inputs": input_latency.len(), "input_interval_ms": INPUT_INTERVAL.as_millis(),
            "elapsed_ms": elapsed.as_secs_f64() * 1_000.0,
            "loop_handler_ms": handling.as_secs_f64() * 1_000.0,
            "render_ms": rendering.as_secs_f64() * 1_000.0,
            "queue_p95_ms": percentile(&event_latency, 95),
            "queue_max_ms": percentile(&event_latency, 100),
            "input_p95_ms": percentile(&input_latency, 95),
            "input_max_ms": percentile(&input_latency, 100),
            "timed_inputs": timed_input_latency.len(),
            "timed_input_p95_ms": percentile(&timed_input_latency, 95),
            "timed_input_max_ms": percentile(&timed_input_latency, 100),
            "milestone_inputs": milestone_input_latency.len(),
            "milestone_input_p95_ms": percentile(&milestone_input_latency, 95),
            "milestone_input_max_ms": percentile(&milestone_input_latency, 100),
            "final_buffer": final_buffer.stable_fingerprint(),
            "normalized_final_buffer": normalized_buffer.stable_fingerprint(),
        })
    );
    normalized_buffer
}

fn attach_events(app: &mut PreviewApp, events: impl IntoIterator<Item = LoopEvent>) {
    let (sender, receiver) = mpsc::unbounded_channel();
    for event in events {
        sender.send(event).expect("enqueue fixture");
    }
    app.loop_events = Some(receiver);
}

fn next_event(app: &mut PreviewApp) -> LoopEvent {
    app.loop_events
        .as_mut()
        .expect("attached receiver")
        .try_recv()
        .expect("queued event")
}

#[test]
fn loop_batch_publishes_interaction_and_terminal_boundaries_before_queued_tail() {
    let boundaries = [
        (
            LoopEvent::ToolApprovalRequired {
                id: "approval".to_owned(),
                name: "bash".to_owned(),
                arguments: json!({ "command": "true" }),
            },
            true,
            false,
        ),
        (
            LoopEvent::PlanComplete {
                tool_call_id: "plan".to_owned(),
                title: "Fixture plan".to_owned(),
                task_count: 1,
            },
            true,
            false,
        ),
        (
            LoopEvent::AwaitingInput {
                tool_call_id: "question".to_owned(),
                tool_name: "AskUserQuestion".to_owned(),
            },
            false,
            false,
        ),
        (
            LoopEvent::Error {
                error: "fixture failure".to_owned(),
            },
            false,
            false,
        ),
        (
            LoopEvent::Finished {
                session_id: "preview-session".to_owned(),
                stop_reason: LoopStopReason::Completed,
            },
            false,
            true,
        ),
    ];
    for (boundary, pending, finished) in boundaries {
        let mut app = fixture_app();
        let (input, _receiver) = mpsc::unbounded_channel();
        app.loop_input = Some(input);
        attach_events(
            &mut app,
            [LoopEvent::TextDelta {
                delta: "queued tail".to_owned(),
            }],
        );
        let mut observed = 0;
        assert_eq!(app.handle_loop_event_batch(boundary, || observed += 1), 1);
        assert_eq!(observed, 1);
        assert_eq!(
            !app.conversation
                .presentation()
                .pending_interactions
                .is_empty(),
            pending
        );
        assert_eq!(app.loop_input.is_none(), finished);
        assert_eq!(
            matches!(app.state.agent_run, state::AgentRunState::Idle),
            finished
        );
        if pending {
            assert_eq!(app.state.focus, FocusTarget::DecisionDock);
        }
        assert!(matches!(
            next_event(&mut app),
            LoopEvent::TextDelta { delta } if delta == "queued tail"
        ));
    }
}

#[test]
fn loop_batch_publishes_questions_before_awaiting_input_arrives() {
    let mut app = fixture_app();
    attach_events(
        &mut app,
        [LoopEvent::AwaitingInput {
            tool_call_id: "question".to_owned(),
            tool_name: "AskUserQuestion".to_owned(),
        }],
    );
    let count = app.handle_loop_event_batch(
        LoopEvent::ToolCallComplete {
            id: "question".to_owned(),
            name: "AskUserQuestion".to_owned(),
            arguments: json!({ "questions": [{
                "header": "Next", "question": "Continue?",
                "options": [{ "label": "Continue" }], "multi_select": false
            }] }),
        },
        || {},
    );
    assert_eq!(count, 1);
    assert_eq!(app.state.focus, FocusTarget::DecisionDock);
    assert!(matches!(
        app.conversation.presentation().pending_interactions.first(),
        Some(PendingInteraction::Questions(questions))
            if questions.tool_call_id == "question"
                && questions.questions[0].question == "Continue?"
    ));
    assert!(matches!(
        next_event(&mut app),
        LoopEvent::AwaitingInput { .. }
    ));
}

#[test]
fn loop_batch_yields_after_time_budget_without_consuming_the_next_event() {
    let mut app = fixture_app();
    attach_events(&mut app, mixed_events(3));
    let first = next_event(&mut app);
    assert_eq!(
        app.handle_loop_event_batch(first, || {
            std::thread::sleep(LOOP_BATCH_MAX_DURATION + Duration::from_millis(1));
        }),
        1
    );
    assert!(matches!(
        next_event(&mut app),
        LoopEvent::ThinkingComplete { .. }
    ));
}

#[test]
fn loop_batch_caps_work_preserves_order_and_allows_input_between_frames() {
    let count = LOOP_BATCH_MAX_EVENTS * 3 + 1;
    let mut app = fixture_app();
    attach_events(&mut app, mixed_events(count));
    let mut terminal = Terminal::new(TestBackend::new(120, 36)).expect("test terminal");
    terminal
        .draw(|frame| app.render(frame))
        .expect("initial render");
    let mut processed = 0;
    let mut observed = 0;
    let mut batches = 0;
    while processed < count {
        let first = next_event(&mut app);
        let batch = app.handle_loop_event_batch(first, || observed += 1);
        assert!((1..=LOOP_BATCH_MAX_EVENTS).contains(&batch));
        processed += batch;
        batches += 1;
        terminal
            .draw(|frame| app.render(frame))
            .expect("batch render");
        // The production select checks terminal input first when the batch
        // returns. Exercise the same handler before consuming the queued tail.
        assert!(app
            .handle_event(input_sample(InputSource::Timed).1)
            .handled());
        assert_eq!(app.state.composer.buffer.content(), "x".repeat(batches));
        terminal
            .draw(|frame| app.render(frame))
            .expect("input render");
    }
    assert_eq!(observed, count);
    assert_eq!(processed, count);
    assert!(batches >= count.div_ceil(LOOP_BATCH_MAX_EVENTS));
    assert!(matches!(app.state.agent_run, state::AgentRunState::Idle));
    // The batch method must not take ownership of Loop(None) cleanup.
    assert!(app
        .loop_events
        .as_ref()
        .expect("receiver retained")
        .is_empty());

    let mut reference = fixture_app();
    for event in mixed_events(count) {
        reference.handle_loop_event(event);
    }
    assert_eq!(
        app.conversation.presentation(),
        reference.conversation.presentation(),
        "the entire transcript must match direct ordered replay"
    );
    reference.state.composer = app.state.composer.clone();
    let mut reference_terminal = Terminal::new(TestBackend::new(120, 36)).expect("reference");
    reference_terminal
        .draw(|frame| reference.render(frame))
        .expect("reference render");
    assert_eq!(
        BufferSnapshot::capture(terminal.backend().buffer(), None),
        BufferSnapshot::capture(reference_terminal.backend().buffer(), None),
        "batched frames must converge to the same ordered transcript and input"
    );
}

#[test]
fn loop_batch_returns_when_no_next_event_is_ready_without_detaching_stream() {
    let mut app = fixture_app();
    let (sender, receiver) = mpsc::unbounded_channel();
    app.loop_events = Some(receiver);
    for disconnected in [false, true] {
        assert_eq!(
            app.handle_loop_event_batch(
                LoopEvent::TextDelta {
                    delta: "answer".to_owned()
                },
                || {},
            ),
            1
        );
        assert!(app.loop_events.is_some());
        if !disconnected {
            // Explicitly close after the empty-but-live case, preserving the
            // receiver so the next call exercises an empty disconnected queue.
            app.loop_events.as_mut().expect("receiver").close();
        }
    }
    drop(sender);
}

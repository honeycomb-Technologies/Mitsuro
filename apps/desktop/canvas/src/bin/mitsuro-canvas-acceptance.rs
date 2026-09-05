//! Product-API provisioner and report evaluator for Native Canvas acceptance.
//!
//! This binary is feature-gated and is never a data source for the UI. It
//! creates ordinary Code sessions, terminals, Browsers, and canvas bindings on
//! an explicitly selected isolated server, then verifies the reports emitted
//! by the normal `mitsuro-canvas` binary.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
use std::io::{Read as _, Write as _};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{bail, Context as _, Result};
use futures_util::{SinkExt as _, StreamExt as _};
use mitsuro_client::{
    BrowserSessionStatus, BrowserTerminationState, CanvasNodeBinding, CanvasResourceNodeRequest,
    CanvasResourceSpec, CreateCanvasRequest, CreateCanvasResourceRequest,
    CreateCanvasResourceResponse, MitsuroClient, SessionType, SpatialCanvas,
    SpatialCanvasSurface, TakeTerminalControlRequest, TerminalSize, TerminalTransportCommand,
    TerminalTransportEvent, WorldRect,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest as _, Sha256};
use tokio_tungstenite::tungstenite::Message;
use uuid::Uuid;
use wayland_client::globals::{registry_queue_init, GlobalListContents};
use wayland_client::protocol::{wl_pointer, wl_registry};
use wayland_client::{Connection, Dispatch, Proxy as _, QueueHandle};

mod wlr_virtual_pointer {
    use wayland_client;
    use wayland_client::protocol::*;

    pub mod __interfaces {
        use wayland_client::protocol::__interfaces::*;

        wayland_scanner::generate_interfaces!(
            "protocols/wlr-virtual-pointer-unstable-v1.xml"
        );
    }

    use self::__interfaces::*;
    wayland_scanner::generate_client_code!("protocols/wlr-virtual-pointer-unstable-v1.xml");
}

use wlr_virtual_pointer::zwlr_virtual_pointer_manager_v1::ZwlrVirtualPointerManagerV1;
use wlr_virtual_pointer::zwlr_virtual_pointer_v1::ZwlrVirtualPointerV1;

const MANIFEST_SCHEMA_VERSION: u16 = 1;
const REPORT_SCHEMA_VERSION: u16 = 1;
const AGENT_COUNT: usize = 16;
const TERMINAL_COUNT: usize = 8;
const BROWSER_COUNT: usize = 4;

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("mitsuro-canvas-acceptance: {error:#}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let mut arguments = std::env::args().skip(1);
    let command = arguments.next().context(
        "expected prepare, populate, verify, or evaluate followed by named arguments",
    )?;
    let arguments = NamedArguments::parse(arguments)?;
    match command.as_str() {
        "prepare" => prepare(&arguments).await,
        "populate" => populate(&arguments).await,
        "verify" => verify(&arguments).await,
        "terminal-load" => terminal_load(&arguments).await,
        "pointer-probe" => pointer_probe(&arguments),
        "pointer-drag" => pointer_drag(&arguments),
        "evaluate" => evaluate(&arguments),
        _ => bail!("unknown acceptance command {command:?}"),
    }
}

fn pointer_probe(arguments: &NamedArguments) -> Result<()> {
    arguments.require_empty()?;
    let pointer = AcceptanceVirtualPointer::connect()?;
    pointer.connection.roundtrip()?;
    println!(
        "{}",
        serde_json::to_string(&json!({
            "protocol": "zwlr_virtual_pointer_manager_v1",
            "version": pointer.manager.version(),
            "wayland_display": pointer.display,
            "advertised": true
        }))?
    );
    Ok(())
}

fn pointer_drag(arguments: &NamedArguments) -> Result<()> {
    arguments.require_only(&["duration-seconds", "width", "height", "interval-ms"])?;
    let duration_seconds = arguments
        .required("duration-seconds")?
        .parse::<u64>()
        .context("parsing --duration-seconds")?;
    let width = arguments
        .required("width")?
        .parse::<u32>()
        .context("parsing --width")?;
    let height = arguments
        .required("height")?
        .parse::<u32>()
        .context("parsing --height")?;
    let interval_ms = arguments
        .required("interval-ms")?
        .parse::<u64>()
        .context("parsing --interval-ms")?;
    if !(1..=86_400).contains(&duration_seconds) {
        bail!("pointer drag duration must be 1-86400 seconds");
    }
    if !(640..=7_680).contains(&width) || !(480..=4_320).contains(&height) {
        bail!("pointer drag extent must be a 640x480 through 7680x4320 surface");
    }
    if !(16..=1_000).contains(&interval_ms) {
        bail!("pointer drag interval must be 16-1000 milliseconds");
    }

    let pointer = AcceptanceVirtualPointer::connect()?;
    pointer.connection.roundtrip()?;
    let started = Instant::now();
    let duration = Duration::from_secs(duration_seconds);
    let center_x = width / 2;
    let center_y = height / 2;
    let points = [
        (
            center_x.saturating_add(18).min(width),
            center_y.saturating_add(9).min(height),
        ),
        (
            center_x.saturating_add(7).min(width),
            center_y.saturating_add(24).min(height),
        ),
        (center_x, center_y),
    ];
    let mut drag_sequences = 0_u64;
    while started.elapsed() < duration {
        pointer.motion_absolute(started, center_x, center_y, width, height);
        pointer.button(started, wl_pointer::ButtonState::Pressed);
        for (x, y) in points {
            pointer.motion_absolute(started, x, y, width, height);
        }
        pointer.button(started, wl_pointer::ButtonState::Released);
        pointer.connection.flush()?;
        drag_sequences = drag_sequences.saturating_add(1);
        std::thread::sleep(Duration::from_millis(interval_ms));
    }
    pointer.connection.roundtrip()?;
    println!(
        "{}",
        serde_json::to_string(&json!({
            "protocol": "zwlr_virtual_pointer_manager_v1",
            "wayland_display": pointer.display,
            "configured_duration_seconds": duration_seconds,
            "elapsed_ms": started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
            "drag_sequences": drag_sequences,
            "extent": {"width": width, "height": height},
            "interval_ms": interval_ms
        }))?
    );
    Ok(())
}

struct AcceptanceVirtualPointer {
    connection: Connection,
    _event_queue: wayland_client::EventQueue<PointerState>,
    manager: ZwlrVirtualPointerManagerV1,
    pointer: ZwlrVirtualPointerV1,
    display: String,
}

impl AcceptanceVirtualPointer {
    fn connect() -> Result<Self> {
        if std::env::var_os("WAYLAND_SOCKET").is_some() {
            bail!("WAYLAND_SOCKET is ambiguous; select the compositor only with WAYLAND_DISPLAY");
        }
        let display = std::env::var("WAYLAND_DISPLAY")
            .context("WAYLAND_DISPLAY must select the acceptance compositor")?;
        if display.is_empty() || display.chars().any(char::is_control) {
            bail!("WAYLAND_DISPLAY must be a visible socket name");
        }
        let connection = Connection::connect_to_env()
            .context("connecting to the selected WAYLAND_DISPLAY socket")?;
        let (globals, event_queue) = registry_queue_init::<PointerState>(&connection)
            .context("reading the selected compositor registry")?;
        let queue_handle = event_queue.handle();
        let manager: ZwlrVirtualPointerManagerV1 = globals
            .bind(&queue_handle, 1..=2, ())
            .context("selected compositor does not advertise zwlr_virtual_pointer_manager_v1")?;
        let pointer = manager.create_virtual_pointer(None, &queue_handle, ());
        connection.flush()?;
        Ok(Self {
            connection,
            _event_queue: event_queue,
            manager,
            pointer,
            display,
        })
    }

    fn motion_absolute(&self, started: Instant, x: u32, y: u32, width: u32, height: u32) {
        self.pointer
            .motion_absolute(pointer_timestamp(started), x, y, width, height);
        self.pointer.frame();
    }

    fn button(&self, started: Instant, state: wl_pointer::ButtonState) {
        const BTN_MIDDLE: u32 = 0x112;
        self.pointer
            .button(pointer_timestamp(started), BTN_MIDDLE, state);
        self.pointer.frame();
    }
}

fn pointer_timestamp(started: Instant) -> u32 {
    let wrapped = started.elapsed().as_millis() & u128::from(u32::MAX);
    u32::try_from(wrapped).unwrap_or_default()
}

struct PointerState;

impl Dispatch<wl_registry::WlRegistry, GlobalListContents> for PointerState {
    fn event(
        _: &mut Self,
        _: &wl_registry::WlRegistry,
        _: wl_registry::Event,
        _: &GlobalListContents,
        _: &Connection,
        _: &QueueHandle<Self>,
    ) {
    }
}

impl Dispatch<ZwlrVirtualPointerManagerV1, ()> for PointerState {
    fn event(
        _: &mut Self,
        _: &ZwlrVirtualPointerManagerV1,
        _: wlr_virtual_pointer::zwlr_virtual_pointer_manager_v1::Event,
        _: &(),
        _: &Connection,
        _: &QueueHandle<Self>,
    ) {
    }
}

impl Dispatch<ZwlrVirtualPointerV1, ()> for PointerState {
    fn event(
        _: &mut Self,
        _: &ZwlrVirtualPointerV1,
        _: wlr_virtual_pointer::zwlr_virtual_pointer_v1::Event,
        _: &(),
        _: &Connection,
        _: &QueueHandle<Self>,
    ) {
    }
}

async fn prepare(arguments: &NamedArguments) -> Result<()> {
    let server = arguments.required("server")?;
    let project = canonical_project(arguments.required_path("project")?)?;
    let manifest_path = arguments.required_path("manifest")?;
    validate_output_path(&manifest_path)?;
    if manifest_path.exists() {
        bail!("prepare refuses to replace an existing manifest");
    }
    let client = checked_client(server).await?;
    let canvases = client
        .list_canvases(Some(SpatialCanvasSurface::Code))
        .await?;
    if !canvases.canvases.is_empty() {
        bail!(
            "acceptance server is not isolated: it already owns {} Code canvas(es)",
            canvases.canvases.len()
        );
    }
    let response = client
        .create_canvas(&CreateCanvasRequest {
            create_op_id: format!("canvas-acceptance-{}", Uuid::new_v4()),
            project_dir: project.to_string_lossy().into_owned(),
            surface: SpatialCanvasSurface::Code,
            name: "Acceptance".to_owned(),
            shared_theme_id: "graphite".to_owned(),
        })
        .await?;
    if !response.created {
        bail!("isolated acceptance canvas create unexpectedly replayed an earlier operation");
    }
    let manifest = ProvisionManifestV1 {
        schema_version: MANIFEST_SCHEMA_VERSION,
        server_origin: server.to_owned(),
        project_dir: project.to_string_lossy().into_owned(),
        canvas_id: response.canvas.id,
        agents: Vec::new(),
        terminals: Vec::new(),
        browsers: Vec::new(),
        populated: false,
    };
    write_manifest(&manifest_path, &manifest)?;
    println!("{}", serde_json::to_string_pretty(&manifest)?);
    Ok(())
}

async fn populate(arguments: &NamedArguments) -> Result<()> {
    let manifest_path = arguments.required_path("manifest")?;
    let mut manifest = read_manifest(&manifest_path)?;
    if manifest.populated {
        return verify_manifest_resources(&manifest).await;
    }
    let client = checked_client(&manifest.server_origin).await?;
    let intents_added = ensure_resource_intents(&mut manifest)?;
    if intents_added {
        // The stable operation and node IDs must be durable before the first
        // request. A process interruption can then replay the same server-owned
        // operation without creating an orphan resource.
        write_manifest(&manifest_path, &manifest)?;
    }
    let canvas = client.get_canvas(&manifest.canvas_id).await?;
    validate_manifest_canvas(&manifest, &canvas)?;

    for group in [ProvisionGroup::Agent, ProvisionGroup::Terminal, ProvisionGroup::Browser] {
        let count = group.intents(&manifest).len();
        for index in 0..count {
            let request = group.intents(&manifest)[index].request.clone();
            let response = client
                .create_canvas_resource(&manifest.canvas_id, &request)
                .await
                .with_context(|| {
                    format!(
                        "atomically provisioning {} {}/{}",
                        group.label(),
                        index + 1,
                        count
                    )
                })?;
            validate_resource_response(&manifest, &request, &response)?;
        }
    }

    verify_manifest_resources(&manifest).await?;
    manifest.populated = true;
    write_manifest(&manifest_path, &manifest)?;
    println!("{}", serde_json::to_string_pretty(&manifest)?);
    Ok(())
}

async fn verify(arguments: &NamedArguments) -> Result<()> {
    let manifest = read_manifest(&arguments.required_path("manifest")?)?;
    verify_manifest_resources(&manifest).await
}

async fn terminal_load(arguments: &NamedArguments) -> Result<()> {
    let manifest = read_manifest(&arguments.required_path("manifest")?)?;
    validate_provision_manifest(&manifest, true)?;
    if !manifest.populated {
        bail!("terminal load requires a fully populated acceptance manifest");
    }
    let duration_seconds = arguments
        .required("duration-seconds")?
        .parse::<u64>()
        .context("parsing --duration-seconds")?;
    if !(5..=86_400).contains(&duration_seconds) {
        bail!("terminal load duration must be 5-86400 seconds");
    }
    let report_path = arguments.required_path("report")?;
    validate_output_path(&report_path)?;
    let client = checked_client(&manifest.server_origin).await?;
    let started = Instant::now();
    let mut tasks = Vec::with_capacity(TERMINAL_COUNT);
    for terminal in &manifest.terminals {
        let client = client.clone();
        let terminal_id = terminal.request.node.id.clone();
        tasks.push(tokio::spawn(run_terminal_load(
            client,
            terminal_id,
            Duration::from_secs(duration_seconds),
        )));
    }
    let mut terminals = Vec::with_capacity(TERMINAL_COUNT);
    for task in tasks {
        terminals.push(task.await.context("joining terminal load task")??);
    }
    let report = TerminalLoadReportV1 {
        schema_version: REPORT_SCHEMA_VERSION,
        configured_duration_seconds: duration_seconds,
        elapsed_ms: started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
        total_input_commands: terminals.iter().map(|item| item.input_commands).sum(),
        total_output_bytes: terminals.iter().map(|item| item.output_bytes).sum(),
        passed: terminals.len() == TERMINAL_COUNT
            && terminals
                .iter()
                .all(|item| item.input_commands > 0 && item.output_bytes > 0),
        terminals,
    };
    atomic_write_private(&report_path, &serde_json::to_vec_pretty(&report)?)?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    if !report.passed {
        bail!("one or more terminal load streams produced no input or output");
    }
    Ok(())
}

async fn run_terminal_load(
    client: MitsuroClient,
    terminal_id: String,
    duration: Duration,
) -> Result<TerminalLoadItem> {
    let client_id = format!("canvas-acceptance-load-{terminal_id}");
    let lease = client
        .take_terminal_control(
            &terminal_id,
            &TakeTerminalControlRequest {
                client_id: client_id.clone(),
            },
        )
        .await
        .with_context(|| format!("taking control of terminal {terminal_id}"))?
        .lease;
    let descriptor = client.get_terminal(&terminal_id).await?;
    let url = client.terminal_stream_url(&terminal_id, Some(descriptor.next_offset))?;
    let request = client.websocket_request(&url)?;
    let (socket, _) = tokio_tungstenite::connect_async(request)
        .await
        .with_context(|| format!("attaching terminal load stream {terminal_id}"))?;
    let (mut sink, mut source) = socket.split();
    let initial_command = TerminalTransportCommand::InputText {
        lease_token: lease.lease_token.clone(),
        text: "i=0; while :; do i=$((i+1)); printf 'mitsuro-acceptance-output-%08d\\n' \"$i\"; sleep 0.02; done &\n"
            .to_owned(),
    };
    sink.send(Message::Text(serde_json::to_string(&initial_command)?))
        .await?;
    let started = Instant::now();
    let deadline = started + duration;
    let mut interval = tokio::time::interval(Duration::from_millis(250));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut input_commands = 1_u64;
    let mut output_bytes = 0_u64;
    let mut replay_gaps = 0_u64;
    let mut rejections = 0_u64;
    while Instant::now() < deadline {
        tokio::select! {
            _ = interval.tick() => {
                let command = TerminalTransportCommand::InputText {
                    lease_token: lease.lease_token.clone(),
                    text: format!("printf 'mitsuro-acceptance-input-%08d\\n'\n", input_commands),
                };
                sink.send(Message::Text(serde_json::to_string(&command)?)).await?;
                input_commands = input_commands.saturating_add(1);
                if input_commands % 20 == 0 {
                    let ping = TerminalTransportCommand::Ping {
                        lease_token: Some(lease.lease_token.clone()),
                    };
                    sink.send(Message::Text(serde_json::to_string(&ping)?)).await?;
                }
            }
            incoming = source.next() => {
                let Some(incoming) = incoming else {
                    bail!("terminal load stream {terminal_id} ended early");
                };
                let incoming = incoming?;
                if let Message::Text(text) = incoming {
                    match serde_json::from_str::<TerminalTransportEvent>(&text) {
                        Ok(TerminalTransportEvent::Output { bytes, .. }) => {
                            output_bytes = output_bytes.saturating_add(
                                u64::try_from(bytes.len()).unwrap_or(u64::MAX),
                            );
                        }
                        Ok(TerminalTransportEvent::Gap { .. }) => {
                            replay_gaps = replay_gaps.saturating_add(1);
                        }
                        Ok(TerminalTransportEvent::CommandRejected { .. }) => {
                            rejections = rejections.saturating_add(1);
                        }
                        Ok(TerminalTransportEvent::Exited { .. }) => {
                            bail!("terminal {terminal_id} exited during load");
                        }
                        Ok(_) | Err(_) => {}
                    }
                }
            }
        }
    }
    let _ = sink
        .send(Message::Text(serde_json::to_string(
            &TerminalTransportCommand::Detach,
        )?))
        .await;
    let _ = sink.send(Message::Close(None)).await;
    if rejections > 0 {
        bail!("terminal {terminal_id} rejected {rejections} load commands");
    }
    Ok(TerminalLoadItem {
        terminal_id,
        elapsed_ms: started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
        input_commands,
        output_bytes,
        replay_gaps,
        command_rejections: rejections,
    })
}

async fn verify_manifest_resources(manifest: &ProvisionManifestV1) -> Result<()> {
    validate_provision_manifest(manifest, true)?;
    let client = checked_client(&manifest.server_origin).await?;
    let canvas = client.get_canvas(&manifest.canvas_id).await?;
    validate_manifest_canvas(manifest, &canvas)?;
    let expected_agents = provisioned_resource_ids(&manifest.agents);
    let expected_terminals = provisioned_resource_ids(&manifest.terminals);
    let expected_browsers = provisioned_resource_ids(&manifest.browsers);
    let sessions = client.list_sessions().await?;
    let terminals = client.list_terminals().await?;
    let browsers = client.list_browser_sessions().await?;
    let actual_agents = sessions
        .iter()
        .filter(|session| session.session_type == SessionType::Code)
        .map(|session| &session.id)
        .collect::<BTreeSet<_>>();
    let actual_terminals = terminals
        .terminals
        .iter()
        .filter(|terminal| terminal.working_dir == manifest.project_dir)
        .map(|terminal| &terminal.id)
        .collect::<BTreeSet<_>>();
    let actual_browsers = browsers
        .sessions
        .iter()
        .filter(|browser| {
            browser.project_dir == manifest.project_dir
                && !browser_is_confirmed_ended(browser.status, browser.termination_state)
        })
        .map(|browser| &browser.id)
        .collect::<BTreeSet<_>>();
    if !expected_agents.is_subset(&actual_agents) {
        bail!("not all {AGENT_COUNT} provisioned Code Agents exist");
    }
    if !expected_terminals.is_subset(&actual_terminals) {
        bail!("not all {TERMINAL_COUNT} provisioned Terminals are live in the exact project");
    }
    if !expected_browsers.is_subset(&actual_browsers) {
        bail!("not all {BROWSER_COUNT} provisioned Browsers are live in the exact project");
    }
    let bindings = canvas
        .state
        .nodes
        .iter()
        .map(|node| match &node.binding {
            CanvasNodeBinding::Agent { session_id } => ("agent", session_id.as_str()),
            CanvasNodeBinding::Terminal { terminal_id } => ("terminal", terminal_id.as_str()),
            CanvasNodeBinding::Browser {
                browser_session_id, ..
            } => ("browser", browser_session_id.as_str()),
            CanvasNodeBinding::Files { .. } => ("files", ""),
            CanvasNodeBinding::Changes => ("changes", ""),
        })
        .collect::<BTreeSet<_>>();
    for id in &expected_agents {
        if !bindings.contains(&("agent", *id)) {
            bail!("Agent {id} is not bound to the acceptance canvas");
        }
    }
    for id in &expected_terminals {
        if !bindings.contains(&("terminal", *id)) {
            bail!("Terminal {id} is not bound to the acceptance canvas");
        }
    }
    for id in &expected_browsers {
        if !bindings.contains(&("browser", *id)) {
            bail!("Browser {id} is not bound to the acceptance canvas");
        }
    }
    println!(
        "verified canvas {} with {} Agent, {} Terminal, and {} Browser bindings",
        manifest.canvas_id, AGENT_COUNT, TERMINAL_COUNT, BROWSER_COUNT
    );
    Ok(())
}

fn browser_is_confirmed_ended(
    status: BrowserSessionStatus,
    termination: Option<BrowserTerminationState>,
) -> bool {
    status == BrowserSessionStatus::Stopped
        || (status == BrowserSessionStatus::Error
            && matches!(
                termination,
                Some(
                    BrowserTerminationState::NeverStarted
                        | BrowserTerminationState::ConfirmedStopped
                )
            ))
}

fn evaluate(arguments: &NamedArguments) -> Result<()> {
    let pulse_full_path = arguments.required_path("pulse-full")?;
    let pulse_static_path = arguments.required_path("pulse-static")?;
    let stress_path = arguments.required_path("stress")?;
    let terminal_load_path = arguments.required_path("terminal-load")?;
    let output_path = arguments.required_path("output")?;
    validate_output_path(&output_path)?;
    let expected_source = arguments.required("source-commit")?;
    if !is_sha1(expected_source) {
        bail!("source-commit must be the exact 40-character Git commit");
    }
    let expected_canvas_sha = arguments.required("canvas-sha256")?;
    let expected_server_sha = arguments.required("server-sha256")?;
    if !is_sha256(expected_canvas_sha) || !is_sha256(expected_server_sha) {
        bail!("binary hashes must be exact lowercase SHA-256 values");
    }
    let pulse_full = read_json(&pulse_full_path)?;
    let pulse_static = read_json(&pulse_static_path)?;
    let stress = read_json(&stress_path)?;
    let terminal_load = read_json(&terminal_load_path)?;
    let mut failures = Vec::new();
    validate_report(
        &pulse_full,
        "pulse_full",
        expected_source,
        expected_canvas_sha,
        expected_server_sha,
        &mut failures,
    );
    validate_report(
        &pulse_static,
        "pulse_static",
        expected_source,
        expected_canvas_sha,
        expected_server_sha,
        &mut failures,
    );
    validate_report(
        &stress,
        "stress",
        expected_source,
        expected_canvas_sha,
        expected_server_sha,
        &mut failures,
    );
    let full_p95 = json_f64(
        &pulse_full,
        &["timing", "aura_render_to_prepaint", "p95_ms"],
    );
    let static_p95 = json_f64(
        &pulse_static,
        &["timing", "static_render_to_prepaint", "p95_ms"],
    );
    let overhead_ms = full_p95
        .zip(static_p95)
        .map(|(full, static_value)| full - static_value);
    match overhead_ms {
        Some(overhead) if overhead <= 2.0 => {}
        Some(overhead) => failures.push(format!(
            "PulseVeil render-to-prepaint p95 overhead is {overhead:.3}ms, above 2.0ms"
        )),
        None => failures.push("PulseVeil calibration p95 samples are missing".to_owned()),
    }
    if json_u64(&terminal_load, &["schema_version"]) != Some(u64::from(REPORT_SCHEMA_VERSION))
        || terminal_load.pointer("/passed").and_then(Value::as_bool) != Some(true)
    {
        failures.push("terminal load report is missing, invalid, or failed".to_owned());
    }
    if json_u64(&terminal_load, &["configured_duration_seconds"]).unwrap_or_default() < 1_800 {
        failures.push("terminal load ran for less than the required 30 minutes".to_owned());
    }
    if terminal_load
        .get("terminals")
        .and_then(Value::as_array)
        .map(Vec::len)
        != Some(TERMINAL_COUNT)
    {
        failures.push("terminal load did not cover exactly 8 live PTYs".to_owned());
    }
    if json_u64(&terminal_load, &["total_input_commands"]).unwrap_or_default() < 8_000 {
        failures.push("terminal load sent too few sustained input commands".to_owned());
    }
    if json_u64(&terminal_load, &["total_output_bytes"]).unwrap_or_default() < 1_048_576 {
        failures.push("terminal load observed less than 1 MiB of PTY output".to_owned());
    }
    if json_u64(&stress, &["terminal_control", "unique_terminal_ids"])
        != Some(TERMINAL_COUNT as u64)
    {
        failures.push("native Terminal control did not cover exactly 8 stable IDs".to_owned());
    }
    let teardown_terminal_id = json_str(
        &stress,
        &["terminal_control", "teardown_target_terminal_id"],
    );
    if teardown_terminal_id.is_none()
        || stress
            .pointer("/terminal_control/teardown_target_reacquired")
            .and_then(Value::as_bool)
            != Some(true)
        || teardown_terminal_id.is_some_and(|terminal_id| {
            json_u64(
                &stress,
                &["terminal_control", "acquisitions_by_terminal", terminal_id],
            )
            .unwrap_or_default()
                < 2
        })
    {
        failures.push(
            "the staged Terminal did not prove a distinct post-load control reacquisition"
                .to_owned(),
        );
    }
    let combined = CombinedVerdictV1 {
        schema_version: REPORT_SCHEMA_VERSION,
        source_commit: expected_source.to_owned(),
        canvas_sha256: expected_canvas_sha.to_owned(),
        server_sha256: expected_server_sha.to_owned(),
        pulse_full_p95_ms: full_p95,
        pulse_static_p95_ms: static_p95,
        pulse_overhead_ms: overhead_ms,
        passed: failures.is_empty(),
        failures,
        reports: BTreeMap::from([
            ("pulse_full".to_owned(), pulse_full_path.to_string_lossy().into_owned()),
            (
                "pulse_static".to_owned(),
                pulse_static_path.to_string_lossy().into_owned(),
            ),
            ("stress".to_owned(), stress_path.to_string_lossy().into_owned()),
            (
                "terminal_load".to_owned(),
                terminal_load_path.to_string_lossy().into_owned(),
            ),
        ]),
    };
    atomic_write_private(&output_path, &serde_json::to_vec_pretty(&combined)?)?;
    println!("{}", serde_json::to_string_pretty(&combined)?);
    if !combined.passed {
        bail!("native Canvas acceptance did not pass");
    }
    Ok(())
}

fn validate_report(
    report: &Value,
    expected_phase: &str,
    expected_source: &str,
    expected_canvas_sha: &str,
    expected_server_sha: &str,
    failures: &mut Vec<String>,
) {
    let prefix = format!("{expected_phase} report");
    if json_u64(report, &["schema_version"]) != Some(u64::from(REPORT_SCHEMA_VERSION)) {
        failures.push(format!("{prefix} has the wrong schema"));
    }
    if json_str(report, &["phase"]) != Some(expected_phase) {
        failures.push(format!("{prefix} has the wrong phase"));
    }
    if json_str(report, &["source_commit"]) != Some(expected_source) {
        failures.push(format!("{prefix} source commit does not match"));
    }
    if json_str(report, &["client", "sha256"]) != Some(expected_canvas_sha) {
        failures.push(format!("{prefix} Canvas binary hash does not match"));
    }
    if json_str(report, &["server", "executable", "sha256"]) != Some(expected_server_sha) {
        failures.push(format!("{prefix} server binary hash does not match"));
    }
    if report.pointer("/complete").and_then(Value::as_bool) != Some(true) {
        failures.push(format!("{prefix} did not run for its configured duration"));
    }
    if report
        .pointer("/verdict/eligible")
        .and_then(Value::as_bool)
        != Some(true)
    {
        failures.push(format!("{prefix} is not individually eligible"));
    }
    let width = json_u64(report, &["viewport", "physical_width"]).unwrap_or_default();
    let height = json_u64(report, &["viewport", "physical_height"]).unwrap_or_default();
    if width < 1_900 || height < 1_000 {
        failures.push(format!(
            "{prefix} viewport {width}x{height} is not a 1080p-class run"
        ));
    }
}

fn ensure_resource_intents(manifest: &mut ProvisionManifestV1) -> Result<bool> {
    validate_provision_manifest(manifest, false)?;
    let mut changed = false;
    for group in [ProvisionGroup::Agent, ProvisionGroup::Terminal, ProvisionGroup::Browser] {
        while group.intents(manifest).len() < group.expected_count() {
            let index = group.intents(manifest).len();
            let intent = new_resource_intent(group, index);
            group.intents_mut(manifest).push(intent);
            changed = true;
        }
    }
    validate_provision_manifest(manifest, false)?;
    Ok(changed)
}

fn new_resource_intent(group: ProvisionGroup, index: usize) -> ProvisionIntentV1 {
    let global_index = group.global_index(index);
    let column = global_index % 4;
    let row = global_index / 4;
    let (title, width, height, resource) = match group {
        ProvisionGroup::Agent => (
            format!("Acceptance Agent {:02}", index + 1),
            720.0,
            520.0,
            CanvasResourceSpec::Agent,
        ),
        ProvisionGroup::Terminal => (
            format!("Acceptance Terminal {:02}", index + 1),
            680.0,
            420.0,
            CanvasResourceSpec::Terminal {
                size: TerminalSize { cols: 100, rows: 30 },
            },
        ),
        ProvisionGroup::Browser => (
            format!("Acceptance Browser {:02}", index + 1),
            720.0,
            520.0,
            CanvasResourceSpec::Browser {
                url: Some("about:blank".to_owned()),
            },
        ),
    };
    ProvisionIntentV1 {
        request: CreateCanvasResourceRequest {
            create_op_id: format!("canvas-acceptance-resource-{}", Uuid::new_v4()),
            client_id: "mitsuro-canvas-acceptance-provisioner".to_owned(),
            node: CanvasResourceNodeRequest {
                id: Uuid::new_v4().to_string(),
                title,
                rect: WorldRect {
                    x: column as f64 * 780.0 - 1_200.0,
                    y: row as f64 * 580.0 - 1_400.0,
                    width,
                    height,
                },
                z_index: i64::try_from(global_index + 1).unwrap_or(i64::MAX),
            },
            resource,
            retry_ambiguous: false,
        },
    }
}

fn validate_provision_manifest(manifest: &ProvisionManifestV1, require_complete: bool) -> Result<()> {
    if manifest.schema_version != MANIFEST_SCHEMA_VERSION {
        bail!("unsupported provision manifest schema");
    }
    if manifest.agents.len() > AGENT_COUNT
        || manifest.terminals.len() > TERMINAL_COUNT
        || manifest.browsers.len() > BROWSER_COUNT
    {
        bail!("provision manifest contains more resources than the acceptance workload");
    }
    if report.pointer("/verdict/eligible").and_then(Value::as_bool) != Some(true) {
        failures.push(format!("{prefix} is not individually eligible"));
    }
    if require_complete
        && (manifest.agents.len() != AGENT_COUNT
            || manifest.terminals.len() != TERMINAL_COUNT
            || manifest.browsers.len() != BROWSER_COUNT)
    {
        bail!("provision manifest does not contain the exact 16/8/4 workload");
    }
    let mut op_ids = BTreeSet::new();
    let mut node_ids = BTreeSet::new();
    for group in [ProvisionGroup::Agent, ProvisionGroup::Terminal, ProvisionGroup::Browser] {
        for intent in group.intents(manifest) {
            if intent.request.retry_ambiguous {
                bail!("acceptance never confirms an ambiguous resource retry automatically");
            }
            if intent.request.client_id != "mitsuro-canvas-acceptance-provisioner" {
                bail!("provision manifest has an unexpected client ID");
            }
            if !op_ids.insert(intent.request.create_op_id.as_str())
                || !node_ids.insert(intent.request.node.id.as_str())
            {
                bail!("provision manifest reuses a resource operation or node ID");
            }
            if !group.matches(&intent.request.resource) {
                bail!("provision manifest resource is stored in the wrong group");
            }
        }
    }
    Ok(())
}

fn validate_manifest_canvas(manifest: &ProvisionManifestV1, canvas: &SpatialCanvas) -> Result<()> {
    if canvas.id != manifest.canvas_id
        || canvas.project_dir != manifest.project_dir
        || canvas.surface != SpatialCanvasSurface::Code
    {
        bail!("manifest canvas no longer has the exact project/surface authority");
    }
    let intents = all_intents(manifest).collect::<BTreeMap<_, _>>();
    for node in &canvas.state.nodes {
        let Some(intent) = intents.get(node.id.as_str()) else {
            bail!("acceptance canvas contains a node outside the persisted provision intent");
        };
        if node.title != intent.request.node.title
            || node.rect != intent.request.node.rect
            || node.z_index != intent.request.node.z_index
        {
            bail!("acceptance canvas node metadata differs from its persisted intent");
        }
        let bound_id = binding_resource_id(&intent.request.resource, &node.binding)?;
        if bound_id != intent.request.node.id {
            bail!("acceptance canvas node binding differs from its stable resource/node ID");
        }
    }
    Ok(())
}

fn validate_resource_response(
    manifest: &ProvisionManifestV1,
    request: &CreateCanvasResourceRequest,
    response: &CreateCanvasResourceResponse,
) -> Result<()> {
    validate_manifest_canvas(manifest, &response.canvas)?;
    if response.node.id != request.node.id
        || response.node.title != request.node.title
        || response.node.rect != request.node.rect
        || response.node.z_index != request.node.z_index
    {
        bail!("atomic resource response changed the persisted node intent");
    }
    let resource_id = binding_resource_id(&request.resource, &response.node.binding)?;
    if resource_id != request.node.id {
        bail!("atomic resource response did not preserve the stable resource/node ID");
    }
    if !response
        .canvas
        .state
        .nodes
        .iter()
        .any(|node| node == &response.node)
    {
        bail!("atomic resource response did not include its bound node in the canvas");
    }
    Ok(())
}

fn binding_resource_id<'a>(
    resource: &CanvasResourceSpec,
    binding: &'a CanvasNodeBinding,
) -> Result<&'a str> {
    match (resource, binding) {
        (CanvasResourceSpec::Agent, CanvasNodeBinding::Agent { session_id }) => Ok(session_id),
        (
            CanvasResourceSpec::Terminal { .. },
            CanvasNodeBinding::Terminal { terminal_id },
        ) => Ok(terminal_id),
        (
            CanvasResourceSpec::Browser { .. },
            CanvasNodeBinding::Browser {
                browser_session_id, ..
            },
        ) => Ok(browser_session_id),
        _ => bail!("atomic resource response returned the wrong binding kind"),
    }
}

fn all_intents(
    manifest: &ProvisionManifestV1,
) -> impl Iterator<Item = (&str, &ProvisionIntentV1)> {
    manifest
        .agents
        .iter()
        .chain(&manifest.terminals)
        .chain(&manifest.browsers)
        .map(|intent| (intent.request.node.id.as_str(), intent))
}

fn provisioned_resource_ids(intents: &[ProvisionIntentV1]) -> BTreeSet<&str> {
    intents
        .iter()
        .map(|intent| intent.request.node.id.as_str())
        .collect()
}

async fn checked_client(origin: &str) -> Result<MitsuroClient> {
    let client = MitsuroClient::new(origin.to_owned())?;
    let health = client.health().await?;
    if health.status != "ok" || health.identity != "mitsuro-server" || health.api_protocol != 1 {
        bail!("selected server did not prove the matching Mitsuro health identity/protocol");
    }
    for (name, required) in [
        ("desktop_canvases", 1),
        ("canvas_resource_create", 1),
        ("terminal_sessions", 2),
        ("browser_sessions", 1),
        ("agent_streams", 1),
    ] {
        if !health.supports_capability(name, required) {
            bail!("selected server lacks {name}:{required}");
        }
    }
    Ok(client)
}

#[derive(Debug, Serialize, Deserialize)]
struct ProvisionManifestV1 {
    schema_version: u16,
    server_origin: String,
    project_dir: String,
    canvas_id: String,
    agents: Vec<ProvisionIntentV1>,
    terminals: Vec<ProvisionIntentV1>,
    browsers: Vec<ProvisionIntentV1>,
    populated: bool,
}

#[derive(Debug, Serialize, Deserialize)]
struct ProvisionIntentV1 {
    request: CreateCanvasResourceRequest,
}

#[derive(Debug, Clone, Copy)]
enum ProvisionGroup {
    Agent,
    Terminal,
    Browser,
}

impl ProvisionGroup {
    fn label(self) -> &'static str {
        match self {
            Self::Agent => "Agent",
            Self::Terminal => "Terminal",
            Self::Browser => "Browser",
        }
    }

    fn expected_count(self) -> usize {
        match self {
            Self::Agent => AGENT_COUNT,
            Self::Terminal => TERMINAL_COUNT,
            Self::Browser => BROWSER_COUNT,
        }
    }

    fn global_index(self, index: usize) -> usize {
        match self {
            Self::Agent => index,
            Self::Terminal => AGENT_COUNT + index,
            Self::Browser => AGENT_COUNT + TERMINAL_COUNT + index,
        }
    }

    fn matches(self, resource: &CanvasResourceSpec) -> bool {
        matches!(
            (self, resource),
            (Self::Agent, CanvasResourceSpec::Agent)
                | (Self::Terminal, CanvasResourceSpec::Terminal { .. })
                | (Self::Browser, CanvasResourceSpec::Browser { .. })
        )
    }

    fn intents(self, manifest: &ProvisionManifestV1) -> &[ProvisionIntentV1] {
        match self {
            Self::Agent => &manifest.agents,
            Self::Terminal => &manifest.terminals,
            Self::Browser => &manifest.browsers,
        }
    }

    fn intents_mut(self, manifest: &mut ProvisionManifestV1) -> &mut Vec<ProvisionIntentV1> {
        match self {
            Self::Agent => &mut manifest.agents,
            Self::Terminal => &mut manifest.terminals,
            Self::Browser => &mut manifest.browsers,
        }
    }
}

#[derive(Debug, Serialize)]
struct CombinedVerdictV1 {
    schema_version: u16,
    source_commit: String,
    canvas_sha256: String,
    server_sha256: String,
    pulse_full_p95_ms: Option<f64>,
    pulse_static_p95_ms: Option<f64>,
    pulse_overhead_ms: Option<f64>,
    passed: bool,
    failures: Vec<String>,
    reports: BTreeMap<String, String>,
}

#[derive(Debug, Serialize)]
struct TerminalLoadReportV1 {
    schema_version: u16,
    configured_duration_seconds: u64,
    elapsed_ms: u64,
    total_input_commands: u64,
    total_output_bytes: u64,
    passed: bool,
    terminals: Vec<TerminalLoadItem>,
}

#[derive(Debug, Serialize)]
struct TerminalLoadItem {
    terminal_id: String,
    elapsed_ms: u64,
    input_commands: u64,
    output_bytes: u64,
    replay_gaps: u64,
    command_rejections: u64,
}

struct NamedArguments {
    values: BTreeMap<String, String>,
}

impl NamedArguments {
    fn parse(arguments: impl Iterator<Item = String>) -> Result<Self> {
        let mut arguments = arguments.peekable();
        let mut values = BTreeMap::new();
        while let Some(name) = arguments.next() {
            let Some(name) = name.strip_prefix("--") else {
                bail!("expected named argument, got {name:?}");
            };
            if name.is_empty() || values.contains_key(name) {
                bail!("invalid or duplicate argument --{name}");
            }
            let value = arguments
                .next()
                .with_context(|| format!("--{name} requires a value"))?;
            values.insert(name.to_owned(), value);
        }
        Ok(Self { values })
    }

    fn required(&self, name: &str) -> Result<&str> {
        self.values
            .get(name)
            .map(String::as_str)
            .with_context(|| format!("missing --{name}"))
    }

    fn required_path(&self, name: &str) -> Result<PathBuf> {
        let path = PathBuf::from(self.required(name)?);
        if !path.is_absolute() {
            bail!("--{name} must be absolute");
        }
        Ok(path)
    }

    fn require_empty(&self) -> Result<()> {
        if let Some(name) = self.values.keys().next() {
            bail!("unexpected argument --{name}");
        }
        Ok(())
    }

    fn require_only(&self, allowed: &[&str]) -> Result<()> {
        if let Some(name) = self
            .values
            .keys()
            .find(|name| !allowed.contains(&name.as_str()))
        {
            bail!("unexpected argument --{name}");
        }
        Ok(())
    }
}

fn canonical_project(path: PathBuf) -> Result<PathBuf> {
    let path = path
        .canonicalize()
        .context("canonicalizing acceptance project")?;
    if !path.is_dir() {
        bail!("acceptance project must be a directory");
    }
    Ok(path)
}

fn read_manifest(path: &Path) -> Result<ProvisionManifestV1> {
    let manifest: ProvisionManifestV1 = serde_json::from_slice(&std::fs::read(path)?)?;
    validate_provision_manifest(&manifest, false)?;
    Ok(manifest)
}

fn write_manifest(path: &Path, manifest: &ProvisionManifestV1) -> Result<()> {
    atomic_write_private(path, &serde_json::to_vec_pretty(manifest)?)
}

fn read_json(path: &Path) -> Result<Value> {
    serde_json::from_slice(&std::fs::read(path)?).with_context(|| format!("reading {}", path.display()))
}

fn validate_output_path(path: &Path) -> Result<()> {
    if !path.is_absolute() {
        bail!("output path must be absolute");
    }
    let parent = path
        .parent()
        .context("output path has no parent")?
        .canonicalize()
        .context("canonicalizing output parent")?;
    if !parent.is_dir() {
        bail!("output parent is not a directory");
    }
    if let Ok(metadata) = std::fs::symlink_metadata(path) {
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            bail!("output target must be a regular file, never a symlink");
        }
    }
    Ok(())
}

fn atomic_write_private(path: &Path, bytes: &[u8]) -> Result<()> {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("acceptance.json");
    let staged = path.with_file_name(format!(".{name}.{}.tmp", std::process::id()));
    let mut options = OpenOptions::new();
    options.create(true).truncate(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let mut file = options.open(&staged)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    std::fs::rename(staged, path)?;
    Ok(())
}

fn json_value_at<'a>(value: &'a Value, path: &[&str]) -> Option<&'a Value> {
    path.iter().try_fold(value, |value, key| value.get(*key))
}

fn json_str<'a>(value: &'a Value, path: &[&str]) -> Option<&'a str> {
    json_value_at(value, path)?.as_str()
}

fn json_u64(value: &Value, path: &[&str]) -> Option<u64> {
    json_value_at(value, path)?.as_u64()
}

fn json_f64(value: &Value, path: &[&str]) -> Option<f64> {
    json_value_at(value, path)?.as_f64()
}

fn is_sha1(value: &str) -> bool {
    value.len() == 40
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[allow(dead_code)]
fn sha256_file(path: &Path) -> Result<String> {
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 128 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provisioned_layout_persists_exact_unique_atomic_intents() {
        let mut manifest = ProvisionManifestV1 {
            schema_version: MANIFEST_SCHEMA_VERSION,
            server_origin: "http://127.0.0.1:3999".to_owned(),
            project_dir: "/project".to_owned(),
            canvas_id: "canvas".to_owned(),
            agents: Vec::new(),
            terminals: Vec::new(),
            browsers: Vec::new(),
            populated: false,
        };
        assert!(ensure_resource_intents(&mut manifest).unwrap());
        assert!(!ensure_resource_intents(&mut manifest).unwrap());
        validate_provision_manifest(&manifest, true).unwrap();
        let intents = all_intents(&manifest)
            .map(|(_, intent)| intent)
            .collect::<Vec<_>>();
        assert_eq!(intents.len(), AGENT_COUNT + TERMINAL_COUNT + BROWSER_COUNT);
        assert_eq!(
            intents
                .iter()
                .map(|intent| &intent.request.node.id)
                .collect::<BTreeSet<_>>()
                .len(),
            intents.len()
        );
        assert_eq!(
            intents
                .iter()
                .map(|intent| &intent.request.create_op_id)
                .collect::<BTreeSet<_>>()
                .len(),
            intents.len()
        );
        assert!(intents.iter().all(|intent| !intent.request.retry_ambiguous));
        assert!(intents.iter().any(|intent| intent.request.node.rect.x < 0.0));
        assert!(intents.iter().any(|intent| intent.request.node.rect.y < 0.0));

        let encoded = serde_json::to_vec(&manifest).unwrap();
        let decoded: ProvisionManifestV1 = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(
            serde_json::to_value(&decoded).unwrap(),
            serde_json::to_value(&manifest).unwrap()
        );
    }

    #[test]
    fn hashes_require_exact_lowercase_wire_shapes() {
        assert!(is_sha1(&"a".repeat(40)));
        assert!(!is_sha1(&"x".repeat(40)));
        assert!(is_sha256(&"f".repeat(64)));
        assert!(!is_sha256(&"F".repeat(64)));
    }
}

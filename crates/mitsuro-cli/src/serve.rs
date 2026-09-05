//! `mitsuro serve` — unified server with embedded web app and Tailscale integration
//!
//! Starts the Mitsuro API server with the web frontend embedded in the binary.
//! On first run, prompts for provider and API key configuration.
//! Detects and reuses an already-running instance if present.

use anyhow::{Context, Result};
use std::io::{self, Write};
use std::net::{Ipv4Addr, TcpListener as StdTcpListener};

use mitsuro_core::ai::providers::ProviderId;
use mitsuro_core::paths;
use mitsuro_core::server_instance;
use mitsuro_core::storage::credentials::CredentialStore;
use mitsuro_core::tailscale::{self, TailscaleServeSetup};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

const DEFAULT_SERVER_PORT: u16 = 3000;
const DEFAULT_PORT_SEARCH_SPAN: u16 = 100;

/// Run the serve command.
pub async fn run(port: u16, desktop_managed: bool) -> Result<()> {
    // Check for existing running server
    if let Some(instance) = server_instance::detect_running_server().await {
        print_banner(instance.port, false);
        println!(
            "  Server already running (PID {}). Reusing existing instance.\n",
            instance.pid
        );
        // Don't start a new server — just print the URLs and exit
        if !desktop_managed {
            print_tailscale_status(tailscale::setup_tailscale_serve(instance.port));
        }
        return Ok(());
    }

    // First-run setup: check if credentials are configured
    let store = CredentialStore::load().unwrap_or_default();
    if !desktop_managed && store.providers_with_auth().is_empty() {
        run_setup_wizard()?;
    }

    let requested_port = port;
    let (port, listener) = reserve_server_listener(port, desktop_managed)?;

    if port != requested_port {
        println!(
            "  Port {} is already in use. Using {} instead.",
            requested_port, port
        );
    }

    // Hold singleton authority for the full server lifetime. The guard writes
    // the PID record atomically and only removes the exact record it owns.
    let _instance_guard = server_instance::ServerInstanceGuard::acquire(port)?;
    let shutdown_signal = shutdown_signal()?;

    print_banner(port, true);

    // Setup Tailscale serve (non-blocking, best-effort)
    if !desktop_managed {
        print_tailscale_status(tailscale::setup_tailscale_serve(port));
    }

    init_server_tracing();

    let config = mitsuro_server::ServerConfig {
        port,
        ..Default::default()
    };

    // Start server with graceful shutdown. Dropping the guard afterward removes
    // the PID record while preserving any newer process's replacement record.
    if desktop_managed {
        mitsuro_server::start_desktop_managed_server_with_listener_and_shutdown(
            config,
            listener,
            shutdown_signal,
        )
        .await?;
    } else {
        mitsuro_server::start_server_with_listener_and_shutdown(config, listener, shutdown_signal)
            .await?;
    }

    Ok(())
}

#[cfg(unix)]
fn shutdown_signal() -> Result<impl std::future::Future<Output = ()>> {
    use tokio::signal::unix::{signal, SignalKind};

    let mut terminate = signal(SignalKind::terminate()).context("listening for SIGTERM")?;
    Ok(async move {
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                if let Err(error) = result {
                    tracing::error!(%error, "Failed to listen for Ctrl-C");
                }
            }
            _ = terminate.recv() => {}
        }
        tracing::info!("Shutdown signal received");
    })
}

#[cfg(not(unix))]
fn shutdown_signal() -> Result<impl std::future::Future<Output = ()>> {
    Ok(async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::error!(%error, "Failed to listen for Ctrl-C");
        }
        tracing::info!("Shutdown signal received");
    })
}

fn init_server_tracing() {
    let filter = EnvFilter::from_default_env().add_directive(tracing::Level::INFO.into());
    let stdout_layer = fmt::layer().with_ansi(true);

    let log_dir = paths::logs_dir();
    if let Err(error) = std::fs::create_dir_all(&log_dir) {
        eprintln!("Failed to create server log directory: {}", error);
        tracing_subscriber::registry()
            .with(filter)
            .with(stdout_layer)
            .init();
        return;
    }

    match std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_dir.join("mitsuro-serve.log"))
    {
        Ok(log_file) => {
            let file_layer = fmt::layer()
                .with_ansi(false)
                .with_writer(std::sync::Mutex::new(log_file));
            tracing_subscriber::registry()
                .with(filter)
                .with(stdout_layer)
                .with(file_layer)
                .init();
        }
        Err(error) => {
            eprintln!("Failed to open server log file: {}", error);
            tracing_subscriber::registry()
                .with(filter)
                .with(stdout_layer)
                .init();
        }
    }
}

fn reserve_server_listener(
    port: u16,
    desktop_managed: bool,
) -> Result<(u16, tokio::net::TcpListener)> {
    let address = server_bind_address(desktop_managed);
    if desktop_managed {
        let listener = bind_listener(address, port)
            .with_context(|| format!("Failed to bind desktop server to 127.0.0.1:{port}."))?;
        return Ok((port, listener));
    }

    if allows_port_fallback(port, desktop_managed) {
        return reserve_listener_with_fallback(port, DEFAULT_PORT_SEARCH_SPAN);
    }

    let listener = bind_listener(address, port)
        .with_context(|| format!("Failed to bind server port {}.", port))?;
    Ok((port, listener))
}

fn server_bind_address(desktop_managed: bool) -> Ipv4Addr {
    if desktop_managed {
        Ipv4Addr::LOCALHOST
    } else {
        Ipv4Addr::UNSPECIFIED
    }
}

fn allows_port_fallback(port: u16, desktop_managed: bool) -> bool {
    !desktop_managed && port == DEFAULT_SERVER_PORT
}

fn reserve_listener_with_fallback(
    start_port: u16,
    search_span: u16,
) -> Result<(u16, tokio::net::TcpListener)> {
    let mut reserved_listener = None;
    let selected_port =
        find_available_port(start_port, search_span, |candidate| {
            match bind_listener(Ipv4Addr::UNSPECIFIED, candidate) {
                Ok(listener) => {
                    reserved_listener = Some(listener);
                    Ok(true)
                }
                Err(err) if err.kind() == io::ErrorKind::AddrInUse => Ok(false),
                Err(err) => Err(err)
                    .with_context(|| format!("Failed while probing server port {}.", candidate)),
            }
        })?;

    let listener = reserved_listener.expect("selected port must have a reserved listener");
    Ok((selected_port, listener))
}

fn bind_listener(address: Ipv4Addr, port: u16) -> io::Result<tokio::net::TcpListener> {
    let listener = StdTcpListener::bind((address, port))?;
    listener.set_nonblocking(true)?;
    tokio::net::TcpListener::from_std(listener)
}

fn find_available_port<F>(start_port: u16, search_span: u16, mut probe: F) -> Result<u16>
where
    F: FnMut(u16) -> Result<bool>,
{
    let end_port = start_port.saturating_add(search_span);

    for offset in 0..=search_span {
        let Some(candidate) = start_port.checked_add(offset) else {
            break;
        };

        if probe(candidate)? {
            return Ok(candidate);
        }
    }

    anyhow::bail!(
        "No available server port found in the {}-{} range.",
        start_port,
        end_port
    );
}

fn print_tailscale_status(setup: TailscaleServeSetup) {
    match setup {
        TailscaleServeSetup::Configured { url } => {
            println!("  Tailscale: {}\n", url);
        }
        TailscaleServeSetup::PermissionDenied { url, detail } => {
            println!("  Tailscale URL: {}", url);
            println!("  Tailscale setup skipped: permission denied for `tailscale serve`.");
            println!("  Fix once: sudo tailscale set --operator=$USER");
            println!("  Then rerun: mitsuro serve");
            println!("  Details: {}\n", detail);
        }
        TailscaleServeSetup::NotInstalled => {
            println!("  Tip: Install Tailscale to access Mitsuro from any device.");
            println!("       https://tailscale.com/download\n");
        }
        TailscaleServeSetup::Offline => {
            println!("  Tailscale installed, but this device is offline.\n");
        }
        TailscaleServeSetup::Failed { detail } => {
            println!("  Tailscale setup failed: {}\n", detail);
        }
    }
}

fn print_banner(port: u16, starting: bool) {
    println!();
    println!(
        "  \x1b[1;37mMitsuro\x1b[0m server {}",
        if starting { "starting" } else { "running" }
    );
    println!("  ─────────────────────────────────────");
    println!("  Local:  http://localhost:{}", port);
}

/// Interactive CLI setup wizard for first-time configuration.
fn run_setup_wizard() -> Result<()> {
    println!();
    println!("  \x1b[1;37mMitsuro\x1b[0m — First-time setup");
    println!("  ─────────────────────────────────────");
    println!();

    let providers = ProviderId::all();
    println!("  Select a provider:");
    for (i, provider) in providers.iter().enumerate() {
        let marker = if i == 0 { " (default)" } else { "" };
        println!("    {}. {}{}", i + 1, provider, marker);
    }
    println!();

    print!("  Choice [1]: ");
    io::stdout().flush()?;

    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    let input = input.trim();

    let provider = if input.is_empty() {
        providers[0]
    } else {
        let idx: usize = input.parse().context("Invalid number")?;
        if idx == 0 || idx > providers.len() {
            anyhow::bail!("Invalid choice: {}", idx);
        }
        providers[idx - 1]
    };

    println!();
    print!("  API key for {}: ", provider);
    io::stdout().flush()?;

    let mut api_key = String::new();
    io::stdin().read_line(&mut api_key)?;
    let api_key = api_key.trim().to_string();

    if api_key.is_empty() {
        anyhow::bail!("API key cannot be empty");
    }

    let mut store = CredentialStore::load().unwrap_or_default();
    store.set(provider, api_key);
    store.save().context("Failed to save credentials")?;

    println!();
    println!("  \x1b[32m✓\x1b[0m Credentials saved for {}", provider);
    println!();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{allows_port_fallback, find_available_port, server_bind_address};

    #[test]
    fn falls_forward_when_preferred_port_is_busy() {
        let selected =
            find_available_port(3000, 5, |port| Ok(port == 3002)).expect("find fallback port");

        assert_eq!(selected, 3002);
    }

    #[test]
    fn explicit_non_default_port_does_not_fallback() {
        let err = find_available_port(4242, 0, |_| Ok(false))
            .expect_err("non-default explicit port should fail when occupied");

        assert!(err.to_string().contains("4242-4242"));
    }

    #[test]
    fn desktop_managed_bind_policy_is_loopback_only() {
        assert!(server_bind_address(true).is_loopback());
        assert!(server_bind_address(false).is_unspecified());
    }

    #[test]
    fn desktop_managed_bind_policy_never_falls_forward() {
        assert!(!allows_port_fallback(3000, true));
        assert!(!allows_port_fallback(4242, true));
        assert!(allows_port_fallback(3000, false));
        assert!(!allows_port_fallback(4242, false));
    }
}

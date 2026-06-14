mod buffer;
mod config;
mod log_entry;
mod metrics;
mod mqtt;
mod orchestrator;
mod sources;

use anyhow::{Context, Result};
use config::{Config, DEFAULT_CONFIG_PATH};
use std::path::PathBuf;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialise tracing (RUST_LOG controls verbosity; default = info).
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_ansi(false) // ANSI codes in syslog/journald produce unreadable log bodies
        .init();

    // Config path: first CLI argument or the default.
    let config_path: PathBuf = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(DEFAULT_CONFIG_PATH));

    let cfg = Config::load(&config_path)
        .with_context(|| format!("failed to load config from {}", config_path.display()))?;

    // I1: intentionally NOT logging ingest_key — it is a secret.
    info!(
        "spotflowd starting — device_id={} broker={}:{}",
        cfg.device.id, cfg.mqtt.broker, cfg.mqtt.port
    );

    // Shutdown channel: broadcast `true` when a signal is received.
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    // Start the persistent MQTT connection (event loop spawned internally).
    let publisher = mqtt::start(
        &cfg.device.id,
        &cfg.device.ingest_key,
        &cfg.mqtt,
        shutdown_rx.clone(),
    )
    .context("failed to start MQTT client")?;

    // Signal handler task — listens for SIGTERM and SIGINT.
    tokio::spawn(async move {
        wait_for_shutdown_signal().await;
        info!("shutdown signal received");
        let _ = shutdown_tx.send(true);
    });

    // Run the orchestrator (blocks until shutdown).
    orchestrator::run(cfg, publisher, shutdown_rx).await?;

    info!("spotflowd stopped");
    Ok(())
}

async fn wait_for_shutdown_signal() {
    use tokio::signal::unix::{signal, SignalKind};

    let mut sigterm = signal(SignalKind::terminate()).expect("failed to register SIGTERM handler");
    let mut sigint = signal(SignalKind::interrupt()).expect("failed to register SIGINT handler");

    tokio::select! {
        _ = sigterm.recv() => {}
        _ = sigint.recv() => {}
    }
}

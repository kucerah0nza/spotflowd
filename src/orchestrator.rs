//! Orchestrator — wires sources → buffer → MQTT publisher.
//!
//! Responsibilities:
//! - Start log source tasks (journald, syslog).
//! - Receive log entries from sources via an mpsc channel.
//! - Write entries into the two-tier buffer.
//! - Run the publish loop: drain memory first (newest), then disk chunks
//!   newest-first, when MQTT is connected.
//! - On shutdown: flush in-memory buffer to disk before exiting.

use crate::buffer::Buffer;
use crate::config::Config;
use crate::log_entry::LogEntry;
use crate::mqtt::MqttPublisher;
use anyhow::Result;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tokio::time::{sleep, Duration};
use tracing::{error, info, warn};

/// How many entries to publish in a single MQTT message.
const PUBLISH_BATCH_SIZE: usize = 200;
/// How long to wait between publish loop iterations when there is nothing to send.
const IDLE_SLEEP_MS: u64 = 200;

pub async fn run(cfg: Config, publisher: MqttPublisher, shutdown: tokio::sync::watch::Receiver<bool>) -> Result<()> {
    let (tx, mut rx) = mpsc::channel::<LogEntry>(4096);

    // -----------------------------------------------------------------------
    // Start sources
    // -----------------------------------------------------------------------

    #[cfg(feature = "journald")]
    if cfg.sources.journald {
        let tx2 = tx.clone();
        tokio::spawn(async move {
            if let Err(e) = crate::sources::journald::run(tx2).await {
                error!("journald source failed: {e}");
            }
        });
    }

    if cfg.sources.syslog {
        let tx2 = tx.clone();
        let path = cfg.sources.syslog_path.clone();
        tokio::spawn(async move {
            if let Err(e) = crate::sources::syslog::run(path, tx2).await {
                error!("syslog source failed: {e}");
            }
        });
    }

    // Drop the original sender so the channel closes when all source tasks end.
    drop(tx);

    // -----------------------------------------------------------------------
    // Buffer (shared between ingestion task and publish loop)
    // -----------------------------------------------------------------------
    let buffer = Arc::new(Mutex::new(Buffer::new(cfg.buffer.clone())));

    // -----------------------------------------------------------------------
    // Ingestion task: drain the mpsc channel into the buffer
    // -----------------------------------------------------------------------
    {
        let buffer = buffer.clone();
        let mut shutdown_rx = shutdown.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    entry = rx.recv() => {
                        match entry {
                            Some(e) => {
                                let mut buf = buffer.lock().await;
                                if let Err(err) = buf.push(e) {
                                    error!("buffer push error: {err}");
                                }
                            }
                            None => break, // all sources closed
                        }
                    }
                    _ = shutdown_rx.changed() => break,
                }
            }
        });
    }

    // -----------------------------------------------------------------------
    // Publish loop
    // -----------------------------------------------------------------------
    let mut shutdown_rx = shutdown.clone();

    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => {
                info!("shutdown signal received, flushing buffer to disk...");
                let mut buf = buffer.lock().await;
                if let Err(e) = buf.flush_memory_to_disk() {
                    warn!("flush on shutdown failed: {e}");
                }
                info!("buffer flushed, exiting");
                break;
            }
            _ = publish_tick(&publisher, &buffer) => {}
        }
    }

    Ok(())
}

/// One iteration of the publish loop.
async fn publish_tick(publisher: &MqttPublisher, buffer: &Arc<Mutex<Buffer>>) {
    if !publisher.is_connected() {
        sleep(Duration::from_millis(IDLE_SLEEP_MS)).await;
        return;
    }

    // --- Memory first (newest data) ---
    let memory_entries = {
        let mut buf = buffer.lock().await;
        if buf.memory_len() > 0 {
            buf.drain_memory()
        } else {
            vec![]
        }
    };

    if !memory_entries.is_empty() {
        // Publish in batches of PUBLISH_BATCH_SIZE.
        for chunk in memory_entries.chunks(PUBLISH_BATCH_SIZE) {
            if let Err(e) = publisher.publish_batch(chunk).await {
                warn!("publish error (memory batch): {e}");
                // Re-insert failed entries back into the buffer.
                let mut buf = buffer.lock().await;
                for entry in chunk {
                    let _ = buf.push(entry.clone());
                }
                sleep(Duration::from_millis(IDLE_SLEEP_MS)).await;
                return;
            }
        }
        return; // Prioritise memory; check again next tick before touching disk.
    }

    // --- Disk next (older data, newest chunk first) ---
    let next_chunk = {
        let buf = buffer.lock().await;
        match buf.next_disk_chunk() {
            Ok(p) => p,
            Err(e) => {
                warn!("error reading spool directory: {e}");
                None
            }
        }
    };

    if let Some(chunk_path) = next_chunk {
        match Buffer::read_chunk(&chunk_path) {
            Ok(entries) => {
                let mut publish_ok = true;
                for chunk in entries.chunks(PUBLISH_BATCH_SIZE) {
                    if let Err(e) = publisher.publish_batch(chunk).await {
                        warn!("publish error (disk chunk): {e}");
                        publish_ok = false;
                        break;
                    }
                }
                if publish_ok {
                    if let Err(e) = Buffer::delete_chunk(&chunk_path) {
                        warn!("failed to delete published chunk: {e}");
                    }
                }
            }
            Err(e) => {
                warn!("failed to read chunk {}: {e} — deleting corrupt chunk", chunk_path.display());
                let _ = Buffer::delete_chunk(&chunk_path);
            }
        }
        return;
    }

    // Nothing to publish — idle.
    sleep(Duration::from_millis(IDLE_SLEEP_MS)).await;
}

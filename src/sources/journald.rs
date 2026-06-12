//! journald log source.
//!
//! Follows the system journal from the current tail, converting each entry
//! to a `LogEntry` and sending it to the shared channel.
//! Uses `spawn_blocking` because the systemd journal API is synchronous.

use crate::log_entry::{LogEntry, Severity};
use anyhow::Result;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, warn};

pub async fn run(tx: mpsc::Sender<LogEntry>, shutdown: Arc<AtomicBool>) -> Result<()> {
    tokio::task::spawn_blocking(move || run_blocking(tx, shutdown)).await??;
    Ok(())
}

fn run_blocking(tx: mpsc::Sender<LogEntry>, shutdown: Arc<AtomicBool>) -> Result<()> {
    use systemd::journal::{JournalSeek, OpenOptions};

    let mut journal = OpenOptions::default()
        .system(true)
        .current_user(false)
        .open()
        .map_err(|e| anyhow::anyhow!("failed to open journald: {e}"))?;

    journal
        .seek(JournalSeek::Tail)
        .map_err(|e| anyhow::anyhow!("journal seek failed: {e}"))?;

    loop {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }
        match journal.next_entry() {
            Ok(Some(entry)) => {
                if let Some(log_entry) = entry_to_log(&entry) {
                    debug!("journald entry: {:?}", log_entry.body);
                    if tx.blocking_send(log_entry).is_err() {
                        break;
                    }
                }
            }
            Ok(None) => {
                let _ = journal.wait(Some(Duration::from_millis(200)));
            }
            Err(e) => {
                warn!("journald read error: {e}");
                std::thread::sleep(Duration::from_secs(1));
            }
        }
    }

    Ok(())
}

fn entry_to_log(entry: &std::collections::BTreeMap<String, String>) -> Option<LogEntry> {
    let body = entry.get("MESSAGE")?.clone();
    if body.is_empty() {
        return None;
    }

    let severity = entry
        .get("PRIORITY")
        .and_then(|p| p.parse::<u8>().ok())
        .map(Severity::from_syslog_priority)
        .unwrap_or(Severity::Info);

    // __REALTIME_TIMESTAMP is microseconds since Unix epoch.
    let timestamp_ms = entry
        .get("__REALTIME_TIMESTAMP")
        .and_then(|v| v.parse::<u64>().ok())
        .map(|us| us / 1000);

    // __MONOTONIC_TIMESTAMP is microseconds since boot.
    let uptime_ms = entry
        .get("__MONOTONIC_TIMESTAMP")
        .and_then(|v| v.parse::<u64>().ok())
        .map(|us| us / 1000);

    Some(LogEntry {
        body,
        severity,
        timestamp_ms,
        uptime_ms,
        source: "journald".to_string(),
    })
}

//! journald log source.
//!
//! Follows the system journal from the current tail, converting each entry
//! to a `LogEntry` and sending it to the shared channel.
//! Uses `spawn_blocking` because the systemd journal API is synchronous.

use super::strip_ansi;
use crate::log_entry::{LabelValue, LogEntry, Severity};
use anyhow::Result;
use std::collections::HashMap;
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
    let body = strip_ansi(entry.get("MESSAGE")?);
    if body.is_empty() {
        return None;
    }

    let severity = entry
        .get("PRIORITY")
        .and_then(|p| p.parse::<u8>().ok())
        .map(Severity::from_syslog_priority);

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

    let mut labels: HashMap<String, LabelValue> = HashMap::new();
    labels.insert("source".into(), LabelValue::Str("journald".into()));

    if let Some(h) = entry.get("_HOSTNAME") {
        labels.insert("hostname".into(), LabelValue::Str(h.clone()));
    }
    // Prefer SYSLOG_IDENTIFIER (e.g. "kernel"), fall back to _COMM (executable name).
    let process = entry
        .get("SYSLOG_IDENTIFIER")
        .or_else(|| entry.get("_COMM"))
        .cloned();
    if let Some(p) = process {
        labels.insert("process".into(), LabelValue::Str(p));
    }
    if let Some(pid) = entry.get("_PID").and_then(|v| v.parse::<i64>().ok()) {
        labels.insert("pid".into(), LabelValue::Int(pid));
    }
    if let Some(unit) = entry.get("_SYSTEMD_UNIT") {
        labels.insert("unit".into(), LabelValue::Str(unit.clone()));
    }

    Some(LogEntry {
        body,
        severity,
        timestamp_ms,
        uptime_ms,
        source: "journald".to_string(),
        labels,
    })
}

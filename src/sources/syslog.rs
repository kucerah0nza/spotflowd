//! Syslog file-tail source.
//!
//! Opens the configured syslog file, seeks to the end, then tails new lines.
//! Handles log rotation: detects when the file is replaced (inode change) and
//! reopens it.
//!
//! Parses both RFC 3164 and RFC 5424 syslog formats. Falls back to treating
//! the whole line as the message body if parsing fails.

use crate::log_entry::{LogEntry, Severity};
use anyhow::Result;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

pub async fn run(path: PathBuf, tx: mpsc::Sender<LogEntry>, shutdown: Arc<AtomicBool>) -> Result<()> {
    tokio::task::spawn_blocking(move || run_blocking(path, tx, shutdown)).await??;
    Ok(())
}

fn run_blocking(path: PathBuf, tx: mpsc::Sender<LogEntry>, shutdown: Arc<AtomicBool>) -> Result<()> {
    let file = open_and_seek_end(&path)?;
    info!("syslog source tailing {}", path.display());
    let mut reader = BufReader::new(file);
    let mut current_inode = inode_of(&path);

    loop {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }
        let mut line = String::new();
        match reader.read_line(&mut line) {
            Ok(0) => {
                std::thread::sleep(Duration::from_millis(200));
                let new_inode = inode_of(&path);
                if new_inode != current_inode {
                    debug!("syslog file rotated, reopening {}", path.display());
                    match open_at_start(&path) {
                        Ok(f) => {
                            reader = BufReader::new(f);
                            current_inode = new_inode;
                        }
                        Err(e) => warn!("failed to reopen syslog after rotation: {e}"),
                    }
                }
            }
            Ok(_) => {
                let trimmed = line.trim_end_matches('\n').trim_end_matches('\r');
                if !trimmed.is_empty() {
                    debug!("syslog entry: {}", trimmed);
                    let entry = parse_line(trimmed);
                    if tx.blocking_send(entry).is_err() {
                        break;
                    }
                }
            }
            Err(e) => {
                warn!("syslog read error: {e}");
                std::thread::sleep(Duration::from_secs(1));
            }
        }
    }
    Ok(())
}

fn open_and_seek_end(path: &PathBuf) -> Result<std::fs::File> {
    let mut f = std::fs::File::open(path)
        .map_err(|e| anyhow::anyhow!("cannot open syslog file {}: {e}", path.display()))?;
    f.seek(SeekFrom::End(0))?;
    Ok(f)
}

fn open_at_start(path: &PathBuf) -> Result<std::fs::File> {
    let f = std::fs::File::open(path)
        .map_err(|e| anyhow::anyhow!("cannot open syslog file {}: {e}", path.display()))?;
    Ok(f)
}

fn inode_of(path: &PathBuf) -> Option<u64> {
    use std::os::unix::fs::MetadataExt;
    std::fs::metadata(path).ok().map(|m| m.ino())
}

// ---------------------------------------------------------------------------
// Syslog line parsing
// ---------------------------------------------------------------------------

fn parse_line(line: &str) -> LogEntry {
    // Try RFC 5424 first: `<PRI>1 TIMESTAMP HOSTNAME APP-NAME PROCID MSGID SD MSG`
    if let Some(entry) = try_rfc5424(line) {
        return entry;
    }
    // Try RFC 3164: `<PRI>TIMESTAMP HOSTNAME TAG: MSG`
    if let Some(entry) = try_rfc3164(line) {
        return entry;
    }
    // Fallback: treat whole line as message body.
    LogEntry {
        body: line.to_string(),
        severity: Severity::Info,
        timestamp_ms: None,
        uptime_ms: None,
        source: "syslog".to_string(),
    }
}

fn parse_priority(pri: &str) -> (Severity, u8) {
    let n: u8 = pri.parse().unwrap_or(13); // default INFO
    let severity_code = n & 0x07;
    (Severity::from_syslog_priority(severity_code), n)
}

fn try_rfc5424(line: &str) -> Option<LogEntry> {
    // Format: <PRI>VERSION TIMESTAMP HOSTNAME APP PROCID MSGID SD MSG
    if !line.starts_with('<') {
        return None;
    }
    let end = line.find('>')?;
    let pri_str = &line[1..end];
    let rest = &line[end + 1..];

    // Version field must be "1"
    if !rest.starts_with('1') {
        return None;
    }
    let rest = rest[1..].trim_start();

    let parts: Vec<&str> = rest.splitn(7, ' ').collect();
    if parts.len() < 7 {
        return None;
    }

    let (severity, _) = parse_priority(pri_str);
    let timestamp_ms = parse_iso8601_ms(parts[0]);
    let body = parts[6].trim_start_matches("BOM").to_string();

    Some(LogEntry {
        body,
        severity,
        timestamp_ms,
        uptime_ms: None,
        source: "syslog".to_string(),
    })
}

fn try_rfc3164(line: &str) -> Option<LogEntry> {
    // Format: <PRI>Mon DD HH:MM:SS hostname tag[pid]: message
    if !line.starts_with('<') {
        return None;
    }
    let end = line.find('>')?;
    let pri_str = &line[1..end];
    let rest = &line[end + 1..];

    // Skip "Mon DD HH:MM:SS hostname tag: " prefix to get the message.
    // We take everything after the 4th space as a reasonable approximation.
    let mut spaces = 0;
    let msg_start = rest
        .char_indices()
        .find(|(_, c)| {
            if *c == ' ' {
                spaces += 1;
            }
            spaces == 4
        })
        .map(|(i, _)| i + 1)
        .unwrap_or(0);

    let body = rest[msg_start..].to_string();
    if body.is_empty() {
        return None;
    }

    let (severity, _) = parse_priority(pri_str);

    Some(LogEntry {
        body,
        severity,
        timestamp_ms: None, // RFC 3164 timestamps have no year; skip for now
        uptime_ms: None,
        source: "syslog".to_string(),
    })
}

fn parse_iso8601_ms(s: &str) -> Option<u64> {
    // Parse RFC 3339 / ISO 8601 timestamp to Unix milliseconds.
    // Expected format: 2024-01-15T10:30:00.000Z or 2024-01-15T10:30:00Z
    chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|dt| dt.timestamp_millis() as u64)
}

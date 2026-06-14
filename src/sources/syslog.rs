//! Syslog file-tail source.
//!
//! Opens the configured syslog file, seeks to the end, then tails new lines.
//! Handles log rotation: detects when the file is replaced (inode change) and
//! reopens it.
//!
//! Parses both RFC 3164 and RFC 5424 syslog formats. Falls back to treating
//! the whole line as the message body if parsing fails.

use super::strip_ansi;
use crate::log_entry::{LabelValue, LogEntry, Severity};
use std::collections::HashMap;
use anyhow::Result;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::path::{Path, PathBuf};
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

fn open_and_seek_end(path: &Path) -> Result<std::fs::File> {
    let mut f = std::fs::File::open(path)
        .map_err(|e| anyhow::anyhow!("cannot open syslog file {}: {e}", path.display()))?;
    f.seek(SeekFrom::End(0))?;
    Ok(f)
}

fn open_at_start(path: &Path) -> Result<std::fs::File> {
    let f = std::fs::File::open(path)
        .map_err(|e| anyhow::anyhow!("cannot open syslog file {}: {e}", path.display()))?;
    Ok(f)
}

fn inode_of(path: &Path) -> Option<u64> {
    use std::os::unix::fs::MetadataExt;
    std::fs::metadata(path).ok().map(|m| m.ino())
}

// ---------------------------------------------------------------------------
// Syslog line parsing
// ---------------------------------------------------------------------------

fn base_labels() -> HashMap<String, LabelValue> {
    let mut m = HashMap::new();
    m.insert("source".into(), LabelValue::Str("syslog".into()));
    m
}

fn parse_line(line: &str) -> LogEntry {
    let mut entry = try_rfc5424(line)
        .or_else(|| try_rfc3164(line))
        .or_else(|| try_rsyslog_default(line))
        .unwrap_or_else(|| LogEntry {
            body: line.to_string(),
            severity: None,
            timestamp_ms: None,
            uptime_ms: None,
            source: "syslog".to_string(),
            labels: base_labels(),
        });

    // Strip ANSI escape codes from body (e.g. from tracing-subscriber colour output).
    entry.body = strip_ansi(&entry.body);

    // When the syslog format carries no PRI (rsyslog default, fallback), try to
    // infer severity from a tracing-formatted body: "TIMESTAMP  LEVEL target: msg".
    if entry.severity.is_none() {
        entry.severity = infer_tracing_severity(&entry.body);
    }

    entry
}

/// Try to infer severity from a `tracing`-formatted log body, e.g.:
///   `2026-06-13T19:26:54.351Z  WARN spotflowd::mqtt: message`
/// Returns None if the pattern is not recognised.
fn infer_tracing_severity(body: &str) -> Option<Severity> {
    let mut it = body.split_ascii_whitespace();
    // First token must look like an ISO 8601 / RFC 3339 timestamp.
    let ts = it.next()?;
    if !ts.contains('T') || !ts.contains(':') {
        return None;
    }
    match it.next()? {
        "ERROR" => Some(Severity::Error),
        "WARN"  => Some(Severity::Warning),
        "INFO"  => Some(Severity::Info),
        "DEBUG" => Some(Severity::Debug),
        "TRACE" => Some(Severity::Debug),
        _       => None,
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
    let severity = Some(severity);
    let timestamp_ms = parse_iso8601_ms(parts[0]);
    // B2: strip the actual UTF-8 BOM character (U+FEFF), not the text "BOM".
    let body = parts[6].trim_start_matches('\u{FEFF}').to_string();

    let mut labels = base_labels();
    // parts: [TIMESTAMP, HOSTNAME, APP-NAME, PROCID, MSGID, SD, MSG]
    if parts[1] != "-" {
        labels.insert("hostname".into(), LabelValue::Str(parts[1].to_string()));
    }
    if parts[2] != "-" {
        labels.insert("process".into(), LabelValue::Str(parts[2].to_string()));
    }
    if let Ok(pid) = parts[3].parse::<i64>() {
        labels.insert("pid".into(), LabelValue::Int(pid));
    }

    Some(LogEntry {
        body,
        severity,
        timestamp_ms,
        uptime_ms: None,
        source: "syslog".to_string(),
        labels,
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

    let (severity, _) = parse_priority(pri_str);
    let severity = Some(severity);

    // Extract hostname (4th token) and tag (5th token) for labels.
    // B3: use split_ascii_whitespace() so space-padded single-digit days
    // ("Jun  1") don't insert an empty token that shifts everything right.
    let mut labels = base_labels();
    let tokens: Vec<&str> = rest.split_ascii_whitespace().collect();
    // tokens: [MON, DD, HH:MM:SS, HOSTNAME, TAG[PID]:, MSG...]
    if tokens.len() > 3 {
        labels.insert("hostname".into(), LabelValue::Str(tokens[3].to_string()));
    }
    let (process, pid) = if tokens.len() > 4 {
        let tag = tokens[4].trim_end_matches(':');
        parse_tag(tag)
    } else {
        (String::new(), None)
    };
    if !process.is_empty() {
        labels.insert("process".into(), LabelValue::Str(process));
    }
    if let Some(p) = pid {
        labels.insert("pid".into(), LabelValue::Int(p));
    }

    // B3: strip "TAG[PID]: " prefix so body contains only the actual message.
    let tag_and_msg = &rest[msg_start..];
    let body = match tag_and_msg.find(": ") {
        Some(pos) => tag_and_msg[pos + 2..].to_string(),
        None => tag_and_msg.to_string(),
    };
    if body.is_empty() {
        return None;
    }

    Some(LogEntry {
        body,
        severity,
        timestamp_ms: None, // RFC 3164 timestamps have no year; skip for now
        uptime_ms: None,
        source: "syslog".to_string(),
        labels,
    })
}

/// Parse a syslog tag like `myapp[1234]` into (process_name, optional_pid).
fn parse_tag(tag: &str) -> (String, Option<i64>) {
    if let Some(bracket) = tag.find('[') {
        let process = tag[..bracket].to_string();
        let pid = tag[bracket + 1..]
            .trim_end_matches(']')
            .parse::<i64>()
            .ok();
        (process, pid)
    } else {
        (tag.to_string(), None)
    }
}

/// Try rsyslog default file format (no `<PRI>` prefix):
///   `RFC3339_TIMESTAMP HOSTNAME TAG[PID]: MESSAGE`
/// e.g. `2026-06-12T15:26:45.123456+02:00 debian myapp[999]: hello`
fn try_rsyslog_default(line: &str) -> Option<LogEntry> {
    // Must not start with '<' (those are handled by RFC parsers).
    if line.starts_with('<') {
        return None;
    }
    // Split into at most 4 parts: TIMESTAMP HOSTNAME TAG_PID: MESSAGE
    let mut parts = line.splitn(4, ' ');
    let timestamp_str = parts.next()?;
    let hostname = parts.next()?;
    let tag_part = parts.next()?; // e.g. "myapp[999]:" or "myapp:"
    let message = parts.next().unwrap_or("").trim_start();

    // I2: require a valid ISO 8601 timestamp to reject non-syslog lines.
    let timestamp_ms = parse_iso8601_ms(timestamp_str)?;

    // I3: require a non-empty message body.
    if message.is_empty() {
        return None;
    }
    let body = message.to_string();

    // Strip trailing colon from tag.
    let tag = tag_part.trim_end_matches(':');
    let (process, pid) = parse_tag(tag);

    let mut labels = base_labels();
    labels.insert("hostname".into(), LabelValue::Str(hostname.to_string()));
    if !process.is_empty() {
        labels.insert("process".into(), LabelValue::Str(process));
    }
    if let Some(p) = pid {
        labels.insert("pid".into(), LabelValue::Int(p));
    }

    Some(LogEntry {
        body,
        severity: None, // rsyslog default format carries no priority
        timestamp_ms: Some(timestamp_ms),
        uptime_ms: None,
        source: "syslog".to_string(),
        labels,
    })
}

fn parse_iso8601_ms(s: &str) -> Option<u64> {
    // Parse RFC 3339 / ISO 8601 timestamp to Unix milliseconds.
    // M3: use try_from to avoid wrapping pre-epoch timestamps to huge u64 values.
    chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .and_then(|dt| u64::try_from(dt.timestamp_millis()).ok())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn label_str(entry: &LogEntry, key: &str) -> Option<String> {
        match entry.labels.get(key) {
            Some(LabelValue::Str(s)) => Some(s.clone()),
            _ => None,
        }
    }

    fn label_int(entry: &LogEntry, key: &str) -> Option<i64> {
        match entry.labels.get(key) {
            Some(LabelValue::Int(i)) => Some(*i),
            _ => None,
        }
    }

    // --- parse_tag ---

    #[test]
    fn parse_tag_with_pid() {
        assert_eq!(parse_tag("myapp[1234]"), ("myapp".into(), Some(1234)));
    }

    #[test]
    fn parse_tag_without_pid() {
        assert_eq!(parse_tag("kernel"), ("kernel".into(), None));
    }

    #[test]
    fn parse_tag_empty() {
        assert_eq!(parse_tag(""), ("".into(), None));
    }

    // --- RFC 5424 ---

    #[test]
    fn rfc5424_full_line() {
        let line = "<34>1 2026-06-12T15:30:00Z myhost myapp 1234 - - test message";
        let e = try_rfc5424(line).unwrap();
        assert_eq!(e.body, "test message");
        // PRI=34: facility=4, severity=2 (Critical)
        assert_eq!(e.severity, Some(Severity::Critical));
        assert_eq!(label_str(&e, "hostname").as_deref(), Some("myhost"));
        assert_eq!(label_str(&e, "process").as_deref(), Some("myapp"));
        assert_eq!(label_int(&e, "pid"), Some(1234));
        assert!(e.timestamp_ms.is_some());
    }

    #[test]
    fn rfc5424_nil_fields_omitted_from_labels() {
        let line = "<13>1 2026-06-12T10:00:00Z - - - - - hello";
        let e = try_rfc5424(line).unwrap();
        assert_eq!(e.body, "hello");
        assert!(!e.labels.contains_key("hostname"));
        assert!(!e.labels.contains_key("process"));
    }

    #[test]
    fn rfc5424_rejects_non_pri_line() {
        assert!(try_rfc5424("plain text").is_none());
    }

    // --- RFC 3164 ---

    #[test]
    fn rfc3164_body_excludes_tag() {
        // B3: body must not include "TAG[PID]: "
        let line = "<13>Jun 12 15:30:00 myhost myapp[999]: actual message";
        let e = try_rfc3164(line).unwrap();
        assert_eq!(e.body, "actual message");
        assert_eq!(label_str(&e, "hostname").as_deref(), Some("myhost"));
        assert_eq!(label_str(&e, "process").as_deref(), Some("myapp"));
        assert_eq!(label_int(&e, "pid"), Some(999));
    }

    #[test]
    fn rfc3164_tag_without_pid() {
        let line = "<13>Jun 12 15:30:00 myhost kernel: oops";
        let e = try_rfc3164(line).unwrap();
        assert_eq!(e.body, "oops");
        assert_eq!(label_str(&e, "process").as_deref(), Some("kernel"));
        assert!(label_int(&e, "pid").is_none());
    }

    // --- rsyslog default ---

    #[test]
    fn rsyslog_default_parsed() {
        let line = "2026-06-12T15:26:45+02:00 debian myapp[999]: hello world";
        let e = try_rsyslog_default(line).unwrap();
        assert_eq!(e.body, "hello world");
        assert!(e.timestamp_ms.is_some());
        assert_eq!(label_str(&e, "hostname").as_deref(), Some("debian"));
        assert_eq!(label_str(&e, "process").as_deref(), Some("myapp"));
        assert_eq!(label_int(&e, "pid"), Some(999));
        assert_eq!(e.severity, None);
    }

    #[test]
    fn rsyslog_default_rejects_invalid_timestamp() {
        // I2: non-ISO8601 first token → None
        assert!(try_rsyslog_default("not-a-ts hostname app: msg").is_none());
    }

    #[test]
    fn rsyslog_default_rejects_empty_message() {
        // I3: no message after tag → None
        let line = "2026-06-12T15:26:45+02:00 debian myapp:";
        assert!(try_rsyslog_default(line).is_none());
    }

    #[test]
    fn rsyslog_default_ignores_pri_lines() {
        assert!(try_rsyslog_default("<13>Jun 12 15:30:00 host tag: msg").is_none());
    }

    // --- parse_line fallback ---

    #[test]
    fn fallback_uses_whole_line_as_body() {
        let e = parse_line("some unrecognised log line here");
        assert_eq!(e.body, "some unrecognised log line here");
        assert_eq!(e.severity, None);
        assert_eq!(label_str(&e, "source").as_deref(), Some("syslog"));
    }

    // --- ANSI stripping ---

    #[test]
    fn ansi_codes_stripped_from_body() {
        // Simulate a tracing-subscriber coloured line reaching syslog (real ESC bytes).
        let line = "2026-06-13T19:26:55+00:00 debian spotflowd[2016]: \
            \x1b[2m2026-06-13T19:26:54Z\x1b[0m \x1b[33m WARN\x1b[0m \
            \x1b[2mspotflowd::mqtt\x1b[0m\x1b[2m:\x1b[0m MQTT connection lost";
        let e = parse_line(line);
        assert!(!e.body.contains('\x1b'), "body must not contain ESC");
        assert!(!e.body.contains("[33m"), "body must not contain raw ANSI params");
        assert!(e.body.contains("MQTT connection lost"));
    }

    #[test]
    fn rsyslog_octal_escaped_ansi_stripped() {
        // rsyslog encodes ESC (0x1b) as the literal four chars "#033" in syslog files.
        let line = "2026-06-13T19:26:55+00:00 debian spotflowd[2016]: \
            #033[2m2026-06-13T19:26:54Z#033[0m #033[33m WARN#033[0m \
            #033[2mspotflowd::mqtt#033[0m#033[2m:#033[0m MQTT connection lost";
        let e = parse_line(line);
        assert!(!e.body.contains("#033"), "body must not contain rsyslog-escaped ESC");
        assert!(e.body.contains("MQTT connection lost"));
    }

    #[test]
    fn tracing_severity_inferred_from_body() {
        // rsyslog default format carries no PRI; severity must be inferred.
        let line = "2026-06-13T19:26:55+00:00 debian spotflowd[2016]: \
            2026-06-13T19:26:54Z  WARN spotflowd::mqtt: connection lost";
        let e = parse_line(line);
        assert_eq!(e.severity, Some(Severity::Warning));
    }

    #[test]
    fn tracing_severity_error_inferred() {
        let line = "2026-06-13T19:26:55+00:00 debian spotflowd[2016]: \
            2026-06-13T19:26:54Z ERROR spotflowd::buf: write failed";
        let e = parse_line(line);
        assert_eq!(e.severity, Some(Severity::Error));
    }

    #[test]
    fn non_tracing_body_leaves_severity_none() {
        // rsyslog default line whose body is a plain message, not tracing format.
        let line = "2026-06-13T19:26:55+00:00 debian nginx[100]: GET / HTTP/1.1 200";
        let e = parse_line(line);
        assert_eq!(e.severity, None);
    }

    // --- B2: BOM stripping ---

    #[test]
    fn rfc5424_utf8_bom_stripped() {
        // B2: the real UTF-8 BOM (U+FEFF) must be removed from the body.
        let line = "<13>1 2026-06-12T10:00:00Z - - - - - \u{FEFF}hello";
        let e = try_rfc5424(line).unwrap();
        assert_eq!(e.body, "hello");
    }

    #[test]
    fn rfc5424_text_bom_not_stripped() {
        // B2: a message that literally starts with "BOM" must not be truncated.
        let line = "<13>1 2026-06-12T10:00:00Z - - - - - BOMber command";
        let e = try_rfc5424(line).unwrap();
        assert_eq!(e.body, "BOMber command");
    }

    // --- B3: RFC 3164 space-padded single-digit days ---

    #[test]
    fn rfc3164_space_padded_day_labels_correct() {
        // B3: "Jun  1" (two spaces) must not shift hostname/process/pid tokens.
        let line = "<13>Jun  1 10:00:00 myhost myapp[999]: actual message";
        let e = try_rfc3164(line).unwrap();
        assert_eq!(e.body, "actual message");
        assert_eq!(label_str(&e, "hostname").as_deref(), Some("myhost"));
        assert_eq!(label_str(&e, "process").as_deref(), Some("myapp"));
        assert_eq!(label_int(&e, "pid"), Some(999));
    }
}

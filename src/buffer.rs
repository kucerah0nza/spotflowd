//! Two-tier buffer: memory ring buffer → disk spool chunks.
//!
//! Write path:  log entries → memory buffer.
//!              When memory is full OR flush timer fires → serialize to a disk chunk.
//!
//! Read path:   memory buffer first (newest), then disk chunks newest-first.
//!              Disk chunks are deleted after successful publish.

use crate::config::BufferConfig;
use crate::log_entry::LogEntry;
use anyhow::{Context, Result};
use std::collections::VecDeque;
use std::fs;
use std::path::{Path, PathBuf};
use tracing::{debug, warn};

// ---------------------------------------------------------------------------
// Disk spool helpers
// ---------------------------------------------------------------------------

/// Returns all spool chunk paths sorted newest-first (highest sequence number first).
/// Only files with a purely numeric stem (e.g. `0000000001.cbor`) are included;
/// other `.cbor` files in the same directory (e.g. `metrics_seq.cbor`) are ignored.
fn spool_files_newest_first(dir: &Path) -> Result<Vec<PathBuf>> {
    if !dir.exists() {
        return Ok(vec![]);
    }
    let mut paths: Vec<PathBuf> = fs::read_dir(dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| is_chunk_file(p))
        .collect();
    // Chunk files are named with a zero-padded sequence number → lexicographic
    // sort descending gives newest-first.
    paths.sort_unstable_by(|a, b| b.file_name().cmp(&a.file_name()));
    Ok(paths)
}

fn total_spool_bytes(dir: &Path) -> u64 {
    if !dir.exists() {
        return 0;
    }
    fs::read_dir(dir)
        .ok()
        .map(|rd| {
            rd.filter_map(|e| e.ok())
                .filter_map(|e| e.metadata().ok())
                .map(|m| m.len())
                .sum()
        })
        .unwrap_or(0)
}

fn next_chunk_path(dir: &Path) -> Result<PathBuf> {
    // Use max existing sequence number + 1 so deleting published chunks never
    // causes a collision with chunks still on disk.
    let max_seq = if dir.exists() {
        fs::read_dir(dir)?
            .filter_map(|e| e.ok())
            .filter_map(|e| {
                let p = e.path();
                if p.extension().and_then(|s| s.to_str()) != Some("cbor") {
                    return None;
                }
                p.file_stem()
                    .and_then(|s| s.to_str())
                    .and_then(|s| s.parse::<u64>().ok())
            })
            .max()
            .unwrap_or(0)
    } else {
        0
    };
    Ok(dir.join(format!("{:010}.cbor", max_seq + 1)))
}

fn drop_oldest_chunk(dir: &Path) -> Result<()> {
    if !dir.exists() {
        return Ok(());
    }
    let mut paths: Vec<PathBuf> = fs::read_dir(dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| is_chunk_file(p))
        .collect();
    paths.sort_unstable_by(|a, b| a.file_name().cmp(&b.file_name()));
    if let Some(oldest) = paths.first() {
        warn!("disk spool full, dropping oldest chunk: {}", oldest.display());
        fs::remove_file(oldest)?;
    }
    Ok(())
}

/// Returns true only for files that are spool chunks: `.cbor` extension and a
/// purely numeric stem (e.g. `0000000001.cbor`). This excludes other `.cbor`
/// files in the same directory such as `metrics_seq.cbor`.
fn is_chunk_file(p: &Path) -> bool {
    p.extension().and_then(|s| s.to_str()) == Some("cbor")
        && p.file_stem()
            .and_then(|s| s.to_str())
            .map(|s| s.chars().all(|c| c.is_ascii_digit()))
            .unwrap_or(false)
}

// ---------------------------------------------------------------------------
// Buffer
// ---------------------------------------------------------------------------

pub struct Buffer {
    cfg: BufferConfig,
    memory: VecDeque<LogEntry>,
    /// Tracks whether disk chunks may exist, to avoid scanning the spool
    /// directory on every idle tick. Set when a chunk is written, cleared
    /// when `next_disk_chunk()` returns None.
    may_have_chunks: bool,
}

impl Buffer {
    pub fn new(cfg: BufferConfig) -> Self {
        // Check on startup whether leftover chunks exist from a previous run.
        let may_have_chunks = spool_files_newest_first(&cfg.disk_path)
            .map(|v| !v.is_empty())
            .unwrap_or(false);
        Self {
            cfg,
            memory: VecDeque::new(),
            may_have_chunks,
        }
    }

    /// Push a log entry into the memory buffer.
    /// When memory is full, flush the current contents to a disk chunk first.
    pub fn push(&mut self, entry: LogEntry) -> Result<()> {
        if self.memory.len() >= self.cfg.memory_max_entries {
            self.flush_memory_to_disk()?;
        }
        self.memory.push_back(entry);
        Ok(())
    }

    /// Flush all current in-memory entries to disk, split into chunks of
    /// `disk_chunk_max_entries`. Called on memory overflow and graceful shutdown.
    pub fn flush_memory_to_disk(&mut self) -> Result<()> {
        if self.memory.is_empty() {
            return Ok(());
        }
        let entries: Vec<LogEntry> = self.memory.drain(..).collect();
        for chunk in entries.chunks(self.cfg.disk_chunk_max_entries) {
            self.write_chunk(chunk)?;
        }
        Ok(())
    }

    /// Write a slice of entries as a single CBOR chunk file on disk.
    /// Uses atomic write-then-rename so a crash mid-write never leaves a
    /// partially-written file that would be mistaken for a corrupt chunk.
    fn write_chunk(&mut self, entries: &[LogEntry]) -> Result<()> {
        let dir = &self.cfg.disk_path;
        fs::create_dir_all(dir).with_context(|| format!("create spool dir {}", dir.display()))?;

        // Enforce max spool size: drop oldest chunks until under the limit.
        let max_bytes = self.cfg.disk_max_size_mb * 1024 * 1024;
        while total_spool_bytes(dir) > max_bytes {
            drop_oldest_chunk(dir)?;
            // If dir is empty after dropping, stop.
            if spool_files_newest_first(dir)?.is_empty() {
                break;
            }
        }

        let path = next_chunk_path(dir)?;
        let mut buf = Vec::new();
        ciborium::into_writer(entries, &mut buf)
            .with_context(|| "CBOR serialization of chunk failed")?;
        // B4: atomic write — write to .tmp then rename so a crash mid-write
        // never leaves a partially-written chunk file.
        let tmp = path.with_extension("cbor.tmp");
        fs::write(&tmp, &buf)
            .with_context(|| format!("write chunk tmp {}", tmp.display()))?;
        fs::rename(&tmp, &path)
            .with_context(|| format!("rename chunk {} → {}", tmp.display(), path.display()))?;
        self.may_have_chunks = true;
        debug!("flushed {} entries to disk chunk {}", entries.len(), path.display());
        Ok(())
    }

    // ---------------------------------------------------------------------------
    // Read path (used by the MQTT publisher)
    // ---------------------------------------------------------------------------

    /// Drain all in-memory entries (newest batch — returned as a Vec so the
    /// caller can publish and, on success, simply discard).
    pub fn drain_memory(&mut self) -> Vec<LogEntry> {
        self.memory.drain(..).collect()
    }

    /// Return the path to the newest disk chunk that has not yet been published,
    /// or `None` if the spool is empty.  Skips the directory scan entirely when
    /// no chunks are expected (reduces I/O on flash / SD cards).
    pub fn next_disk_chunk(&mut self) -> Result<Option<PathBuf>> {
        if !self.may_have_chunks {
            return Ok(None);
        }
        let files = spool_files_newest_first(&self.cfg.disk_path)?;
        let next = files.into_iter().next();
        if next.is_none() {
            self.may_have_chunks = false;
        }
        Ok(next)
    }

    /// Load and deserialize a chunk from disk.
    pub fn read_chunk(path: &Path) -> Result<Vec<LogEntry>> {
        let data = fs::read(path).with_context(|| format!("read chunk {}", path.display()))?;
        let entries: Vec<LogEntry> = ciborium::from_reader(data.as_slice())
            .with_context(|| format!("CBOR decode of chunk {}", path.display()))?;
        Ok(entries)
    }

    /// Delete a chunk after it has been successfully published.
    pub fn delete_chunk(path: &Path) -> Result<()> {
        fs::remove_file(path).with_context(|| format!("delete chunk {}", path.display()))?;
        debug!("deleted published chunk {}", path.display());
        Ok(())
    }

    pub fn memory_len(&self) -> usize {
        self.memory.len()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log_entry::{LogEntry, Severity};
    use std::collections::HashMap;
    use tempfile::TempDir;

    fn make_cfg(dir: &TempDir, memory_max: usize, chunk_max: usize) -> BufferConfig {
        BufferConfig {
            memory_max_entries: memory_max,
            disk_path: dir.path().to_path_buf(),
            disk_max_size_mb: 64,
            disk_chunk_max_entries: chunk_max,
        }
    }

    fn entry(body: &str) -> LogEntry {
        LogEntry {
            body: body.to_string(),
            severity: None,
            timestamp_ms: None,
            uptime_ms: None,
            source: "test".to_string(),
            labels: HashMap::new(),
        }
    }

    /// B1: sequence number must not reuse a number that was deleted.
    #[test]
    fn chunk_path_no_collision_after_delete() {
        let dir = TempDir::new().unwrap();
        let p = dir.path();
        fs::write(p.join("0000000001.cbor"), b"x").unwrap();
        fs::write(p.join("0000000002.cbor"), b"x").unwrap();
        fs::remove_file(p.join("0000000001.cbor")).unwrap();
        // Highest remaining = 2, so next must be 3.
        let next = next_chunk_path(p).unwrap();
        assert_eq!(next.file_name().unwrap(), "0000000003.cbor");
    }

    /// B2: flush must honour disk_chunk_max_entries.
    #[test]
    fn flush_splits_into_chunks() {
        let dir = TempDir::new().unwrap();
        // memory_max=100 so no auto-flush triggers; chunk_max=3
        let cfg = make_cfg(&dir, 100, 3);
        let mut buf = Buffer::new(cfg);
        for i in 0..7 {
            buf.memory.push_back(entry(&format!("msg{i}")));
        }
        buf.flush_memory_to_disk().unwrap();
        assert_eq!(buf.memory_len(), 0);

        let chunks = spool_files_newest_first(dir.path()).unwrap();
        // ceil(7/3) = 3 chunks
        assert_eq!(chunks.len(), 3);
        let c1 = Buffer::read_chunk(&dir.path().join("0000000001.cbor")).unwrap();
        assert_eq!(c1.len(), 3);
        let c2 = Buffer::read_chunk(&dir.path().join("0000000002.cbor")).unwrap();
        assert_eq!(c2.len(), 3);
        let c3 = Buffer::read_chunk(&dir.path().join("0000000003.cbor")).unwrap();
        assert_eq!(c3.len(), 1);
    }

    /// Memory overflow triggers a flush and new entries continue to accumulate.
    #[test]
    fn memory_overflow_flushes_to_disk() {
        let dir = TempDir::new().unwrap();
        let cfg = make_cfg(&dir, 3, 10);
        let mut buf = Buffer::new(cfg);
        buf.push(entry("a")).unwrap();
        buf.push(entry("b")).unwrap();
        buf.push(entry("c")).unwrap();
        // len == memory_max == 3; next push triggers flush
        buf.push(entry("d")).unwrap();

        assert_eq!(buf.memory_len(), 1); // only "d" in memory
        let chunk = buf.next_disk_chunk().unwrap().unwrap();
        let entries = Buffer::read_chunk(&chunk).unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].body, "a");
        assert_eq!(entries[2].body, "c");
    }

    /// Drain memory returns all entries and leaves memory empty.
    #[test]
    fn drain_memory_empties_buffer() {
        let dir = TempDir::new().unwrap();
        let cfg = make_cfg(&dir, 100, 10);
        let mut buf = Buffer::new(cfg);
        buf.push(entry("x")).unwrap();
        buf.push(entry("y")).unwrap();
        let drained = buf.drain_memory();
        assert_eq!(drained.len(), 2);
        assert_eq!(buf.memory_len(), 0);
    }

    /// Severity is correctly round-tripped through the disk spool.
    #[test]
    fn chunk_round_trips_severity() {
        let dir = TempDir::new().unwrap();
        let cfg = make_cfg(&dir, 100, 10);
        let mut buf = Buffer::new(cfg);
        let mut e = entry("hello");
        e.severity = Some(Severity::Error);
        buf.push(e).unwrap();
        buf.flush_memory_to_disk().unwrap();
        let chunk = buf.next_disk_chunk().unwrap().unwrap();
        let entries = Buffer::read_chunk(&chunk).unwrap();
        assert_eq!(entries[0].severity, Some(Severity::Error));
    }
}

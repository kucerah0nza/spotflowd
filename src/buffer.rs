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
fn spool_files_newest_first(dir: &Path) -> Result<Vec<PathBuf>> {
    if !dir.exists() {
        return Ok(vec![]);
    }
    let mut paths: Vec<PathBuf> = fs::read_dir(dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("cbor"))
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
    // Sequence number = number of existing .cbor files + 1, zero-padded to 10 digits.
    let count = if dir.exists() {
        fs::read_dir(dir)?
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("cbor"))
            .count()
    } else {
        0
    };
    Ok(dir.join(format!("{:010}.cbor", count + 1)))
}

fn drop_oldest_chunk(dir: &Path) -> Result<()> {
    if !dir.exists() {
        return Ok(());
    }
    let mut paths: Vec<PathBuf> = fs::read_dir(dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("cbor"))
        .collect();
    paths.sort_unstable_by(|a, b| a.file_name().cmp(&b.file_name()));
    if let Some(oldest) = paths.first() {
        warn!("disk spool full, dropping oldest chunk: {}", oldest.display());
        fs::remove_file(oldest)?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Buffer
// ---------------------------------------------------------------------------

pub struct Buffer {
    cfg: BufferConfig,
    memory: VecDeque<LogEntry>,
}

impl Buffer {
    pub fn new(cfg: BufferConfig) -> Self {
        Self {
            cfg,
            memory: VecDeque::new(),
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

    /// Flush all current in-memory entries to a disk chunk.
    /// Called on: memory overflow, flush timer, graceful shutdown.
    pub fn flush_memory_to_disk(&mut self) -> Result<()> {
        if self.memory.is_empty() {
            return Ok(());
        }
        let entries: Vec<LogEntry> = self.memory.drain(..).collect();
        self.write_chunk(&entries)?;
        Ok(())
    }

    /// Write a slice of entries as a single CBOR chunk file on disk.
    fn write_chunk(&self, entries: &[LogEntry]) -> Result<()> {
        let dir = &self.cfg.disk_path;
        fs::create_dir_all(dir).with_context(|| format!("create spool dir {}", dir.display()))?;

        // Enforce max spool size: drop oldest chunks until under the limit.
        let max_bytes = self.cfg.disk_max_size_mb * 1024 * 1024;
        while total_spool_bytes(dir) >= max_bytes {
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
        fs::write(&path, &buf)
            .with_context(|| format!("write chunk {}", path.display()))?;
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
    /// or `None` if the spool is empty.
    pub fn next_disk_chunk(&self) -> Result<Option<PathBuf>> {
        let files = spool_files_newest_first(&self.cfg.disk_path)?;
        Ok(files.into_iter().next())
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

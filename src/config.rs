use anyhow::{Context, Result};
use serde::Deserialize;
use std::path::{Path, PathBuf};

pub const DEFAULT_CONFIG_PATH: &str = "/etc/spotflow/spotflowd.toml";

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub device: DeviceConfig,
    pub mqtt: MqttConfig,
    pub sources: SourcesConfig,
    pub buffer: BufferConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DeviceConfig {
    pub id: String,
    pub ingest_key: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MqttConfig {
    #[serde(default = "default_broker")]
    pub broker: String,
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default = "default_keepalive_secs")]
    pub keepalive_secs: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SourcesConfig {
    #[allow(dead_code)] // read only when the `journald` feature is enabled
    #[serde(default = "default_true")]
    pub journald: bool,
    #[serde(default = "default_true")]
    pub syslog: bool,
    #[serde(default = "default_syslog_path")]
    pub syslog_path: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BufferConfig {
    /// Maximum number of log entries held in memory before flushing to disk.
    #[serde(default = "default_memory_max_entries")]
    pub memory_max_entries: usize,

    /// Directory for on-disk spool chunks.
    #[serde(default = "default_disk_path")]
    pub disk_path: PathBuf,

    /// Maximum total size of the disk spool in megabytes.
    #[serde(default = "default_disk_max_size_mb")]
    pub disk_max_size_mb: u64,

    /// Number of log entries per disk chunk file.
    #[serde(default = "default_disk_chunk_max_entries")]
    pub disk_chunk_max_entries: usize,
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read config file: {}", path.display()))?;
        let config: Config =
            toml::from_str(&content).with_context(|| "failed to parse config file")?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<()> {
        if self.device.id.is_empty() {
            anyhow::bail!("device.id must not be empty");
        }
        if self.device.ingest_key.is_empty() {
            anyhow::bail!("device.ingest_key must not be empty");
        }
        if self.buffer.memory_max_entries == 0 {
            anyhow::bail!("buffer.memory_max_entries must be > 0");
        }
        if self.buffer.disk_chunk_max_entries == 0 {
            anyhow::bail!("buffer.disk_chunk_max_entries must be > 0");
        }
        Ok(())
    }
}

fn default_broker() -> String {
    "mqtt.spotflow.io".to_string()
}
fn default_port() -> u16 {
    8883
}
fn default_keepalive_secs() -> u64 {
    60
}
fn default_true() -> bool {
    true
}
fn default_syslog_path() -> PathBuf {
    // Try /var/log/syslog first (Debian/Ubuntu); fall back to /var/log/messages (RHEL/Yocto).
    let debian = Path::new("/var/log/syslog");
    if debian.exists() {
        debian.to_path_buf()
    } else {
        PathBuf::from("/var/log/messages")
    }
}
fn default_memory_max_entries() -> usize {
    1000
}
fn default_disk_path() -> PathBuf {
    PathBuf::from("/var/lib/spotflow/spool")
}
fn default_disk_max_size_mb() -> u64 {
    64
}
fn default_disk_chunk_max_entries() -> usize {
    200
}

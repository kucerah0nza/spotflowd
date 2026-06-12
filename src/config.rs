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
    #[serde(default)]
    pub metrics: MetricsConfig,
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
pub struct MetricsConfig {
    /// Set to true to enable the metrics subsystem.
    #[serde(default)]
    pub enabled: bool,
    /// How often to read /proc and /sys (seconds).
    #[serde(default = "default_collection_interval")]
    pub collection_interval_secs: u64,
    /// Upload window: "none" | "1m" | "1h" | "1d".
    /// "none" → publish each raw sample immediately (no aggregation).
    /// "1m"   → accumulate samples for one minute, then publish sum/count/min/max.
    #[serde(default = "default_aggregation_interval")]
    pub aggregation_interval: String,
    #[serde(default)]
    pub groups: MetricsGroupsConfig,
    #[serde(default)]
    pub disk: MetricsDiskConfig,
    #[serde(default)]
    pub network: MetricsNetworkConfig,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            collection_interval_secs: default_collection_interval(),
            aggregation_interval: default_aggregation_interval(),
            groups: MetricsGroupsConfig::default(),
            disk: MetricsDiskConfig::default(),
            network: MetricsNetworkConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct MetricsGroupsConfig {
    #[serde(default = "default_true")]
    pub cpu: bool,
    #[serde(default = "default_true")]
    pub memory: bool,
    #[serde(default = "default_true")]
    pub disk: bool,
    #[serde(default = "default_true")]
    pub network: bool,
    #[serde(default = "default_true")]
    pub system: bool,
}

impl Default for MetricsGroupsConfig {
    fn default() -> Self {
        Self { cpu: true, memory: true, disk: true, network: true, system: true }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct MetricsDiskConfig {
    /// Mount points to report disk space for. Defaults to root only.
    #[serde(default = "default_mount_points")]
    pub mount_points: Vec<String>,
}

impl Default for MetricsDiskConfig {
    fn default() -> Self {
        Self { mount_points: default_mount_points() }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct MetricsNetworkConfig {
    /// Interfaces to report. Empty list = auto-detect all non-loopback interfaces.
    #[serde(default)]
    pub interfaces: Vec<String>,
}

impl Default for MetricsNetworkConfig {
    fn default() -> Self {
        Self { interfaces: vec![] }
    }
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
    #[cfg_attr(not(feature = "journald"), allow(dead_code))]
    #[serde(default = "default_true")]
    pub journald: bool,
    #[serde(default = "default_syslog")]
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
        if self.buffer.disk_max_size_mb == 0 {
            anyhow::bail!("buffer.disk_max_size_mb must be > 0");
        }
        if self.metrics.enabled {
            if self.metrics.collection_interval_secs == 0 {
                anyhow::bail!("metrics.collection_interval_secs must be > 0");
            }
            if !matches!(self.metrics.aggregation_interval.as_str(), "none" | "1m" | "1h" | "1d") {
                anyhow::bail!("metrics.aggregation_interval must be one of: none, 1m, 1h, 1d");
            }
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
fn default_syslog() -> bool {
    // On systemd systems the journald source already captures everything rsyslog writes.
    // Default to disabled to avoid duplicate entries. Users on non-systemd targets
    // (Yocto, no journald feature) get syslog enabled by default.
    cfg!(not(feature = "journald"))
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
fn default_collection_interval() -> u64 {
    10
}
fn default_aggregation_interval() -> String {
    "1m".to_string()
}
fn default_mount_points() -> Vec<String> {
    vec!["/".to_string()]
}

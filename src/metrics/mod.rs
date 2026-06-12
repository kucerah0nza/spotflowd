//! Metrics subsystem — collects OS metrics and publishes them to Spotflow.
//!
//! Architecture:
//!   Collector (reads /proc, /sys every `collection_interval_secs`)
//!     → Aggregator (accumulates sum/count/min/max per stream per window)
//!       → Publisher (CBOR-encodes and sends via MQTT)
//!
//! Aggregation intervals mirror the Spotflow / Zephyr SDK:
//!   "none" → each sample published immediately (raw, sum only)
//!   "1m"   → one message per stream per minute with sum/count/min/max
//!   "1h"   → one message per stream per hour
//!   "1d"   → one message per stream per day
//!
//! Sequence numbers are persisted across restarts in
//!   `<buffer.disk_path>/metrics_seq.cbor`
//! using an atomic write (write-then-rename) to prevent corruption.

pub mod aggregator;
pub mod collector;
pub mod publisher;

use crate::config::MetricsConfig;
use crate::mqtt::MqttPublisher;
use anyhow::Result;
use std::path::PathBuf;
use tokio::sync::watch;
use tokio::time::{interval, Duration, MissedTickBehavior};
use tracing::{debug, warn};

// ---------------------------------------------------------------------------
// CBOR aggregation-interval codes (Spotflow spec, key 22)
// ---------------------------------------------------------------------------
pub const AGG_NONE: u8 = 0;
pub const AGG_1MIN: u8 = 1;
pub const AGG_1HOUR: u8 = 3;
pub const AGG_1DAY: u8 = 4;

// ---------------------------------------------------------------------------
// Shared types
// ---------------------------------------------------------------------------

/// A raw value from the OS.
#[derive(Debug, Clone, Copy)]
pub enum MetricValue {
    Int(i64),
    Float(f64),
}

impl MetricValue {
    pub fn as_f64(self) -> f64 {
        match self {
            MetricValue::Int(i) => i as f64,
            MetricValue::Float(f) => f,
        }
    }
}

/// A single sample produced by the collector on each tick.
#[derive(Debug, Clone)]
pub struct MetricSample {
    pub name: &'static str,
    pub value: MetricValue,
    /// Sorted by key for consistent stream-key generation.
    pub labels: Vec<(&'static str, String)>,
}

/// A metric ready to publish — either raw (agg=none) or fully aggregated.
#[derive(Debug)]
pub struct ReadyMetric {
    pub name: &'static str,
    pub agg_cbor: u8,
    pub labels: Vec<(&'static str, String)>,
    /// Always present (raw value for NONE, aggregated sum otherwise).
    pub sum: f64,
    /// None for AGG_NONE — omit count/min/max from payload per spec.
    pub count: Option<u64>,
    pub min: Option<f64>,
    pub max: Option<f64>,
    pub seq: u64,
    pub uptime_ms: u64,
}

// ---------------------------------------------------------------------------
// Main task
// ---------------------------------------------------------------------------

pub async fn run(
    cfg: MetricsConfig,
    seq_dir: PathBuf,
    publisher: MqttPublisher,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let mut coll = collector::Collector::new();
    let mut agg = aggregator::Aggregator::new(&cfg, seq_dir)?;
    let mut ticker = interval(Duration::from_secs(cfg.collection_interval_secs));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let (samples, uptime_ms) = coll.collect(&cfg);
                let ready = agg.ingest(samples, uptime_ms);
                if !ready.is_empty() {
                    if !publisher.is_connected() {
                        debug!("metrics: MQTT not connected, dropping {} readings", ready.len());
                        continue;
                    }
                    match publisher::publish_metrics(&publisher, &ready).await {
                        Ok(()) => {
                            debug!("published {} metric readings", ready.len());
                            if let Err(e) = agg.save_seq() {
                                warn!("failed to save metric sequence numbers: {e}");
                            }
                        }
                        Err(e) => warn!("metrics publish error: {e}"),
                    }
                }
            }
            _ = shutdown.changed() => break,
        }
    }
    Ok(())
}

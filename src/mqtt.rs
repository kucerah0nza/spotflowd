//! Persistent MQTT client for the Spotflow platform.
//!
//! Maintains a single TLS connection to `mqtt.spotflow.io:8883`.
//! Publishes CBOR-encoded log batches on `ingest-cbor` with QoS 0.
//! rumqttc handles reconnection internally; we track connection state via
//! events so the orchestrator knows whether to drain the buffer or hold.

use crate::config::MqttConfig;
use crate::log_entry::LogEntry;
use anyhow::{Context, Result};
use ciborium::value::Value as CborValue;
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS, TlsConfiguration, Transport};
use rustls::ClientConfig;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, warn};

const INGEST_TOPIC: &str = "ingest-cbor";

// ---------------------------------------------------------------------------
// CBOR encoding
// ---------------------------------------------------------------------------

/// Encode a single log entry as a CBOR map per the Spotflow spec:
///   1 = body, 4 = severity, 6 = deviceUptimeMs, 7 = deviceTimestampMs
/// One MQTT message is published per entry.
fn encode_entry(entry: &LogEntry) -> Result<Vec<u8>> {
    let mut map = vec![
        (CborValue::Integer(1u64.into()), CborValue::Text(entry.body.clone())),
        (
            CborValue::Integer(4u64.into()),
            CborValue::Integer((entry.severity as u64).into()),
        ),
    ];
    if let Some(uptime) = entry.uptime_ms {
        map.push((
            CborValue::Integer(6u64.into()),
            CborValue::Integer(uptime.into()),
        ));
    }
    if let Some(ts) = entry.timestamp_ms {
        map.push((
            CborValue::Integer(7u64.into()),
            CborValue::Integer(ts.into()),
        ));
    }
    let mut buf = Vec::new();
    ciborium::into_writer(&CborValue::Map(map), &mut buf).context("CBOR encode failed")?;
    Ok(buf)
}

// ---------------------------------------------------------------------------
// Publisher handle
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct MqttPublisher {
    client: AsyncClient,
    pub connected: Arc<AtomicBool>,
}

impl MqttPublisher {
    pub fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Relaxed)
    }

    /// Publish a slice of entries — one MQTT message per entry (QoS 0).
    pub async fn publish_batch(&self, entries: &[LogEntry]) -> Result<()> {
        for entry in entries {
            let payload = encode_entry(entry)?;
            self.client
                .publish(INGEST_TOPIC, QoS::AtMostOnce, false, payload)
                .await
                .context("MQTT publish failed")?;
        }
        if !entries.is_empty() {
            debug!("published {} log entries", entries.len());
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Connection setup + event loop
// ---------------------------------------------------------------------------

fn build_client_config() -> Arc<ClientConfig> {
    let mut root_store = rustls::RootCertStore::empty();
    for cert in rustls_native_certs::load_native_certs().unwrap_or_default() {
        root_store.add(cert).ok(); // skip any malformed certs silently
    }
    let config = ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();
    Arc::new(config)
}

/// Create the MQTT client + spawn the event loop task.
/// Returns a `MqttPublisher` handle the orchestrator uses to publish.
pub fn start(
    device_id: &str,
    ingest_key: &str,
    cfg: &MqttConfig,
) -> Result<MqttPublisher> {
    let mut options = MqttOptions::new(device_id, &cfg.broker, cfg.port);
    options.set_credentials(device_id, ingest_key);
    options.set_keep_alive(Duration::from_secs(cfg.keepalive_secs));
    options.set_transport(Transport::tls_with_config(
        TlsConfiguration::Rustls(build_client_config()),
    ));

    let (client, mut eventloop) = AsyncClient::new(options, 64);

    let connected = Arc::new(AtomicBool::new(false));
    let connected_flag = connected.clone();

    // Event loop task — must be polled continuously for rumqttc to function.
    // rumqttc handles reconnection automatically; we just watch for state changes.
    tokio::spawn(async move {
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Packet::ConnAck(_))) => {
                    info!("MQTT connected to Spotflow platform");
                    connected_flag.store(true, Ordering::Relaxed);
                }
                Ok(_) => {}
                Err(e) => {
                    if connected_flag.load(Ordering::Relaxed) {
                        warn!("MQTT connection lost: {e}");
                        connected_flag.store(false, Ordering::Relaxed);
                    } else {
                        debug!("MQTT reconnecting: {e}");
                    }
                    // rumqttc will retry automatically; brief yield before next poll
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    });

    Ok(MqttPublisher { client, connected })
}

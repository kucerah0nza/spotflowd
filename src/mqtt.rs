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

/// Encode a batch of log entries as a CBOR array.
/// Each entry is a CBOR map using integer keys per the Spotflow spec:
///   1 = body, 4 = severity, 6 = deviceUptimeMs, 7 = deviceTimestampMs
pub fn encode_batch(entries: &[LogEntry]) -> Result<Vec<u8>> {
    let items: Vec<CborValue> = entries
        .iter()
        .map(|e| {
            let mut map = vec![
                (CborValue::Integer(1.into()), CborValue::Text(e.body.clone())),
                (
                    CborValue::Integer(4.into()),
                    CborValue::Integer((e.severity as u8 as i128).into()),
                ),
            ];
            if let Some(uptime) = e.uptime_ms {
                map.push((
                    CborValue::Integer(6.into()),
                    CborValue::Integer((uptime as i128).into()),
                ));
            }
            if let Some(ts) = e.timestamp_ms {
                map.push((
                    CborValue::Integer(7.into()),
                    CborValue::Integer((ts as i128).into()),
                ));
            }
            CborValue::Map(map)
        })
        .collect();

    let mut buf = Vec::new();
    ciborium::into_writer(&CborValue::Array(items), &mut buf).context("CBOR encode failed")?;
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

    /// Publish a batch (QoS 0 — fire and forget).
    pub async fn publish_batch(&self, entries: &[LogEntry]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let payload = encode_batch(entries)?;
        let n = entries.len();
        self.client
            .publish(INGEST_TOPIC, QoS::AtMostOnce, false, payload)
            .await
            .context("MQTT publish failed")?;
        debug!("published {n} log entries");
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Connection setup + event loop
// ---------------------------------------------------------------------------

fn build_client_config() -> Arc<ClientConfig> {
    let mut root_store = rustls::RootCertStore::empty();
    root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
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

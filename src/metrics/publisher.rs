//! CBOR encoding and MQTT publishing for metric messages.
//!
//! CBOR key assignments (Spotflow MQTT spec):
//!   0  = messageType (always 5 for metrics)
//!   5  = labels (map of tstr → tstr)
//!   6  = deviceUptimeMs
//!   13 = sequenceNumber
//!   21 = metricName
//!   22 = aggregationInterval  (0=none, 1=1m, 3=1h, 4=1d)
//!   24 = sum  (raw value for AGG_NONE, aggregated sum otherwise)
//!   26 = count (aggregated only)
//!   27 = min   (aggregated only)
//!   28 = max   (aggregated only)

use super::ReadyMetric;
use crate::mqtt::MqttPublisher;
use anyhow::{Context, Result};
use ciborium::value::Value as CborValue;

const MESSAGE_TYPE_METRIC: u64 = 5;

pub async fn publish_metrics(publisher: &MqttPublisher, metrics: &[ReadyMetric]) -> Result<()> {
    for m in metrics {
        let payload = encode_metric(m)?;
        publisher.publish_payload(payload).await?;
    }
    Ok(())
}

fn encode_metric(m: &ReadyMetric) -> Result<Vec<u8>> {
    let mut map = vec![
        (CborValue::Integer(0u64.into()), CborValue::Integer(MESSAGE_TYPE_METRIC.into())),
        (CborValue::Integer(21u64.into()), CborValue::Text(m.name.to_string())),
        (CborValue::Integer(22u64.into()), CborValue::Integer((m.agg_cbor as u64).into())),
    ];

    if !m.labels.is_empty() {
        let label_map: Vec<(CborValue, CborValue)> = m.labels
            .iter()
            .map(|(k, v)| (CborValue::Text(k.clone()), CborValue::Text(v.clone())))
            .collect();
        map.push((CborValue::Integer(5u64.into()), CborValue::Map(label_map)));
    }

    map.push((CborValue::Integer(6u64.into()), CborValue::Integer(m.uptime_ms.into())));
    map.push((CborValue::Integer(13u64.into()), CborValue::Integer(m.seq.into())));
    map.push((CborValue::Integer(24u64.into()), cbor_number(m.sum)));

    // count/min/max only for aggregated intervals (omitted for AGG_NONE per spec).
    if let Some(count) = m.count {
        map.push((CborValue::Integer(26u64.into()), CborValue::Integer(count.into())));
        map.push((CborValue::Integer(27u64.into()), cbor_number(m.min.unwrap_or(m.sum))));
        map.push((CborValue::Integer(28u64.into()), cbor_number(m.max.unwrap_or(m.sum))));
    }

    let mut buf = Vec::new();
    ciborium::into_writer(&CborValue::Map(map), &mut buf)
        .context("CBOR encode metric failed")?;
    Ok(buf)
}

/// Encode a float as an integer when it has no fractional part (smaller payload).
fn cbor_number(v: f64) -> CborValue {
    if v.is_finite() && v.fract() == 0.0 && v >= i64::MIN as f64 && v <= i64::MAX as f64 {
        CborValue::Integer((v as i64).into())
    } else {
        CborValue::Float(v)
    }
}

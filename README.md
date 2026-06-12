# spotflowd

Linux observability daemon for the [Spotflow](https://spotflow.io) platform.

Collects logs from `journald` and syslog, buffers them locally, and streams them to Spotflow over a persistent MQTT/TLS connection.

## How it works

```
journald ──┐
           ├──▶  memory buffer  ──▶  disk spool (on overflow)  ──▶  MQTT → Spotflow
syslog  ───┘
```

- **Memory-first buffer** — entries are held in RAM (configurable size) to minimise flash writes on embedded targets.
- **Disk spool** — when memory is full, entries are flushed to disk in chunks. Oldest chunks are dropped when the spool size limit is reached.
- **Publish order** — when connectivity is restored, the newest data is sent first (memory), then older backlog (disk, newest chunk first).
- **Persistent MQTT connection** — single TLS connection to `mqtt.spotflow.io:8883`; reconnects automatically on failure.

## Installation

### Debian / Ubuntu

**1. Install system dependencies**

```bash
sudo apt-get update && sudo apt-get install -y \
  curl build-essential pkg-config libsystemd-dev rsyslog
```

**2. Install Rust**

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source "$HOME/.cargo/env"
```

**3. Clone and build**

```bash
git clone https://github.com/kucerah0nza/spotflowd.git
cd spotflowd
cargo build --release
sudo cp target/release/spotflowd /usr/sbin/spotflowd
```

**4. Create config file**

```bash
sudo mkdir -p /etc/spotflow
sudo cp config/spotflowd.toml.example /etc/spotflow/spotflowd.toml
sudo nano /etc/spotflow/spotflowd.toml
```

Set `device.id` and `device.ingest_key` to the values from your Spotflow dashboard.

**5. Install and start the systemd service**

```bash
sudo useradd -r -s /bin/false spotflow
sudo mkdir -p /var/lib/spotflow/spool
sudo chown spotflow:spotflow /var/lib/spotflow/spool

sudo cp systemd/spotflowd.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now spotflowd
```

**6. Verify it is running**

```bash
sudo systemctl status spotflowd
sudo journalctl -u spotflowd -f
```

You should see `MQTT connected to Spotflow platform` in the logs.

**7. Send a test log entry**

```bash
logger "hello from spotflowd"
```

The message should appear in the Spotflow dashboard within seconds.

---

### Yocto (embedded Linux)

Build without the journald feature (for systems without systemd):

```bash
cargo build --release --no-default-features
```

A BitBake recipe will be provided in a future release.

---

### Manual run (testing, no systemd service)

```bash
# Run with debug logging, reading config from a custom path
sudo RUST_LOG=debug spotflowd /etc/spotflow/spotflowd.toml
```

`/var/log/syslog` requires read access — run as root or add your user to the `adm` group:

```bash
sudo usermod -aG adm $USER   # then log out and back in
```

## Configuration

Default config path: `/etc/spotflow/spotflowd.toml`

A custom path can be passed as the first CLI argument:

```bash
spotflowd /path/to/config.toml
```

See [`config/spotflowd.toml.example`](config/spotflowd.toml.example) for all options with descriptions.

### Minimal config

```toml
[device]
id = "my-device-001"
ingest_key = "sk_..."
```

All other settings have sensible defaults.

## Log verbosity

Control the daemon's own log output via `RUST_LOG`:

```bash
RUST_LOG=debug spotflowd       # verbose
RUST_LOG=warn  spotflowd       # quiet
```

## Features

| Feature    | Default | Description                       |
|------------|---------|-----------------------------------|
| `journald` | enabled | Collect logs from systemd journal |

Disable journald (e.g. for Yocto without systemd):

```bash
cargo build --release --no-default-features
```

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `MQTT connection lost` in logs | Wrong `ingest_key` or no internet | Check credentials and connectivity |
| `/var/log/syslog: permission denied` | Insufficient permissions | Run as root or add user to `adm` group |
| `failed to open journald` | Missing `libsystemd-dev` | `apt-get install libsystemd-dev` |
| Syslog file not found | `rsyslog` not installed | `apt-get install rsyslog` |
| No logs in Spotflow dashboard | Both sources disabled in config | Set `journald = true` or `syslog = true` |

## Roadmap

- [ ] Metrics (CPU, memory, custom gauges)
- [ ] Crash dump collection
- [ ] Remote log-level control via MQTT
- [ ] Snap package (Ubuntu Core)
- [ ] Yocto / BitBake recipe

## License

Business Source License 1.1 — see [LICENSE.MD](LICENSE.MD).
Converts to Apache 2.0 four years after first public release.
Contact [hello@spotflow.io](mailto:hello@spotflow.io) for alternative licensing.

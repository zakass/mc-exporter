# mc-exporter

A lightweight, zero-dependency Python Prometheus exporter and persistent activity tracker for containerized Minecraft Java Edition servers.

## Features

- **Player Counts & Join/Leave Names**: Tails `latest.log` (and backfills `YYYY-MM-DD-N.log.gz` archives on first run) to track live online players, per-player join/leave counters, last join/leave timestamps, and cumulative playtime—handling log rotations and server restarts without double-counting.
- **Server List Ping (SLP)**: Queries the Minecraft server port (`25565`) directly for health (`minecraft_status_healthy`), ping latency (`minecraft_status_response_time_seconds`), and online/max player counts.
- **Container Memory & CPU**: Polls the Docker Engine API over `/var/run/docker.sock` (compatible with Docker Desktop `overlayfs` and cgroups v2) for working set memory, total usage, RSS/anon, page cache, memory limit, and CPU usage, plus `hibernate_mc` JVM heap logs when present.
- **Persistent State**: Stores player counters, timestamps, playtime, log read offsets, and a chronological join/leave event log in SQLite (`/state/mc_exporter.db`) so metrics survive container rebuilds and server restarts.

## Endpoints

| Path | Description |
| --- | --- |
| `/metrics` | Prometheus metrics (`text/plain; version=0.0.4`) |
| `/players` | JSON summary of all known players and totals |
| `/events` | JSON list of persisted join/leave events (`?limit=100&player=...`) |
| `/healthz` | Health check (`{"status": "ok"}`) |

## Quick Start (Docker Compose)

```yaml
services:
  mc-exporter:
    build: ./mc-exporter
    image: mc-exporter:latest
    container_name: mc-exporter
    restart: unless-stopped
    ports:
      - "9100:9100"
    environment:
      MC_LOGS_DIR: /mc-logs
      MC_CONTAINER_NAME: minecraft
      MC_HOST: minecraft
      MC_PORT: "25565"
      DOCKER_SOCK: /var/run/docker.sock
      STATE_DB_PATH: /state/mc_exporter.db
      BACKFILL_LOGS: "true"
    volumes:
      - ./data/logs:/mc-logs:ro
      - /var/run/docker.sock:/var/run/docker.sock:ro
      - exporter_data:/state
```

## Configuration (Environment Variables)

| Variable | Default | Description |
| --- | --- | --- |
| `MC_LOGS_DIR` | `/mc-logs` | Directory containing `latest.log` and `*.log.gz` |
| `MC_CONTAINER_NAME` | `minecraft` | Docker container name to track |
| `MC_HOST` | `minecraft` | Minecraft host for Server List Ping |
| `MC_PORT` | `25565` | Minecraft port for Server List Ping |
| `DOCKER_SOCK` | `/var/run/docker.sock` | Path to Docker Engine Unix socket |
| `STATE_DB_PATH` | `/state/mc_exporter.db` | SQLite persistence file path |
| `LISTEN_HOST` | `0.0.0.0` | HTTP bind address |
| `LISTEN_PORT` | `9100` | HTTP listen port |
| `DOCKER_POLL_INTERVAL` | `5` | Seconds between Docker stats polls |
| `LOG_POLL_INTERVAL` | `1` | Seconds between `latest.log` polls |
| `SLP_POLL_INTERVAL` | `10` | Seconds between Minecraft SLP pings |
| `BACKFILL_LOGS` | `true` | Parse existing `*.log.gz` archives on first startup |

## Running Tests

```bash
python3 -B -m unittest discover -v
```

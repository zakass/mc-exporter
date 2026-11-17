#!/usr/bin/env python3
"""Prometheus exporter and persistent tracker for Minecraft server metrics.

Tracks:
- Player counts, currently online players, joining/leaving player names,
  join/leave counts, last seen timestamps, and playtime (via log tailing + SLP).
- Docker container memory & CPU usage for Minecraft and stack containers
  (via Docker Engine Unix socket API).
- Minecraft server health, ping latency, and status player counts (via SLP).

All player counters, timestamps, playtime, log offsets, and chronological
join/leave events persist in SQLite across container and server runs.
"""

from __future__ import annotations

import datetime as dt
import gzip
import hashlib
import http.client
import json
import logging
import os
import re
import signal
import socket
import sqlite3
import struct
import sys
import threading
import time
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
LOG = logging.getLogger("mc-exporter")

LOG_LINE_RE = re.compile(r"^\[(\d{2}):(\d{2}):(\d{2})\] \[([^\]]+)\]: (.*)$")
JOIN_RE = re.compile(
    r"^(?:System chat: )?([A-Za-z0-9_]{1,16})(?: \(formerly known as [A-Za-z0-9_]{1,16}\))? joined the game\s*$"
)
LEAVE_RE = re.compile(
    r"^(?:System chat: )?([A-Za-z0-9_]{1,16})(?: \(formerly known as [A-Za-z0-9_]{1,16}\))? left the game\s*$"
)
SERVER_START_RE = re.compile(r"^(?:Loading Minecraft |Starting minecraft server version )")
SERVER_STOP_RE = re.compile(r"^Stopping server\s*$")
HIBERNATE_MEM_RE = re.compile(r"^Memory:\s*(\d+)MB used\s*/\s*(\d+)MB max")
ARCHIVE_NAME_RE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})-(\d+)\.log\.gz$")

HEAD_HASH_BYTES = 4096


@dataclass(frozen=True)
class Config:
    logs_dir: Path
    mc_container_name: str
    mc_host: str
    mc_port: int
    docker_sock: str
    state_db_path: Path
    listen_host: str
    listen_port: int
    docker_poll_interval: float
    log_poll_interval: float
    slp_poll_interval: float
    backfill_logs: bool

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            logs_dir=Path(os.environ.get("MC_LOGS_DIR", "/mc-logs")),
            mc_container_name=os.environ.get("MC_CONTAINER_NAME", "minecraft"),
            mc_host=os.environ.get("MC_HOST", "minecraft"),
            mc_port=int(os.environ.get("MC_PORT", "25565")),
            docker_sock=os.environ.get("DOCKER_SOCK", "/var/run/docker.sock"),
            state_db_path=Path(os.environ.get("STATE_DB_PATH", "/state/mc_exporter.db")),
            listen_host=os.environ.get("LISTEN_HOST", "0.0.0.0"),
            listen_port=int(os.environ.get("LISTEN_PORT", "9100")),
            docker_poll_interval=float(os.environ.get("DOCKER_POLL_INTERVAL", "5")),
            log_poll_interval=float(os.environ.get("LOG_POLL_INTERVAL", "1")),
            slp_poll_interval=float(os.environ.get("SLP_POLL_INTERVAL", "10")),
            backfill_logs=os.environ.get("BACKFILL_LOGS", "true").lower() in ("1", "true", "yes"),
        )


def iso_utc(ts: float) -> str:
    return dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def escape_label(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def format_labels(labels: dict[str, str]) -> str:
    if not labels:
        return ""
    parts = [f'{k}="{escape_label(str(v))}"' for k, v in labels.items()]
    return "{" + ",".join(parts) + "}"


class UnixHTTPConnection(http.client.HTTPConnection):
    """HTTPConnection speaking over a local Unix domain socket."""

    def __init__(self, socket_path: str, timeout: float = 5.0) -> None:
        super().__init__("localhost", timeout=timeout)
        self.socket_path = socket_path

    def connect(self) -> None:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(self.timeout)
        sock.connect(self.socket_path)
        self.sock = sock


def encode_varint(value: int) -> bytes:
    """Encode a 32-bit integer as a Minecraft protocol VarInt."""
    value &= 0xFFFFFFFF
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value != 0:
            byte |= 0x80
        out.append(byte)
        if value == 0:
            break
    return bytes(out)


def read_varint_from_sock(sock: socket.socket) -> int:
    """Read a VarInt directly from a connected socket."""
    num_read = 0
    result = 0
    while True:
        chunk = sock.recv(1)
        if not chunk:
            raise EOFError("Unexpected EOF while reading VarInt")
        byte = chunk[0]
        result |= (byte & 0x7F) << (7 * num_read)
        num_read += 1
        if num_read > 5:
            raise ValueError("VarInt exceeds 5 bytes")
        if (byte & 0x80) == 0:
            return result


def read_varint_from_bytes(buf: bytes, offset: int) -> tuple[int, int]:
    """Decode a VarInt from a byte buffer, returning (value, next_offset)."""
    num_read = 0
    result = 0
    while True:
        if offset >= len(buf):
            raise EOFError("Unexpected EOF in buffer while reading VarInt")
        byte = buf[offset]
        offset += 1
        result |= (byte & 0x7F) << (7 * num_read)
        num_read += 1
        if num_read > 5:
            raise ValueError("VarInt exceeds 5 bytes")
        if (byte & 0x80) == 0:
            return result, offset


def recv_exact(sock: socket.socket, length: int) -> bytes:
    """Read exactly `length` bytes from a socket."""
    chunks: list[bytes] = []
    remaining = length
    while remaining > 0:
        data = sock.recv(remaining)
        if not data:
            raise EOFError("Unexpected EOF while reading packet payload")
        chunks.append(data)
        remaining -= len(data)
    return b"".join(chunks)


def query_minecraft_slp(host: str, port: int, timeout: float = 3.0) -> dict[str, Any]:
    """Perform a Minecraft Java Edition Server List Ping and return status JSON + latency."""
    host_bytes = host.encode("utf-8")
    handshake_payload = (
        encode_varint(0x00)
        + encode_varint(0)
        + encode_varint(len(host_bytes))
        + host_bytes
        + struct.pack(">H", port)
        + encode_varint(1)
    )
    handshake_packet = encode_varint(len(handshake_payload)) + handshake_payload
    status_request_packet = b"\x01\x00"

    start = time.monotonic()
    with socket.create_connection((host, port), timeout=timeout) as sock:
        sock.settimeout(timeout)
        sock.sendall(handshake_packet + status_request_packet)
        packet_len = read_varint_from_sock(sock)
        payload = recv_exact(sock, packet_len)
    elapsed = time.monotonic() - start

    packet_id, idx = read_varint_from_bytes(payload, 0)
    if packet_id != 0x00:
        raise ValueError(f"Unexpected SLP packet id: {packet_id}")
    json_len, idx = read_varint_from_bytes(payload, idx)
    json_bytes = payload[idx : idx + json_len]
    status = json.loads(json_bytes.decode("utf-8"))
    status["_latency_seconds"] = elapsed
    return status


class StateStore:
    """SQLite-backed persistent store for player metrics, events, and log ingestion offsets."""

    def __init__(self, db_path: Path) -> None:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self.db_path = db_path
        self.lock = threading.RLock()
        self.conn = sqlite3.connect(str(db_path), check_same_thread=False, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self._playtime_floor: dict[str, float] = {}
        self._init_schema()

    def _init_schema(self) -> None:
        with self.lock:
            self.conn.execute("PRAGMA journal_mode=WAL;")
            self.conn.execute("PRAGMA synchronous=NORMAL;")
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS players (
                    player TEXT PRIMARY KEY,
                    online INTEGER NOT NULL DEFAULT 0,
                    joins_total INTEGER NOT NULL DEFAULT 0,
                    leaves_total INTEGER NOT NULL DEFAULT 0,
                    last_join_ts REAL,
                    last_leave_ts REAL,
                    playtime_seconds_total REAL NOT NULL DEFAULT 0,
                    current_session_start_ts REAL
                );
                """
            )
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS player_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp REAL NOT NULL,
                    iso_time TEXT NOT NULL,
                    player TEXT NOT NULL,
                    event TEXT NOT NULL,
                    source TEXT NOT NULL
                );
                """
            )
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_player_events_ts ON player_events(timestamp DESC);"
            )
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_player_events_player ON player_events(player, timestamp DESC);"
            )
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS ingest_state (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                """
            )
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS processed_archives (
                    filename TEXT PRIMARY KEY,
                    processed_at REAL NOT NULL
                );
                """
            )

    def close(self) -> None:
        with self.lock:
            self.conn.close()

    def begin(self) -> None:
        self.conn.execute("BEGIN")

    def commit(self) -> None:
        self.conn.execute("COMMIT")

    def rollback(self) -> None:
        self.conn.execute("ROLLBACK")

    def get_state(self, key: str, default: str | None = None) -> str | None:
        with self.lock:
            row = self.conn.execute("SELECT value FROM ingest_state WHERE key = ?", (key,)).fetchone()
            return row["value"] if row else default

    def set_state(self, key: str, value: str) -> None:
        self.conn.execute(
            "INSERT INTO ingest_state(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )

    def is_archive_processed(self, filename: str) -> bool:
        with self.lock:
            row = self.conn.execute(
                "SELECT 1 FROM processed_archives WHERE filename = ?", (filename,)
            ).fetchone()
            return row is not None

    def mark_archive_processed(self, filename: str) -> None:
        self.conn.execute(
            "INSERT OR IGNORE INTO processed_archives(filename, processed_at) VALUES(?, ?)",
            (filename, time.time()),
        )

    def _ensure_player(self, player: str) -> sqlite3.Row:
        row = self.conn.execute("SELECT * FROM players WHERE player = ?", (player,)).fetchone()
        if row is None:
            self.conn.execute("INSERT INTO players(player) VALUES(?)", (player,))
            row = self.conn.execute("SELECT * FROM players WHERE player = ?", (player,)).fetchone()
        assert row is not None
        return row

    def record_join(self, player: str, ts: float, source: str) -> None:
        row = self._ensure_player(player)
        playtime = float(row["playtime_seconds_total"])
        if row["online"] and row["current_session_start_ts"] is not None:
            # Defensive: close unclosed session before starting new one without double-counting leave.
            playtime += max(0.0, ts - float(row["current_session_start_ts"]))
        last_join = max(float(row["last_join_ts"] or 0.0), ts)
        self.conn.execute(
            """
            UPDATE players
            SET online = 1,
                joins_total = joins_total + 1,
                last_join_ts = ?,
                playtime_seconds_total = ?,
                current_session_start_ts = ?
            WHERE player = ?
            """,
            (last_join, playtime, ts, player),
        )
        self.conn.execute(
            "INSERT INTO player_events(timestamp, iso_time, player, event, source) VALUES(?, ?, ?, 'joined', ?)",
            (ts, iso_utc(ts), player, source),
        )

    def record_leave(self, player: str, ts: float, source: str) -> None:
        row = self._ensure_player(player)
        playtime = float(row["playtime_seconds_total"])
        if row["online"] and row["current_session_start_ts"] is not None:
            playtime += max(0.0, ts - float(row["current_session_start_ts"]))
        last_leave = max(float(row["last_leave_ts"] or 0.0), ts)
        self.conn.execute(
            """
            UPDATE players
            SET online = 0,
                leaves_total = leaves_total + 1,
                last_leave_ts = ?,
                playtime_seconds_total = ?,
                current_session_start_ts = NULL
            WHERE player = ?
            """,
            (last_leave, playtime, player),
        )
        self.conn.execute(
            "INSERT INTO player_events(timestamp, iso_time, player, event, source) VALUES(?, ?, ?, 'left', ?)",
            (ts, iso_utc(ts), player, source),
        )

    def close_all_online_players(self, ts: float, source: str) -> int:
        rows = self.conn.execute("SELECT player FROM players WHERE online = 1").fetchall()
        for r in rows:
            self.record_leave(r["player"], ts, source)
        return len(rows)

    def record_hibernate_memory(self, used_bytes: int, max_bytes: int, ts: float) -> None:
        self.set_state("jvm_mem_used_bytes", str(used_bytes))
        self.set_state("jvm_mem_max_bytes", str(max_bytes))
        self.set_state("jvm_mem_ts", str(ts))

    def snapshot_metrics(self) -> dict[str, Any]:
        """Return an atomic snapshot of persisted player & event state for Prometheus."""
        now = time.time()
        with self.lock:
            players_rows = self.conn.execute(
                "SELECT * FROM players ORDER BY player COLLATE NOCASE ASC"
            ).fetchall()
            event_count_row = self.conn.execute("SELECT COUNT(*) AS cnt, MAX(timestamp) AS max_ts FROM player_events").fetchone()
            jvm_used = self.get_state("jvm_mem_used_bytes")
            jvm_max = self.get_state("jvm_mem_max_bytes")
            jvm_ts = self.get_state("jvm_mem_ts")

            players = []
            for r in players_rows:
                name = r["player"]
                playtime = float(r["playtime_seconds_total"])
                if r["online"] and r["current_session_start_ts"] is not None:
                    playtime += max(0.0, now - float(r["current_session_start_ts"]))
                # Guard Prometheus counter monotonicity across sub-second log rounding.
                floor = self._playtime_floor.get(name, 0.0)
                if playtime < floor:
                    playtime = floor
                else:
                    self._playtime_floor[name] = playtime

                players.append(
                    {
                        "player": name,
                        "online": int(r["online"]),
                        "joins_total": int(r["joins_total"]),
                        "leaves_total": int(r["leaves_total"]),
                        "last_join_ts": float(r["last_join_ts"]) if r["last_join_ts"] is not None else None,
                        "last_leave_ts": float(r["last_leave_ts"]) if r["last_leave_ts"] is not None else None,
                        "playtime_seconds_total": playtime,
                    }
                )

            return {
                "players": players,
                "events_recorded_total": int(event_count_row["cnt"]) if event_count_row else 0,
                "last_event_ts": float(event_count_row["max_ts"]) if event_count_row and event_count_row["max_ts"] is not None else None,
                "jvm_mem_used_bytes": int(jvm_used) if jvm_used is not None else None,
                "jvm_mem_max_bytes": int(jvm_max) if jvm_max is not None else None,
                "jvm_mem_ts": float(jvm_ts) if jvm_ts is not None else None,
            }

    def query_events(self, limit: int = 100, player: str | None = None) -> list[dict[str, Any]]:
        limit = max(1, min(limit, 5000))
        with self.lock:
            if player:
                rows = self.conn.execute(
                    "SELECT id, timestamp, iso_time, player, event, source "
                    "FROM player_events WHERE player = ? ORDER BY id DESC LIMIT ?",
                    (player, limit),
                ).fetchall()
            else:
                rows = self.conn.execute(
                    "SELECT id, timestamp, iso_time, player, event, source "
                    "FROM player_events ORDER BY id DESC LIMIT ?",
                    (limit,),
                ).fetchall()
            return [dict(r) for r in rows]


class LogTracker:
    """Backfills historical gzipped logs and tails latest.log across rotations."""

    def __init__(self, logs_dir: Path, store: StateStore, backfill_enabled: bool = True) -> None:
        self.logs_dir = logs_dir
        self.store = store
        self.backfill_enabled = backfill_enabled
        self._last_line_ts: float | None = None

    @staticmethod
    def _archive_sort_key(path: Path) -> tuple[str, int]:
        m = ARCHIVE_NAME_RE.match(path.name)
        if not m:
            return ("9999-99-99", 0)
        return (f"{m.group(1)}-{m.group(2)}-{m.group(3)}", int(m.group(4)))

    def _list_archives(self) -> list[Path]:
        if not self.logs_dir.exists():
            return []
        archives = [p for p in self.logs_dir.iterdir() if p.is_file() and ARCHIVE_NAME_RE.match(p.name)]
        archives.sort(key=self._archive_sort_key)
        return archives

    @staticmethod
    def _parse_hhmmss(line: str) -> tuple[int, int, int, str] | None:
        m = LOG_LINE_RE.match(line)
        if not m:
            return None
        return int(m.group(1)), int(m.group(2)), int(m.group(3)), m.group(5)

    def _infer_start_date_from_lines(self, lines: list[str], mtime_ts: float) -> dt.date:
        """Determine the UTC calendar date of the first timestamped line in a log chunk."""
        mtime_dt = dt.datetime.fromtimestamp(mtime_ts, tz=dt.timezone.utc)
        crossings = 0
        prev_sec: int | None = None
        last_sec: int | None = None
        for line in lines:
            parsed = self._parse_hhmmss(line)
            if not parsed:
                continue
            hh, mm, ss, _ = parsed
            sec = hh * 3600 + mm * 60 + ss
            if prev_sec is not None and sec < prev_sec - 3600:
                crossings += 1
            prev_sec = sec
            last_sec = sec

        end_date = mtime_dt.date()
        if last_sec is not None:
            mtime_sec = mtime_dt.hour * 3600 + mtime_dt.minute * 60 + mtime_dt.second
            # If last log line was 23:59:xx and file mtime ticked to 00:00:xx UTC
            if mtime_sec - last_sec < -43200:
                end_date = end_date - dt.timedelta(days=1)
        return end_date - dt.timedelta(days=crossings)

    def _resolve_live_ts(self, hh: int, mm: int, ss: int, mtime_ts: float) -> float:
        """Resolve an [HH:MM:SS] timestamp for newly tailed live lines."""
        now = time.time()
        now_dt = dt.datetime.fromtimestamp(now, tz=dt.timezone.utc)
        base_date = now_dt.date()
        candidates = [
            dt.datetime(d.year, d.month, d.day, hh, mm, ss, tzinfo=dt.timezone.utc).timestamp()
            for d in (base_date - dt.timedelta(days=1), base_date, base_date + dt.timedelta(days=1))
        ]
        best = min(candidates, key=lambda c: abs(c - now))
        if abs(best - now) <= 60.0:
            # Preserve exact sub-second wall clock when tailing in real time.
            return now if abs(best - now) <= 3.0 else best
        mtime_dt = dt.datetime.fromtimestamp(mtime_ts, tz=dt.timezone.utc)
        d = mtime_dt.date()
        m_candidates = [
            dt.datetime(cd.year, cd.month, cd.day, hh, mm, ss, tzinfo=dt.timezone.utc).timestamp()
            for cd in (d - dt.timedelta(days=1), d, d + dt.timedelta(days=1))
        ]
        return min(m_candidates, key=lambda c: abs(c - mtime_ts))

    def _process_line(self, msg: str, ts: float, source: str) -> None:
        prev_ts = self._last_line_ts
        self._last_line_ts = ts
        if SERVER_START_RE.match(msg):
            # If the previous run crashed without Stopping server, close sessions at its last logged timestamp.
            close_ts = prev_ts if prev_ts and prev_ts <= ts else ts
            self.store.close_all_online_players(close_ts, source="server_lifecycle")
            return
        if SERVER_STOP_RE.match(msg):
            self.store.close_all_online_players(ts, source="server_lifecycle")
            return

        jm = JOIN_RE.match(msg)
        if jm:
            self.store.record_join(jm.group(1), ts, source=source)
            return

        lm = LEAVE_RE.match(msg)
        if lm:
            self.store.record_leave(lm.group(1), ts, source=source)
            return

        mm = HIBERNATE_MEM_RE.match(msg)
        if mm:
            used_bytes = int(mm.group(1)) * 1024 * 1024
            max_bytes = int(mm.group(2)) * 1024 * 1024
            self.store.record_hibernate_memory(used_bytes, max_bytes, ts)

    def _process_lines_with_date(
        self,
        lines: list[str],
        start_date: dt.date,
        source: str,
    ) -> float | None:
        cur_date = start_date
        prev_sec: int | None = None
        last_ts: float | None = None
        for line in lines:
            parsed = self._parse_hhmmss(line)
            if not parsed:
                continue
            hh, mm, ss, msg = parsed
            sec = hh * 3600 + mm * 60 + ss
            if prev_sec is not None and sec < prev_sec - 3600:
                cur_date += dt.timedelta(days=1)
            prev_sec = sec
            ts = dt.datetime(
                cur_date.year, cur_date.month, cur_date.day, hh, mm, ss, tzinfo=dt.timezone.utc
            ).timestamp()
            last_ts = ts
            self._process_line(msg, ts, source=source)
        return last_ts

    def _process_live_lines(self, lines: list[str], mtime_ts: float, source: str) -> None:
        for line in lines:
            parsed = self._parse_hhmmss(line)
            if not parsed:
                continue
            hh, mm, ss, msg = parsed
            ts = self._resolve_live_ts(hh, mm, ss, mtime_ts)
            self._process_line(msg, ts, source=source)

    @staticmethod
    def _compute_head_hash(raw: bytes) -> tuple[str, int]:
        head = raw[:HEAD_HASH_BYTES]
        return hashlib.sha256(head).hexdigest(), len(head)

    def _read_complete_lines_from_bytes(self, chunk: bytes) -> tuple[list[str], int]:
        """Extract complete newline-terminated lines and return (lines, consumed_bytes)."""
        last_nl = chunk.rfind(b"\n")
        if last_nl == -1:
            return [], 0
        complete = chunk[: last_nl + 1]
        lines = complete.decode("utf-8", errors="replace").splitlines()
        return lines, len(complete)

    def startup(self) -> None:
        """Perform initial historical backfill (once) or catch up from persisted offset."""
        with self.store.lock:
            done = self.store.get_state("backfill_done")
            if done == "1":
                LOG.info("Resuming persisted exporter state from %s", self.store.db_path)
                return

            if not self.backfill_enabled:
                LOG.info("Historical log backfill disabled; starting from current latest.log EOF")
                latest = self.logs_dir / "latest.log"
                self.store.begin()
                try:
                    for arch in self._list_archives():
                        self.store.mark_archive_processed(arch.name)
                    if latest.exists():
                        st = latest.stat()
                        raw = latest.read_bytes()
                        h, hlen = self._compute_head_hash(raw)
                        self.store.set_state("latest_log_inode", str(st.st_ino))
                        self.store.set_state("latest_log_offset", str(len(raw)))
                        self.store.set_state("latest_log_head_hash", h)
                        self.store.set_state("latest_log_head_len", str(hlen))
                    self.store.set_state("backfill_done", "1")
                    self.store.commit()
                except Exception:
                    self.store.rollback()
                    raise
                return

            archives = self._list_archives()
            latest = self.logs_dir / "latest.log"
            LOG.info("Starting initial backfill across %d archived logs...", len(archives))

            self.store.begin()
            try:
                for arch in archives:
                    m = ARCHIVE_NAME_RE.match(arch.name)
                    assert m is not None
                    arch_date = dt.date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
                    try:
                        with gzip.open(arch, "rt", encoding="utf-8", errors="replace") as gz:
                            lines = gz.read().splitlines()
                        self._process_lines_with_date(lines, arch_date, source="archive")
                    except OSError as exc:
                        LOG.warning("Failed reading archive %s: %s", arch.name, exc)
                    self.store.mark_archive_processed(arch.name)

                if latest.exists():
                    st = latest.stat()
                    raw = latest.read_bytes()
                    lines, consumed = self._read_complete_lines_from_bytes(raw)
                    start_date = self._infer_start_date_from_lines(lines, st.st_mtime)
                    self._process_lines_with_date(lines, start_date, source="latest.log")
                    h, hlen = self._compute_head_hash(raw[:consumed] if consumed else raw)
                    self.store.set_state("latest_log_inode", str(st.st_ino))
                    self.store.set_state("latest_log_offset", str(consumed))
                    self.store.set_state("latest_log_head_hash", h)
                    self.store.set_state("latest_log_head_len", str(hlen))

                self.store.set_state("backfill_done", "1")
                self.store.commit()
                snap = self.store.snapshot_metrics()
                LOG.info(
                    "Backfill complete: %d known players, %d total events recorded",
                    len(snap["players"]),
                    snap["events_recorded_total"],
                )
            except Exception:
                self.store.rollback()
                raise

    def _catchup_rotated_archives(
        self, saved_offset: int, saved_head_hash: str | None, saved_head_len: int
    ) -> None:
        """Process any newly rotated .log.gz files without duplicating already-tailed prefix."""
        archives = self._list_archives()
        matched_rotated = False
        for arch in archives:
            if self.store.is_archive_processed(arch.name):
                continue
            m = ARCHIVE_NAME_RE.match(arch.name)
            assert m is not None
            arch_date = dt.date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
            try:
                raw = gzip.decompress(arch.read_bytes())
            except OSError as exc:
                LOG.warning("Failed decompressing rotated archive %s: %s", arch.name, exc)
                self.store.mark_archive_processed(arch.name)
                continue

            start_offset = 0
            if (
                not matched_rotated
                and saved_head_hash
                and saved_head_len > 0
                and len(raw) >= saved_head_len
            ):
                arch_head_hash = hashlib.sha256(raw[:saved_head_len]).hexdigest()
                if arch_head_hash == saved_head_hash:
                    start_offset = min(saved_offset, len(raw))
                    matched_rotated = True

            if start_offset == 0:
                lines = raw.decode("utf-8", errors="replace").splitlines()
                self._process_lines_with_date(lines, arch_date, source="archive")
            elif start_offset < len(raw):
                # Advance date state over skipped prefix so timestamps stay accurate.
                prefix_lines = raw[:start_offset].decode("utf-8", errors="replace").splitlines()
                cur_date = arch_date
                prev_sec: int | None = None
                for pl in prefix_lines:
                    parsed = self._parse_hhmmss(pl)
                    if not parsed:
                        continue
                    sec = parsed[0] * 3600 + parsed[1] * 60 + parsed[2]
                    if prev_sec is not None and sec < prev_sec - 3600:
                        cur_date += dt.timedelta(days=1)
                    prev_sec = sec
                tail_lines = raw[start_offset:].decode("utf-8", errors="replace").splitlines()
                self._process_lines_with_date(tail_lines, cur_date, source="archive")

            self.store.mark_archive_processed(arch.name)

    def poll(self) -> bool:
        """Check latest.log for new lines or rotation. Returns True if new lines were read."""
        latest = self.logs_dir / "latest.log"
        if not latest.exists():
            return False

        with self.store.lock:
            try:
                with open(latest, "rb") as f:
                    st = os.fstat(f.fileno())
                    saved_inode_str = self.store.get_state("latest_log_inode")
                    saved_offset = int(self.store.get_state("latest_log_offset", "0") or "0")
                    saved_head_hash = self.store.get_state("latest_log_head_hash")
                    saved_head_len = int(self.store.get_state("latest_log_head_len", "0") or "0")

                    rotated = (
                        saved_inode_str is not None
                        and (st.st_ino != int(saved_inode_str) or st.st_size < saved_offset)
                    )

                    if not rotated and st.st_size == saved_offset:
                        return False

                    self.store.begin()
                    try:
                        if rotated:
                            self._catchup_rotated_archives(saved_offset, saved_head_hash, saved_head_len)
                            saved_offset = 0
                            saved_head_hash = None
                            saved_head_len = 0

                        f.seek(saved_offset)
                        chunk = f.read()
                        lines, consumed = self._read_complete_lines_from_bytes(chunk)

                        if lines:
                            if abs(time.time() - st.st_mtime) < 120.0:
                                self._process_live_lines(lines, st.st_mtime, source="latest.log")
                            else:
                                start_date = self._infer_start_date_from_lines(lines, st.st_mtime)
                                self._process_lines_with_date(lines, start_date, source="latest.log")

                        new_offset = saved_offset + consumed
                        if new_offset > 0 and (rotated or saved_head_len < min(HEAD_HASH_BYTES, new_offset)):
                            f.seek(0)
                            head_raw = f.read(min(HEAD_HASH_BYTES, new_offset))
                            h, hlen = self._compute_head_hash(head_raw)
                            self.store.set_state("latest_log_head_hash", h)
                            self.store.set_state("latest_log_head_len", str(hlen))
                        elif rotated:
                            self.store.set_state("latest_log_head_hash", "")
                            self.store.set_state("latest_log_head_len", "0")

                        self.store.set_state("latest_log_inode", str(st.st_ino))
                        self.store.set_state("latest_log_offset", str(new_offset))
                        self.store.commit()
                        return consumed > 0
                    except Exception:
                        self.store.rollback()
                        raise
            except OSError as exc:
                LOG.warning("Error reading %s: %s", latest, exc)
                return False

    def reconcile_stopped_container(self) -> None:
        """Close any online players if Minecraft container stopped without writing a leave line."""
        with self.store.lock:
            self.store.begin()
            try:
                closed = self.store.close_all_online_players(time.time(), source="container_stopped")
                self.store.commit()
                if closed:
                    LOG.info("Closed %d online player session(s) after container stop", closed)
            except Exception:
                self.store.rollback()
                raise


class DockerStatsCollector:
    """Collects per-container memory and CPU stats via Docker Engine Unix socket."""

    def __init__(self, docker_sock: str, mc_container_name: str) -> None:
        self.docker_sock = docker_sock
        self.mc_container_name = mc_container_name
        self._lock = threading.RLock()
        self._prev_cpu: dict[str, tuple[str, int, int]] = {}
        self._containers: dict[str, dict[str, Any]] = {}
        self._machine_info: dict[str, Any] = {}
        self._mc_running: bool | None = None
        self._mc_stopped_streak: int = 0

    def _get_json(self, path: str) -> Any:
        conn = UnixHTTPConnection(self.docker_sock, timeout=5.0)
        try:
            conn.request("GET", path)
            resp = conn.getresponse()
            body = resp.read()
            if resp.status >= 400:
                raise RuntimeError(f"Docker API {path} returned HTTP {resp.status}: {body[:200]!r}")
            return json.loads(body.decode("utf-8"))
        finally:
            conn.close()

    def poll_once(self) -> None:
        if not os.path.exists(self.docker_sock):
            with self._lock:
                self._containers = {}
                self._mc_running = None
            return

        containers_list = self._get_json("/containers/json?all=false")
        info = self._get_json("/info")

        new_stats: dict[str, dict[str, Any]] = {}
        mc_found = False

        for c in containers_list:
            cid = c["Id"]
            names = c.get("Names") or [cid[:12]]
            cname = names[0].lstrip("/")
            if cname == self.mc_container_name:
                mc_found = True

            try:
                st = self._get_json(f"/containers/{cid}/stats?stream=false&one-shot=true")
            except Exception as exc:
                LOG.debug("Skipping stats for container %s: %s", cname, exc)
                continue

            mem_stats = st.get("memory_stats") or {}
            m_detail = mem_stats.get("stats") or {}
            usage = int(mem_stats.get("usage", 0))
            inactive_file = int(m_detail.get("inactive_file", m_detail.get("total_inactive_file", 0)))
            working_set = max(0, usage - inactive_file)
            rss = int(m_detail.get("anon", m_detail.get("rss", m_detail.get("total_rss", 0))))
            cache = int(m_detail.get("file", m_detail.get("cache", m_detail.get("total_cache", 0))))
            limit = int(mem_stats.get("limit", 0))

            cpu_stats = st.get("cpu_stats") or {}
            cpu_usage = cpu_stats.get("cpu_usage") or {}
            cpu_total = int(cpu_usage.get("total_usage", 0))
            sys_total = int(cpu_stats.get("system_cpu_usage", 0))
            online_cpus = int(
                cpu_stats.get("online_cpus")
                or len(cpu_usage.get("percpu_usage") or [])
                or info.get("NCPU")
                or 1
            )

            cpu_percent = 0.0
            prev = self._prev_cpu.get(cname)
            if prev is not None:
                prev_cid, prev_cpu_total, prev_sys_total = prev
                if prev_cid == cid and cpu_total >= prev_cpu_total and sys_total > prev_sys_total:
                    cpu_delta = cpu_total - prev_cpu_total
                    sys_delta = sys_total - prev_sys_total
                    cpu_percent = (cpu_delta / sys_delta) * online_cpus * 100.0

            self._prev_cpu[cname] = (cid, cpu_total, sys_total)

            new_stats[cname] = {
                "name": cname,
                "usage_bytes": usage,
                "working_set_bytes": working_set,
                "rss_bytes": rss,
                "cache_bytes": cache,
                "limit_bytes": limit,
                "utilization_ratio": (working_set / limit) if limit > 0 else 0.0,
                "cpu_percent": cpu_percent,
                "cpu_seconds_total": cpu_total / 1e9,
            }

        with self._lock:
            self._containers = new_stats
            self._machine_info = {
                "mem_total": int(info.get("MemTotal", 0)),
                "ncpu": int(info.get("NCPU", 0)),
            }
            self._mc_running = mc_found
            if mc_found:
                self._mc_stopped_streak = 0
            else:
                self._mc_stopped_streak += 1

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "containers": dict(self._containers),
                "machine": dict(self._machine_info),
                "mc_running": self._mc_running,
                "mc_stopped_streak": self._mc_stopped_streak,
            }


class MinecraftSLPCollector:
    """Polls Minecraft Server List Ping (TCP 25565) for health, latency, and player count."""

    def __init__(self, host: str, port: int) -> None:
        self.host = host
        self.port = port
        self._lock = threading.RLock()
        self._healthy: int = 0
        self._response_time_seconds: float = 0.0
        self._online_count: int = 0
        self._max_count: int = 0
        self._version: str = ""
        self._has_polled: bool = False

    def poll_once(self) -> None:
        try:
            status = query_minecraft_slp(self.host, self.port, timeout=3.0)
            players = status.get("players") or {}
            version = (status.get("version") or {}).get("name") or ""
            with self._lock:
                self._healthy = 1
                self._response_time_seconds = float(status.get("_latency_seconds", 0.0))
                self._online_count = int(players.get("online", 0))
                self._max_count = int(players.get("max", 0))
                if version:
                    self._version = str(version)
                self._has_polled = True
        except Exception as exc:
            LOG.debug("Minecraft SLP ping failed (%s:%d): %s", self.host, self.port, exc)
            with self._lock:
                self._healthy = 0
                self._online_count = 0
                self._has_polled = True

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "healthy": self._healthy,
                "response_time_seconds": self._response_time_seconds,
                "online_count": self._online_count,
                "max_count": self._max_count,
                "version": self._version,
                "has_polled": self._has_polled,
                "host": self.host,
                "port": str(self.port),
            }


def render_prometheus_metrics(
    config: Config,
    state_snap: dict[str, Any],
    docker_snap: dict[str, Any],
    slp_snap: dict[str, Any],
) -> str:
    """Render all metrics in Prometheus text exposition format (0.0.4)."""
    lines: list[str] = []

    def add_metric(name: str, mtype: str, help_text: str, samples: list[tuple[dict[str, str], float | int]]) -> None:
        if not samples:
            return
        lines.append(f"# HELP {name} {help_text}")
        lines.append(f"# TYPE {name} {mtype}")
        for labels, value in samples:
            if isinstance(value, float):
                val_str = f"{value:.6f}".rstrip("0").rstrip(".") or "0"
                if val_str == "-0":
                    val_str = "0"
            else:
                val_str = str(value)
            lines.append(f"{name}{format_labels(labels)} {val_str}")

    players = state_snap["players"]
    online_count = sum(p["online"] for p in players)

    # Exporter health & persistence metadata
    add_metric("minecraft_exporter_up", "gauge", "Whether mc-exporter is running (1 = up).", [({}, 1)])
    add_metric(
        "minecraft_exporter_events_recorded_total",
        "counter",
        "Total player join/leave events persisted across server runs.",
        [({}, state_snap["events_recorded_total"])],
    )
    if state_snap["last_event_ts"] is not None:
        add_metric(
            "minecraft_exporter_last_event_timestamp_seconds",
            "gauge",
            "Unix timestamp of the latest recorded player join/leave event.",
            [({}, state_snap["last_event_ts"])],
        )

    # Log-tracked player counts and per-player persisted metrics
    add_metric(
        "minecraft_players_online_count",
        "gauge",
        "Number of players currently online (tracked from server logs).",
        [({}, online_count)],
    )
    add_metric(
        "minecraft_players_known_total",
        "gauge",
        "Total number of unique players observed across all server runs.",
        [({}, len(players))],
    )

    add_metric(
        "minecraft_player_online",
        "gauge",
        "Whether a player is currently online (1 = online, 0 = offline).",
        [({"player": p["player"]}, p["online"]) for p in players],
    )
    add_metric(
        "minecraft_player_joins_total",
        "counter",
        "Total number of times a player has joined the server across all runs.",
        [({"player": p["player"]}, p["joins_total"]) for p in players],
    )
    add_metric(
        "minecraft_player_leaves_total",
        "counter",
        "Total number of times a player has left the server across all runs.",
        [({"player": p["player"]}, p["leaves_total"]) for p in players],
    )
    add_metric(
        "minecraft_player_last_join_timestamp_seconds",
        "gauge",
        "Unix timestamp when the player last joined the server.",
        [({"player": p["player"]}, p["last_join_ts"]) for p in players if p["last_join_ts"] is not None],
    )
    add_metric(
        "minecraft_player_last_leave_timestamp_seconds",
        "gauge",
        "Unix timestamp when the player last left the server.",
        [({"player": p["player"]}, p["last_leave_ts"]) for p in players if p["last_leave_ts"] is not None],
    )
    add_metric(
        "minecraft_player_playtime_seconds_total",
        "counter",
        "Cumulative player playtime in seconds across all server runs.",
        [({"player": p["player"]}, p["playtime_seconds_total"]) for p in players],
    )

    # Server List Ping (mc-monitor replacement) metrics
    if slp_snap["has_polled"]:
        slp_labels = {
            "server_edition": "java",
            "server_host": slp_snap["host"],
            "server_port": slp_snap["port"],
            "server_version": slp_snap["version"],
        }
        add_metric(
            "minecraft_status_healthy",
            "gauge",
            "Whether the Minecraft server responded to Server List Ping (1 = healthy, 0 = down).",
            [(slp_labels, slp_snap["healthy"])],
        )
        add_metric(
            "minecraft_status_players_online_count",
            "gauge",
            "Number of online players reported by Minecraft Server List Ping.",
            [(slp_labels, slp_snap["online_count"])],
        )
        add_metric(
            "minecraft_status_players_max_count",
            "gauge",
            "Maximum player slots reported by Minecraft Server List Ping.",
            [(slp_labels, slp_snap["max_count"])],
        )
        add_metric(
            "minecraft_status_response_time_seconds",
            "gauge",
            "Response time of Minecraft Server List Ping in seconds.",
            [(slp_labels, slp_snap["response_time_seconds"])],
        )

    # Docker container & Minecraft memory / CPU metrics
    containers = docker_snap["containers"]
    mc_stats = containers.get(config.mc_container_name)
    add_metric(
        "minecraft_container_up",
        "gauge",
        "Whether the Minecraft Docker container is running (1 = running, 0 = stopped).",
        [({}, 1 if mc_stats is not None else 0)],
    )
    if mc_stats is not None:
        add_metric(
            "minecraft_server_memory_working_set_bytes",
            "gauge",
            "Minecraft container working set memory in bytes (usage - inactive_file).",
            [({}, mc_stats["working_set_bytes"])],
        )
        add_metric(
            "minecraft_server_memory_usage_bytes",
            "gauge",
            "Minecraft container total cgroup memory usage in bytes.",
            [({}, mc_stats["usage_bytes"])],
        )
        add_metric(
            "minecraft_server_memory_rss_bytes",
            "gauge",
            "Minecraft container anonymous/RSS memory usage in bytes.",
            [({}, mc_stats["rss_bytes"])],
        )
        add_metric(
            "minecraft_server_memory_cache_bytes",
            "gauge",
            "Minecraft container page cache memory in bytes.",
            [({}, mc_stats["cache_bytes"])],
        )
        add_metric(
            "minecraft_server_memory_limit_bytes",
            "gauge",
            "Minecraft container memory limit in bytes.",
            [({}, mc_stats["limit_bytes"])],
        )
        add_metric(
            "minecraft_server_memory_utilization_ratio",
            "gauge",
            "Minecraft container working set memory divided by memory limit.",
            [({}, mc_stats["utilization_ratio"])],
        )
        add_metric(
            "minecraft_server_cpu_usage_percent",
            "gauge",
            "Minecraft container CPU utilization percent (100% = 1 full core).",
            [({}, mc_stats["cpu_percent"])],
        )
        add_metric(
            "minecraft_server_cpu_usage_seconds_total",
            "counter",
            "Minecraft container cumulative CPU time consumed in seconds.",
            [({}, mc_stats["cpu_seconds_total"])],
        )

    # JVM hibernate_mc logged memory (when available)
    if state_snap["jvm_mem_used_bytes"] is not None:
        add_metric(
            "minecraft_jvm_hibernate_memory_used_bytes",
            "gauge",
            "Last JVM heap memory used logged by hibernate_mc in bytes.",
            [({}, state_snap["jvm_mem_used_bytes"])],
        )
    if state_snap["jvm_mem_max_bytes"] is not None:
        add_metric(
            "minecraft_jvm_hibernate_memory_max_bytes",
            "gauge",
            "Last JVM heap max memory logged by hibernate_mc in bytes.",
            [({}, state_snap["jvm_mem_max_bytes"])],
        )
    if state_snap["jvm_mem_ts"] is not None:
        add_metric(
            "minecraft_jvm_hibernate_memory_last_updated_timestamp_seconds",
            "gauge",
            "Timestamp when hibernate_mc last logged JVM memory usage.",
            [({}, state_snap["jvm_mem_ts"])],
        )

    # cAdvisor-compatible per-container metrics (keyed stably by container name)
    if containers:
        c_list = sorted(containers.values(), key=lambda c: c["name"])
        add_metric(
            "container_memory_working_set_bytes",
            "gauge",
            "Container working set memory in bytes (usage - inactive_file).",
            [({"name": c["name"], "container_name": c["name"]}, c["working_set_bytes"]) for c in c_list],
        )
        add_metric(
            "container_memory_usage_bytes",
            "gauge",
            "Container total memory usage in bytes.",
            [({"name": c["name"], "container_name": c["name"]}, c["usage_bytes"]) for c in c_list],
        )
        add_metric(
            "container_memory_rss",
            "gauge",
            "Container anonymous/RSS memory in bytes.",
            [({"name": c["name"], "container_name": c["name"]}, c["rss_bytes"]) for c in c_list],
        )
        add_metric(
            "container_memory_cache",
            "gauge",
            "Container page cache memory in bytes.",
            [({"name": c["name"], "container_name": c["name"]}, c["cache_bytes"]) for c in c_list],
        )
        add_metric(
            "container_spec_memory_limit_bytes",
            "gauge",
            "Container memory limit in bytes.",
            [({"name": c["name"], "container_name": c["name"]}, c["limit_bytes"]) for c in c_list],
        )
        add_metric(
            "container_cpu_usage_seconds_total",
            "counter",
            "Container cumulative CPU time consumed in seconds.",
            [({"name": c["name"], "container_name": c["name"]}, c["cpu_seconds_total"]) for c in c_list],
        )

    machine = docker_snap["machine"]
    if machine.get("mem_total"):
        add_metric("machine_memory_bytes", "gauge", "Total Docker host memory in bytes.", [({}, machine["mem_total"])])
    if machine.get("ncpu"):
        add_metric("machine_cpu_cores", "gauge", "Number of CPU cores on the Docker host.", [({}, machine["ncpu"])])

    return "\n".join(lines) + "\n"


def make_handler(
    config: Config,
    store: StateStore,
    docker_collector: DockerStatsCollector,
    slp_collector: MinecraftSLPCollector,
) -> type[BaseHTTPRequestHandler]:
    class ExporterHandler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, format: str, *args: Any) -> None:
            LOG.debug("HTTP %s - %s", self.address_string(), format % args)

        def _send(self, status: int, content_type: str, body: bytes) -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:
            parsed = urlparse(self.path)
            if parsed.path == "/metrics":
                payload = render_prometheus_metrics(
                    config,
                    store.snapshot_metrics(),
                    docker_collector.snapshot(),
                    slp_collector.snapshot(),
                ).encode("utf-8")
                self._send(200, "text/plain; version=0.0.4; charset=utf-8", payload)
            elif parsed.path == "/events":
                qs = parse_qs(parsed.query)
                limit = int(qs.get("limit", ["100"])[0])
                player = qs.get("player", [None])[0]
                events = store.query_events(limit=limit, player=player)
                body = json.dumps({"events": events}, indent=2).encode("utf-8")
                self._send(200, "application/json; charset=utf-8", body)
            elif parsed.path == "/players":
                snap = store.snapshot_metrics()
                players_out = []
                for p in snap["players"]:
                    players_out.append(
                        {
                            "player": p["player"],
                            "online": bool(p["online"]),
                            "joins_total": p["joins_total"],
                            "leaves_total": p["leaves_total"],
                            "last_join": iso_utc(p["last_join_ts"]) if p["last_join_ts"] else None,
                            "last_leave": iso_utc(p["last_leave_ts"]) if p["last_leave_ts"] else None,
                            "playtime_seconds_total": round(p["playtime_seconds_total"], 1),
                        }
                    )
                body = json.dumps({"players": players_out}, indent=2).encode("utf-8")
                self._send(200, "application/json; charset=utf-8", body)
            elif parsed.path in ("/healthz", "/"):
                body = json.dumps({"status": "ok"}).encode("utf-8")
                self._send(200, "application/json; charset=utf-8", body)
            else:
                self._send(404, "text/plain; charset=utf-8", b"Not Found\n")

    return ExporterHandler


def run_exporter(config: Config) -> None:
    store = StateStore(config.state_db_path)
    tracker = LogTracker(config.logs_dir, store, backfill_enabled=config.backfill_logs)
    docker_collector = DockerStatsCollector(config.docker_sock, config.mc_container_name)
    slp_collector = MinecraftSLPCollector(config.mc_host, config.mc_port)

    tracker.startup()

    # Run initial collector polls before starting HTTP server so first scrape has full data.
    try:
        docker_collector.poll_once()
    except Exception as exc:
        LOG.warning("Initial Docker stats poll failed: %s", exc)
    slp_collector.poll_once()

    stop_event = threading.Event()

    def log_loop() -> None:
        while not stop_event.is_set():
            try:
                had_new = tracker.poll()
                if not had_new:
                    ds = docker_collector.snapshot()
                    if ds["mc_running"] is False and ds["mc_stopped_streak"] >= 2:
                        tracker.reconcile_stopped_container()
            except Exception as exc:
                LOG.exception("Error in log polling loop: %s", exc)
            stop_event.wait(config.log_poll_interval)

    def docker_loop() -> None:
        while not stop_event.is_set():
            try:
                docker_collector.poll_once()
            except Exception as exc:
                LOG.warning("Error in Docker stats loop: %s", exc)
            stop_event.wait(config.docker_poll_interval)

    def slp_loop() -> None:
        while not stop_event.is_set():
            try:
                slp_collector.poll_once()
            except Exception as exc:
                LOG.warning("Error in SLP polling loop: %s", exc)
            stop_event.wait(config.slp_poll_interval)

    threads = [
        threading.Thread(target=log_loop, name="log-poller", daemon=True),
        threading.Thread(target=docker_loop, name="docker-poller", daemon=True),
        threading.Thread(target=slp_loop, name="slp-poller", daemon=True),
    ]
    for t in threads:
        t.start()

    server = ThreadingHTTPServer(
        (config.listen_host, config.listen_port),
        make_handler(config, store, docker_collector, slp_collector),
    )

    def handle_signal(signum: int, _frame: Any) -> None:
        LOG.info("Received signal %d, shutting down...", signum)
        stop_event.set()
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    LOG.info(
        "mc-exporter listening on %s:%d (logs=%s, container=%s, slp=%s:%d, db=%s)",
        config.listen_host,
        config.listen_port,
        config.logs_dir,
        config.mc_container_name,
        config.mc_host,
        config.mc_port,
        config.state_db_path,
    )
    try:
        server.serve_forever()
    finally:
        stop_event.set()
        server.server_close()
        store.close()


if __name__ == "__main__":
    run_exporter(Config.from_env())

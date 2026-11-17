#!/usr/bin/env python3
"""Unit tests for mc-exporter."""

import gzip
import tempfile
import unittest
from pathlib import Path

import exporter


class ExporterTests(unittest.TestCase):
    def test_varint_roundtrip(self) -> None:
        for val in (0, 1, 127, 128, 255, 25565, 2147483647):
            encoded = exporter.encode_varint(val)
            decoded, offset = exporter.read_varint_from_bytes(encoded, 0)
            self.assertEqual(decoded, val)
            self.assertEqual(offset, len(encoded))

    def test_join_leave_formats_and_chat_injection_safety(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            logs_dir = tmp_path / "logs"
            logs_dir.mkdir()
            db_path = tmp_path / "state.db"

            log_lines = [
                "[10:00:00] [main/INFO]: Loading Minecraft 26.3 with Fabric Loader 0.20.1\n",
                "[10:01:00] [Server thread/INFO]: Starting minecraft server version 26.3\n",
                "[10:02:00] [Server thread/INFO]: System chat: PlayerOne joined the game\n",
                "[10:02:10] [Server thread/INFO]: <PlayerOne> FakePlayer joined the game\n",
                "[10:02:15] [Server thread/INFO]: [Not Secure] <PlayerOne> FakePlayer2 left the game\n",
                "[10:03:00] [Server thread/INFO]: PlayerTwo joined the game\n",
                "[10:03:30] [Server thread/INFO]: PlayerThree (formerly known as OldName) joined the game\n",
                "[10:04:00] [Server thread/INFO]: System chat: PlayerOne left the game\n",
                "[10:05:00] [pool-7-thread-1/INFO]: Memory: 962MB used / 12288MB max (7.8%) — Free: 11326MB\n",
            ]
            latest = logs_dir / "latest.log"
            latest.write_text("".join(log_lines), encoding="utf-8")

            store = exporter.StateStore(db_path)
            tracker = exporter.LogTracker(logs_dir, store, backfill_enabled=True)
            tracker.startup()

            snap = store.snapshot_metrics()
            by_name = {p["player"]: p for p in snap["players"]}
            self.assertEqual(set(by_name.keys()), {"PlayerOne", "PlayerTwo", "PlayerThree"})
            self.assertEqual(by_name["PlayerOne"]["online"], 0)
            self.assertEqual(by_name["PlayerOne"]["joins_total"], 1)
            self.assertEqual(by_name["PlayerOne"]["leaves_total"], 1)
            self.assertAlmostEqual(by_name["PlayerOne"]["playtime_seconds_total"], 120.0, delta=1.0)
            self.assertEqual(by_name["PlayerTwo"]["online"], 1)
            self.assertEqual(by_name["PlayerThree"]["online"], 1)
            self.assertEqual(snap["jvm_mem_used_bytes"], 962 * 1024 * 1024)
            self.assertEqual(snap["jvm_mem_max_bytes"], 12288 * 1024 * 1024)
            store.close()

    def test_persistence_across_restart_and_crash_cleanup(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            logs_dir = tmp_path / "logs"
            logs_dir.mkdir()
            db_path = tmp_path / "state.db"

            arch_lines = [
                "[23:59:00] [Server thread/INFO]: System chat: PlayerOne joined the game\n",
                "[23:59:50] [Server thread/INFO]: System chat: PlayerOne left the game\n",
                "[00:00:10] [Server thread/INFO]: System chat: PlayerTwo joined the game\n",
                # Crash happens at 00:00:30 without PlayerTwo leaving!
                "[00:00:30] [Server thread/INFO]: Chunk save complete\n",
            ]
            with gzip.open(logs_dir / "2026-11-14-1.log.gz", "wt", encoding="utf-8") as gz:
                gz.writelines(arch_lines)

            latest = logs_dir / "latest.log"
            latest.write_text(
                "[05:00:00] [main/INFO]: Loading Minecraft 26.3 with Fabric Loader 0.20.1\n"
                "[05:00:10] [Server thread/INFO]: System chat: PlayerOne joined the game\n",
                encoding="utf-8",
            )

            store1 = exporter.StateStore(db_path)
            tracker1 = exporter.LogTracker(logs_dir, store1, backfill_enabled=True)
            tracker1.startup()
            snap1 = store1.snapshot_metrics()
            by_name1 = {p["player"]: p for p in snap1["players"]}
            # PlayerTwo's crashed session should be closed at 00:00:30 (20s), not 05:00:00!
            self.assertEqual(by_name1["PlayerTwo"]["online"], 0)
            self.assertEqual(by_name1["PlayerTwo"]["leaves_total"], 1)
            self.assertAlmostEqual(by_name1["PlayerTwo"]["playtime_seconds_total"], 20.0, delta=0.5)
            self.assertEqual(by_name1["PlayerOne"]["online"], 1)
            self.assertEqual(by_name1["PlayerOne"]["joins_total"], 2)
            store1.close()

            # Simulate exporter restart: state must persist without double-counting!
            with open(latest, "a", encoding="utf-8") as f:
                f.write("[05:01:00] [Server thread/INFO]: System chat: PlayerOne left the game\n")

            store2 = exporter.StateStore(db_path)
            tracker2 = exporter.LogTracker(logs_dir, store2, backfill_enabled=True)
            tracker2.startup()
            self.assertTrue(tracker2.poll())
            self.assertFalse(tracker2.poll())

            snap2 = store2.snapshot_metrics()
            by_name2 = {p["player"]: p for p in snap2["players"]}
            self.assertEqual(by_name2["PlayerOne"]["online"], 0)
            self.assertEqual(by_name2["PlayerOne"]["joins_total"], 2)
            self.assertEqual(by_name2["PlayerOne"]["leaves_total"], 2)
            events = store2.query_events(limit=10)
            self.assertEqual(len(events), 6)
            store2.close()

    def test_log_rotation_and_partial_line_handling(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            logs_dir = tmp_path / "logs"
            logs_dir.mkdir()
            db_path = tmp_path / "state.db"

            latest = logs_dir / "latest.log"
            initial_content = "[01:00:00] [Server thread/INFO]: System chat: PlayerOne joined the game\n"
            latest.write_text(initial_content, encoding="utf-8")

            store = exporter.StateStore(db_path)
            tracker = exporter.LogTracker(logs_dir, store, backfill_enabled=True)
            tracker.startup()

            # Write partial line without newline -> should not consume yet
            with open(latest, "ab") as f:
                f.write(b"[01:00:10] [Server thread/INFO]: System chat: PlayerTwo joi")
            self.assertFalse(tracker.poll())

            # Finish line
            with open(latest, "ab") as f:
                f.write(b"ned the game\n")
            self.assertTrue(tracker.poll())

            # Rotate latest.log to .log.gz WITH an extra trailing line written before rotation
            rotated_content = (
                latest.read_text(encoding="utf-8")
                + "[01:00:20] [Server thread/INFO]: System chat: PlayerOne left the game\n"
            )
            with gzip.open(logs_dir / "2026-11-15-1.log.gz", "wt", encoding="utf-8") as gz:
                gz.write(rotated_content)
            latest.unlink()
            latest.write_text(
                "[01:01:00] [main/INFO]: Loading Minecraft 26.3\n"
                "[01:01:05] [Server thread/INFO]: System chat: PlayerThree joined the game\n",
                encoding="utf-8",
            )

            self.assertTrue(tracker.poll())
            snap = store.snapshot_metrics()
            by_name = {p["player"]: p for p in snap["players"]}
            # PlayerOne joined & left (from rotated tail); PlayerTwo closed on server start; PlayerThree online
            self.assertEqual(by_name["PlayerOne"]["joins_total"], 1)
            self.assertEqual(by_name["PlayerOne"]["leaves_total"], 1)
            self.assertEqual(by_name["PlayerOne"]["online"], 0)
            self.assertEqual(by_name["PlayerTwo"]["joins_total"], 1)
            self.assertEqual(by_name["PlayerTwo"]["leaves_total"], 1)
            self.assertEqual(by_name["PlayerTwo"]["online"], 0)
            self.assertEqual(by_name["PlayerThree"]["joins_total"], 1)
            self.assertEqual(by_name["PlayerThree"]["online"], 1)
            store.close()

    def test_prometheus_metrics_rendering(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            cfg = exporter.Config(
                logs_dir=tmp_path,
                mc_container_name="minecraft",
                mc_host="minecraft",
                mc_port=25565,
                docker_sock="/nonexistent.sock",
                state_db_path=tmp_path / "state.db",
                listen_host="127.0.0.1",
                listen_port=9100,
                docker_poll_interval=5.0,
                log_poll_interval=1.0,
                slp_poll_interval=10.0,
                backfill_logs=False,
            )
            state_snap = {
                "players": [
                    {
                        "player": "PlayerOne",
                        "online": 1,
                        "joins_total": 3,
                        "leaves_total": 2,
                        "last_join_ts": 1700000100.0,
                        "last_leave_ts": 1700000050.0,
                        "playtime_seconds_total": 360.5,
                    }
                ],
                "events_recorded_total": 5,
                "last_event_ts": 1700000100.0,
                "jvm_mem_used_bytes": 500_000_000,
                "jvm_mem_max_bytes": 12_000_000_000,
                "jvm_mem_ts": 1700000090.0,
            }
            docker_snap = {
                "containers": {
                    "minecraft": {
                        "name": "minecraft",
                        "usage_bytes": 8_000_000_000,
                        "working_set_bytes": 7_800_000_000,
                        "rss_bytes": 7_700_000_000,
                        "cache_bytes": 200_000_000,
                        "limit_bytes": 24_000_000_000,
                        "utilization_ratio": 0.325,
                        "cpu_percent": 12.5,
                        "cpu_seconds_total": 100.0,
                    }
                },
                "machine": {"mem_total": 24_000_000_000, "ncpu": 12},
                "mc_running": True,
                "mc_stopped_streak": 0,
            }
            slp_snap = {
                "healthy": 1,
                "response_time_seconds": 0.0025,
                "online_count": 1,
                "max_count": 10,
                "version": "26.3",
                "has_polled": True,
                "host": "minecraft",
                "port": "25565",
            }
            output = exporter.render_prometheus_metrics(cfg, state_snap, docker_snap, slp_snap)
            self.assertIn('minecraft_player_online{player="PlayerOne"} 1', output)
            self.assertIn('minecraft_player_joins_total{player="PlayerOne"} 3', output)
            self.assertIn('minecraft_player_leaves_total{player="PlayerOne"} 2', output)
            self.assertIn('minecraft_player_last_join_timestamp_seconds{player="PlayerOne"} 1700000100', output)
            self.assertIn("minecraft_server_memory_working_set_bytes 7800000000", output)
            self.assertIn('container_memory_working_set_bytes{name="minecraft",container_name="minecraft"} 7800000000', output)
            self.assertIn('minecraft_status_players_online_count{server_edition="java",server_host="minecraft",server_port="25565",server_version="26.3"} 1', output)


if __name__ == "__main__":
    unittest.main()

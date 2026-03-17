"""Dual logger: human-readable stdout + JSON lines file."""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def format_count(n: int | float) -> str:
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.1f}K"
    return str(int(n))


def format_bytes_per_sec(bps: float) -> str:
    if bps >= 1024 * 1024:
        return f"{bps / (1024 * 1024):.1f} MB/s"
    if bps >= 1024:
        return f"{bps / 1024:.1f} KB/s"
    return f"{bps:.0f} B/s"


def format_elapsed(seconds: float) -> str:
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = int(seconds % 60)
    if h > 0:
        return f"{h}h{m:02d}m"
    if m > 0:
        return f"{m}m{s:02d}s"
    return f"{s}s"


class DualLogger:
    def __init__(self, log_dir: str):
        self._log_dir = Path(log_dir)
        self._log_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        self._json_path = self._log_dir / f"compaction-stress-{ts}.jsonl"
        self._json_file = open(self._json_path, "a")
        self.info(f"JSON log: {self._json_path}")

    def info(self, msg: str) -> None:
        ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
        print(f"[{ts}] {msg}", flush=True)

    def warn(self, msg: str) -> None:
        ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
        print(f"[{ts}] WARN: {msg}", file=sys.stderr, flush=True)

    def error(self, msg: str) -> None:
        ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
        print(f"[{ts}] ERROR: {msg}", file=sys.stderr, flush=True)

    def json_event(self, event: dict[str, Any]) -> None:
        event["ts"] = datetime.now(timezone.utc).isoformat()
        self._json_file.write(json.dumps(event) + "\n")
        self._json_file.flush()

    def report(
        self,
        elapsed: float,
        scenario_stats: dict[str, dict[str, Any]],
        cluster_metrics: dict[str, Any] | None,
        offset_stats: dict[str, dict[str, Any]] | None = None,
    ) -> None:
        header = f"── {format_elapsed(elapsed)} elapsed "
        print(f"\n[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] {header:─<66}")

        for name, stats in sorted(scenario_stats.items()):
            records = format_count(stats.get("records", 0))
            bps = format_bytes_per_sec(stats.get("bytes_per_sec", 0))
            errors = stats.get("errors", 0)
            tombstones = stats.get("tombstones", 0)
            extra = ""
            if tombstones:
                extra = f" │ {format_count(tombstones)} tombstones"
            if stats.get("num_topics", 1) > 1:
                extra += f" ({stats['num_topics']} topics)"
            print(f"  {name:<20s}: {records:>8s} records │ {bps:>10s} │ {errors} errors{extra}")

        if offset_stats:
            print("  ── compaction progress ──")
            for topic, ts in sorted(offset_stats.items()):
                if "error" in ts:
                    print(f"  {topic}: {ts['error']}")
                    continue
                offset_range = ts.get("offset_range", 0)
                remaining = ts.get("records_remaining", 0)
                ratio = ts.get("compaction_ratio", 0.0)
                removed = offset_range - remaining
                print(
                    f"  {topic}: "
                    f"{format_count(remaining)} remaining / "
                    f"{format_count(offset_range)} offsets │ "
                    f"{format_count(removed)} removed │ "
                    f"ratio {ratio:.2f}"
                )

        if cluster_metrics:
            print("  ── cluster compaction ──")
            rounds = format_count(cluster_metrics.get("compaction_rounds", 0))
            queue = cluster_metrics.get("queue_depth", 0)
            removed = format_count(cluster_metrics.get("records_removed", 0))
            tombs = format_count(cluster_metrics.get("tombstones_removed", 0))
            print(f"  compaction rounds: {rounds} │ queue depth: {queue} │ records removed: {removed} │ tombstones removed: {tombs}")

        print(flush=True)

        self.json_event({
            "type": "report",
            "elapsed": elapsed,
            "scenarios": scenario_stats,
            "cluster_metrics": cluster_metrics,
            "offset_stats": offset_stats,
        })

    def close(self) -> None:
        self._json_file.close()

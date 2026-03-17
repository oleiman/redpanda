"""Optional Prometheus metrics scraper for Redpanda compaction stats."""

from __future__ import annotations

import re
import threading
import time
from typing import Any

import requests

METRICS_OF_INTEREST = [
    "vectorized_cloud_topics_compaction_scheduler_log_compactions",
    "vectorized_cloud_topics_compaction_scheduler_compaction_queue_length",
    "vectorized_cloud_topics_compaction_worker_records_removed",
    "vectorized_cloud_topics_compaction_worker_tombstones_removed",
    "vectorized_cloud_topics_compaction_worker_compaction_duration_seconds",
]

METRIC_KEY_MAP = {
    "vectorized_cloud_topics_compaction_scheduler_log_compactions": "compaction_rounds",
    "vectorized_cloud_topics_compaction_scheduler_compaction_queue_length": "queue_depth",
    "vectorized_cloud_topics_compaction_worker_records_removed": "records_removed",
    "vectorized_cloud_topics_compaction_worker_tombstones_removed": "tombstones_removed",
    "vectorized_cloud_topics_compaction_worker_compaction_duration_seconds": "compaction_duration_s",
}

_METRIC_LINE_RE = re.compile(
    r'^(\w+)(?:\{[^}]*\})?\s+([\d.eE+\-]+)',
)


def _parse_metrics(text: str) -> dict[str, float]:
    """Parse Prometheus text format, return metric_name -> sum of values."""
    result: dict[str, float] = {}
    for line in text.splitlines():
        if line.startswith("#"):
            continue
        m = _METRIC_LINE_RE.match(line)
        if not m:
            continue
        name, val_str = m.group(1), m.group(2)
        if name in METRICS_OF_INTEREST:
            result[name] = result.get(name, 0.0) + float(val_str)
    return result


class MetricsScraper:
    """Background thread that periodically scrapes Redpanda metrics."""

    def __init__(
        self,
        admin_hosts: list[str],
        interval: float = 10.0,
        timeout: float = 5.0,
    ):
        self._hosts = admin_hosts
        self._interval = interval
        self._timeout = timeout
        self._lock = threading.Lock()
        self._latest: dict[str, float] = {}
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._warn_callback: Any = None

    def set_warn_callback(self, cb: Any) -> None:
        self._warn_callback = cb

    def start(self) -> None:
        if not self._hosts:
            return
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)

    def get_metrics(self) -> dict[str, Any] | None:
        if not self._hosts:
            return None
        with self._lock:
            if not self._latest:
                return None
            return {
                METRIC_KEY_MAP.get(k, k): v
                for k, v in self._latest.items()
            }

    def _run(self) -> None:
        while not self._stop.is_set():
            aggregated: dict[str, float] = {}
            for host in self._hosts:
                try:
                    url = f"http://{host}/metrics"
                    resp = requests.get(url, timeout=self._timeout)
                    resp.raise_for_status()
                    node_metrics = _parse_metrics(resp.text)
                    for k, v in node_metrics.items():
                        aggregated[k] = aggregated.get(k, 0.0) + v
                except Exception as e:
                    if self._warn_callback:
                        self._warn_callback(f"Failed to scrape {host}: {e}")
            with self._lock:
                self._latest = aggregated
            self._stop.wait(self._interval)

"""Track iceberg translation progress by reading table metadata from GCS.

Reads the filesystem catalog layout directly:
  gs://<bucket>/<base>/<namespace>/<table>/metadata/version-hint.text
  gs://<bucket>/<base>/<namespace>/<table>/metadata/v<N>.metadata.json

Extracts row counts and snapshot info from the metadata JSON.
No admin API or REST catalog needed.
"""

from __future__ import annotations

import json
import threading
from typing import Any

from compaction_stress.config import ClusterConfig


class IcebergTracker:
    """Periodically reads iceberg table metadata from GCS."""

    def __init__(
        self,
        cluster: ClusterConfig,
        topics: list[str],
        interval: float = 30.0,
    ):
        self._cluster = cluster
        self._topics = topics
        self._interval = interval
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self._latest: dict[str, dict[str, Any]] = {}
        self._warn_fn: Any = None
        self._fs = None

    def set_warn_callback(self, cb: Any) -> None:
        self._warn_fn = cb

    def start(self) -> None:
        if not self._topics or not self._cluster.gcs_bucket:
            return
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def signal_stop(self) -> None:
        self._stop.set()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=10)

    def get_stats(self) -> dict[str, dict[str, Any]] | None:
        with self._lock:
            return dict(self._latest) if self._latest else None

    def _get_fs(self):
        if self._fs is None:
            import gcsfs
            self._fs = gcsfs.GCSFileSystem()
        return self._fs

    def _run(self) -> None:
        self._stop.wait(15)

        while not self._stop.is_set():
            stats = {}
            for topic in self._topics:
                stats[topic] = self._check_table(topic)
            with self._lock:
                self._latest = stats
            self._stop.wait(self._interval)

    def _table_base(self, topic: str) -> str:
        bucket = self._cluster.gcs_bucket
        base = self._cluster.iceberg_catalog_base_location
        ns = self._cluster.iceberg_catalog_namespace
        return f"{bucket}/{base}/{ns}/{topic}"

    def _check_table(self, topic: str) -> dict[str, Any]:
        try:
            fs = self._get_fs()
            table_path = self._table_base(topic)
            metadata_dir = f"{table_path}/metadata"

            # Read version hint to find current metadata version
            hint_path = f"{metadata_dir}/version-hint.text"
            if not fs.exists(hint_path):
                return {"status": "no table yet"}

            version = fs.cat_file(hint_path).decode().strip()
            metadata_path = f"{metadata_dir}/v{version}.metadata.json"

            if not fs.exists(metadata_path):
                return {"status": f"metadata v{version} not found"}

            raw = json.loads(fs.cat_file(metadata_path))

            # Extract info from metadata
            snapshots = raw.get("snapshots", [])
            current_snapshot_id = raw.get("current-snapshot-id", -1)

            rows = 0
            files = 0
            total_size = 0
            added_rows = 0
            added_files = 0
            for snap in snapshots:
                if snap.get("snapshot-id") == current_snapshot_id:
                    summary = snap.get("summary", {})
                    rows = int(summary.get("total-records", 0))
                    files = int(summary.get("total-data-files", 0))
                    total_size = int(summary.get("total-files-size", 0))
                    added_rows = int(summary.get("added-records", 0))
                    added_files = int(summary.get("added-data-files", 0))
                    break

            return {
                "version": int(version),
                "snapshots": len(snapshots),
                "rows": rows,
                "files": files,
                "total_size": total_size,
                "last_added_rows": added_rows,
                "last_added_files": added_files,
            }
        except Exception as e:
            return {"error": str(e)}

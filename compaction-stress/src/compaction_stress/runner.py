"""Orchestrator: setup, run scenarios, report, teardown."""

from __future__ import annotations

import multiprocessing
import signal
import time
from typing import Any

from compaction_stress.config import Config, parse_duration
from compaction_stress.iceberg_tracker import IcebergTracker
from compaction_stress.logging import DualLogger
from compaction_stress.metrics import MetricsScraper
from compaction_stress.offset_tracker import OffsetTracker
from compaction_stress.scenarios.base import (
    KGO_VERIFIER,
    STATS_SIZE,
    ScenarioHandle,
    VerifierWorker,
    start_verifier,
)
from compaction_stress.setup import run_setup


class Runner:
    def __init__(
        self,
        config: Config,
        scenario_name: str | None = None,
        set_cluster_config: bool = False,
    ):
        self.config = config
        self.scenario_name = scenario_name
        self.set_cluster_config = set_cluster_config
        self.logger = DualLogger(config.log_dir)
        self._shutdown_flag = False
        self.handles: list[ScenarioHandle] = []
        self.workers: list[VerifierWorker] = []
        self.scraper: MetricsScraper | None = None
        self.tracker: OffsetTracker | None = None
        self.iceberg_tracker: IcebergTracker | None = None

    def run(self) -> None:
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

        if not KGO_VERIFIER:
            self.logger.error("kgo-verifier not found. Build it or add to PATH.")
            self.logger.close()
            return

        self.logger.info(f"Using kgo-verifier: {KGO_VERIFIER}")

        enabled = self.config.enabled_scenarios(self.scenario_name)
        self.logger.info(f"Enabled scenarios: {', '.join(enabled)}")

        topic_map = run_setup(
            self.config, enabled,
            set_cluster_config=self.set_cluster_config,
            log_fn=self.logger.info,
            warn_fn=self.logger.warn,
        )

        if self._shutdown_flag:
            self.logger.info("Interrupted during setup, exiting.")
            self.logger.close()
            return

        if self.config.cluster.admin_hosts:
            admin_hosts = self.config.cluster.admin_hosts
            self.scraper = MetricsScraper(admin_hosts)
            self.scraper.set_warn_callback(self.logger.warn)
            self.scraper.start()
            self.logger.info(f"Metrics scraper started ({len(admin_hosts)} nodes)")

        if self._shutdown_flag:
            self._cleanup()
            return

        # Launch kgo-verifier processes.
        # One process per (producer, topic). num_producers controls how many
        # parallel kgo-verifier instances hit each topic.
        for name in enabled:
            if self._shutdown_flag:
                break
            topics = topic_map.get(name, [])
            sc = self.config.get_scenario(name)
            handle = ScenarioHandle(name, sc.num_topics, sc.msg_size)
            self.handles.append(handle)

            per_topic_rate = max(1024, sc.rate_limit_bps // max(len(topics), 1))

            for topic in topics:
                if self._shutdown_flag:
                    break
                stats = multiprocessing.Array('d', STATS_SIZE)
                handle.add_stats(stats)
                worker = start_verifier(
                    name, self.config.cluster, sc,
                    topic, per_topic_rate, stats,
                )
                self.workers.append(worker)

        if self._shutdown_flag:
            self._cleanup()
            return

        self.logger.info(f"Launched {len(self.workers)} kgo-verifier process(es)")

        all_topics = [t for topics in topic_map.values() for t in topics]
        self.tracker = OffsetTracker(
            self.config.cluster, all_topics,
            interval=self.config.report_interval,
        )
        self.tracker.set_warn_callback(self.logger.warn)
        self.tracker.start()
        self.logger.info(f"Offset tracker started for {len(all_topics)} topic(s)")

        # Start iceberg tracker if any iceberg topics and GCS bucket configured
        iceberg_topics = [
            t for name in enabled
            for t in topic_map.get(name, [])
            if self.config.get_scenario(name).topic_config.get("redpanda.iceberg.mode")
        ]
        if iceberg_topics and self.config.cluster.gcs_bucket:
            self.iceberg_tracker = IcebergTracker(
                self.config.cluster, iceberg_topics,
                interval=self.config.report_interval,
            )
            self.iceberg_tracker.set_warn_callback(self.logger.warn)
            self.iceberg_tracker.start()
            self.logger.info(f"Iceberg tracker started for {len(iceberg_topics)} topic(s)")

        duration = parse_duration(self.config.duration) if self.config.duration else None
        start_time = time.monotonic()

        self.logger.info(
            f"Running {'indefinitely' if duration is None else f'for {self.config.duration}'}"
        )

        while not self._shutdown_flag:
            elapsed = time.monotonic() - start_time
            if duration and elapsed >= duration:
                self.logger.info("Duration reached, shutting down...")
                break

            wait_time = self.config.report_interval
            if duration:
                remaining = duration - elapsed
                wait_time = min(wait_time, max(remaining, 0.5))

            deadline = time.monotonic() + wait_time
            while time.monotonic() < deadline and not self._shutdown_flag:
                time.sleep(0.5)

            if not self._shutdown_flag:
                self._report(time.monotonic() - start_time)

        self._report(time.monotonic() - start_time)
        self._cleanup()

    def _cleanup(self) -> None:
        if self.tracker:
            self.tracker.signal_stop()
        if self.iceberg_tracker:
            self.iceberg_tracker.signal_stop()
        if self.scraper:
            self.scraper.stop()

        if self.workers:
            self.logger.info("Shutting down kgo-verifier processes...")
            for w in self.workers:
                w.shutdown()

        self.logger.info("Done.")
        self.logger.close()

    def _report(self, elapsed: float) -> None:
        scenario_stats: dict[str, dict[str, Any]] = {}
        for h in self.handles:
            scenario_stats[h.name] = h.get_stats()

        cluster_metrics = self.scraper.get_metrics() if self.scraper else None
        offset_stats = self.tracker.get_stats() if self.tracker else None
        iceberg_stats = self.iceberg_tracker.get_stats() if self.iceberg_tracker else None
        self.logger.report(elapsed, scenario_stats, cluster_metrics, offset_stats, iceberg_stats)

    def _handle_signal(self, signum: int, frame: Any) -> None:
        if self._shutdown_flag:
            self.logger.info("Forced exit.")
            for w in self.workers:
                try:
                    w.proc.kill()
                except Exception:
                    pass
            import sys
            sys.exit(1)
        self.logger.info(f"Received signal {signum}, shutting down gracefully... (repeat to force)")
        self._shutdown_flag = True

"""Orchestrator: setup, run scenarios, report, teardown."""

from __future__ import annotations

import multiprocessing
import signal
import time
from typing import Any

from compaction_stress.config import Config, parse_duration
from compaction_stress.logging import DualLogger
from compaction_stress.metrics import MetricsScraper
from compaction_stress.offset_tracker import OffsetTracker
from compaction_stress.scenarios.base import (
    KGO_REPEATER,
    STATS_SIZE,
    RepeaterWorker,
    ScenarioHandle,
    start_repeater,
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
        self.workers: list[RepeaterWorker] = []
        self.scraper: MetricsScraper | None = None
        self.tracker: OffsetTracker | None = None

    def run(self) -> None:
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

        if not KGO_REPEATER:
            self.logger.error("kgo-repeater not found. Build it or add to PATH.")
            self.logger.close()
            return

        self.logger.info(f"Using kgo-repeater: {KGO_REPEATER}")

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

        # One kgo-repeater process per scenario.
        # num_producers maps to --workers (in-process parallelism).
        for name in enabled:
            if self._shutdown_flag:
                break
            topics = topic_map.get(name, [])
            sc = self.config.get_scenario(name)
            handle = ScenarioHandle(name, sc.num_topics, sc.msg_size)
            self.handles.append(handle)

            stats = multiprocessing.Array('d', STATS_SIZE)
            handle.set_stats(stats)

            group = f"ct-stress-{name}"
            worker = start_repeater(name, self.config.cluster, sc, topics, group, stats)
            self.workers.append(worker)

        if self._shutdown_flag:
            self._cleanup()
            return

        self.logger.info(f"Launched {len(self.workers)} kgo-repeater process(es)")

        all_topics = [t for topics in topic_map.values() for t in topics]
        self.tracker = OffsetTracker(
            self.config.cluster, all_topics,
            interval=self.config.report_interval,
        )
        self.tracker.set_warn_callback(self.logger.warn)
        self.tracker.start()
        self.logger.info(f"Offset tracker started for {len(all_topics)} topic(s)")

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
        if self.scraper:
            self.scraper.stop()

        if self.workers:
            self.logger.info("Shutting down kgo-repeater processes...")
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
        self.logger.report(elapsed, scenario_stats, cluster_metrics, offset_stats)

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

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
    STATS_SIZE,
    ScenarioHandle,
    key_prefixes_for,
    scenario_worker,
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
        self.shutdown = multiprocessing.Event()
        self.handles: list[ScenarioHandle] = []
        self.processes: list[multiprocessing.Process] = []
        self.scraper: MetricsScraper | None = None
        self.tracker: OffsetTracker | None = None

    def run(self) -> None:
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

        enabled = self.config.enabled_scenarios(self.scenario_name)
        self.logger.info(f"Enabled scenarios: {', '.join(enabled)}")

        topic_map = run_setup(
            self.config, enabled,
            set_cluster_config=self.set_cluster_config,
            log_fn=self.logger.info,
            warn_fn=self.logger.warn,
        )

        if self.shutdown.is_set():
            self.logger.info("Interrupted during setup, exiting.")
            self.logger.close()
            return

        if self.config.cluster.admin_hosts:
            admin_hosts = self.config.cluster.admin_hosts
            self.scraper = MetricsScraper(admin_hosts)
            self.scraper.set_warn_callback(self.logger.warn)
            self.scraper.start()
            self.logger.info(f"Metrics scraper started ({len(admin_hosts)} nodes)")

        if self.shutdown.is_set():
            self._cleanup()
            return

        # Launch each scenario in its own process
        for name in enabled:
            if self.shutdown.is_set():
                break
            topics = topic_map.get(name, [])
            sc = self.config.get_scenario(name)
            prefixes = key_prefixes_for(name, topics)
            stats = multiprocessing.Array('d', STATS_SIZE)

            p = multiprocessing.Process(
                target=scenario_worker,
                args=(name, self.config.cluster, sc, topics, prefixes,
                      self.shutdown, stats),
                name=f"scenario-{name}",
                daemon=True,
            )
            self.processes.append(p)
            self.handles.append(ScenarioHandle(name, sc.num_topics, stats))
            p.start()

        if self.shutdown.is_set():
            self._cleanup()
            return

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

        while not self.shutdown.is_set():
            elapsed = time.monotonic() - start_time
            if duration and elapsed >= duration:
                self.logger.info("Duration reached, shutting down...")
                self.shutdown.set()
                break

            # Wait for either the report interval or remaining duration,
            # whichever is shorter.
            wait_time = self.config.report_interval
            if duration:
                remaining = duration - elapsed
                wait_time = min(wait_time, max(remaining, 0.5))

            self.shutdown.wait(wait_time)
            if not self.shutdown.is_set():
                self._report(time.monotonic() - start_time)

        self._report(time.monotonic() - start_time)
        self._cleanup()

    def _cleanup(self) -> None:
        # Signal background threads to stop (don't block waiting —
        # they're daemon threads and will die on process exit if stuck
        # in blocking I/O).
        if self.tracker:
            self.tracker.signal_stop()
        if self.scraper:
            self.scraper.stop()

        if self.processes:
            self.logger.info("Waiting for scenarios to finish...")
            for p in self.processes:
                p.join(timeout=10)
                if p.is_alive():
                    self.logger.warn(f"Terminating stuck process: {p.name}")
                    p.terminate()
                    p.join(timeout=5)

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
        if self.shutdown.is_set():
            # Second signal — force exit immediately
            self.logger.info("Forced exit.")
            for p in self.processes:
                if p.is_alive():
                    p.terminate()
            import sys
            sys.exit(1)
        self.logger.info(f"Received signal {signum}, shutting down gracefully... (repeat to force)")
        self.shutdown.set()

"""Orchestrator: setup, run scenarios, report, teardown."""

from __future__ import annotations

import signal
import threading
import time
from typing import Any

from compaction_stress.config import Config, parse_duration
from compaction_stress.logging import DualLogger
from compaction_stress.metrics import MetricsScraper
from compaction_stress.offset_tracker import OffsetTracker
from compaction_stress.scenarios import create_scenario
from compaction_stress.scenarios.base import BaseScenario
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
        self.shutdown = threading.Event()
        self.scenarios: list[BaseScenario] = []
        self.threads: list[threading.Thread] = []
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

        if self.config.cluster.admin_hosts:
            admin_hosts = self.config.cluster.admin_hosts
            self.scraper = MetricsScraper(admin_hosts)
            self.scraper.set_warn_callback(self.logger.warn)
            self.scraper.start()
            self.logger.info(f"Metrics scraper started ({len(admin_hosts)} nodes)")

        for name in enabled:
            topics = topic_map.get(name, [])
            scenario = create_scenario(
                name=name,
                cluster=self.config.cluster,
                config=self.config,
                topics=topics,
                logger=self.logger,
            )
            self.scenarios.append(scenario)
            t = threading.Thread(
                target=self._run_scenario,
                args=(scenario,),
                name=f"scenario-{name}",
                daemon=True,
            )
            self.threads.append(t)
            t.start()

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
            self.shutdown.wait(self.config.report_interval)
            if not self.shutdown.is_set():
                self._report(time.monotonic() - start_time)

        self._report(time.monotonic() - start_time)

        self.logger.info("Waiting for scenarios to finish...")
        for t in self.threads:
            t.join(timeout=60)

        if self.tracker:
            self.tracker.stop()
        if self.scraper:
            self.scraper.stop()

        self.logger.info("Done.")
        self.logger.close()

    def _run_scenario(self, scenario: BaseScenario) -> None:
        try:
            scenario.run(self.shutdown)
        except Exception as e:
            self.logger.error(f"[{scenario.name}] Error: {e}")

    def _report(self, elapsed: float) -> None:
        scenario_stats: dict[str, dict[str, Any]] = {}
        for s in self.scenarios:
            scenario_stats[s.name] = s.get_stats()

        cluster_metrics = self.scraper.get_metrics() if self.scraper else None
        offset_stats = self.tracker.get_stats() if self.tracker else None
        self.logger.report(elapsed, scenario_stats, cluster_metrics, offset_stats)

    def _handle_signal(self, signum: int, frame: Any) -> None:
        self.logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.shutdown.set()

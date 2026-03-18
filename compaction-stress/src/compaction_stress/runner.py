"""Orchestrator: setup, run scenarios, report, teardown."""

from __future__ import annotations

import multiprocessing
import signal
import subprocess
import time
from typing import Any

from compaction_stress.config import Config, parse_duration
from compaction_stress.logging import DualLogger
from compaction_stress.metrics import MetricsScraper
from compaction_stress.offset_tracker import OffsetTracker
from compaction_stress.scenarios.base import (
    GO_BINARY,
    STATS_SIZE,
    ScenarioHandle,
    key_prefixes_for,
    scenario_worker,
    start_go_worker,
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
        self._shutdown_flag = False  # plain bool for signal-responsive polling
        self.handles: list[ScenarioHandle] = []
        self.processes: list[multiprocessing.Process] = []  # Python fallback
        self.go_procs: list[subprocess.Popen] = []  # Go subprocesses
        self.scraper: MetricsScraper | None = None
        self.tracker: OffsetTracker | None = None
        self._use_go = GO_BINARY is not None

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

        # Launch producer processes
        if self._use_go:
            self.logger.info(f"Using Go producer: {GO_BINARY}")
        else:
            self.logger.info("Go producer not found, using Python fallback")

        for name in enabled:
            if self._shutdown_flag:
                break
            topics = topic_map.get(name, [])
            sc = self.config.get_scenario(name)
            prefixes = key_prefixes_for(name, topics)
            handle = ScenarioHandle(name, sc.num_topics)
            self.handles.append(handle)

            if self._use_go:
                # Go mode: one subprocess per worker, each handling all topics.
                # Single franz-go client per process = fewer broker connections.
                worker_rate = max(1024, sc.rate_limit_bps // sc.num_producers)
                prefix = prefixes[0]  # all topics use the same prefix (except multi_partition)
                for wid in range(sc.num_producers):
                    stats = multiprocessing.Array('d', STATS_SIZE)
                    handle.add_worker_stats(stats)
                    proc = start_go_worker(
                        name, wid, self.config.cluster, sc,
                        topics, prefix, worker_rate, stats,
                    )
                    self.go_procs.append(proc)
            else:
                # Python fallback: one process per worker
                for wid in range(sc.num_producers):
                    stats = multiprocessing.Array('d', STATS_SIZE)
                    handle.add_worker_stats(stats)
                    p = multiprocessing.Process(
                        target=scenario_worker,
                        args=(name, wid, sc.num_producers, self.config.cluster,
                              sc, topics, prefixes, self.shutdown, stats),
                        name=f"scenario-{name}-w{wid}",
                        daemon=True,
                    )
                    self.processes.append(p)
                    p.start()

        if self._shutdown_flag:
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

        while not self._shutdown_flag:
            elapsed = time.monotonic() - start_time
            if duration and elapsed >= duration:
                self.logger.info("Duration reached, shutting down...")
                self.shutdown.set()
                break

            # Poll with short sleeps so signals are handled promptly.
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

        has_workers = self.processes or self.go_procs
        if has_workers:
            self.logger.info("Waiting for scenarios to finish...")

        # Terminate Go subprocesses
        for proc in self.go_procs:
            if proc.poll() is None:
                proc.terminate()
        for proc in self.go_procs:
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.logger.warn(f"Killing stuck Go process pid={proc.pid}")
                proc.kill()
                proc.wait(timeout=5)

        # Join Python fallback processes
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
        if self._shutdown_flag:
            # Second signal — force exit immediately
            self.logger.info("Forced exit.")
            for proc in self.go_procs:
                if proc.poll() is None:
                    proc.kill()
            for p in self.processes:
                if p.is_alive():
                    p.terminate()
            import sys
            sys.exit(1)
        self.logger.info(f"Received signal {signum}, shutting down gracefully... (repeat to force)")
        self._shutdown_flag = True
        self.shutdown.set()

"""Orchestrator: setup, run scenarios, report, teardown."""

from __future__ import annotations

import multiprocessing
import signal
import time
from typing import Any

from compaction_stress.config import Config, parse_duration
from compaction_stress.iceberg_tracker import IcebergTracker
from compaction_stress.avro_producer import AVRO_STATS_SIZE, avro_worker
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
        no_offset_tracker: bool = False,
    ):
        self.config = config
        self.scenario_name = scenario_name
        self.set_cluster_config = set_cluster_config
        self.no_offset_tracker = no_offset_tracker
        self.logger = DualLogger(config.log_dir)
        self._shutdown_flag = False
        self.shutdown_event = multiprocessing.Event()
        self.handles: list[ScenarioHandle] = []
        self.workers: list[VerifierWorker] = []
        self.avro_procs: list[multiprocessing.Process] = []
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

        # Launch producer processes per scenario.
        # For iceberg: topic 0 gets Avro producers, remaining topics get
        # kgo-verifier for throughput. All other scenarios use kgo-verifier.
        for name in enabled:
            if self._shutdown_flag:
                break
            topics = topic_map.get(name, [])
            sc = self.config.get_scenario(name)
            is_iceberg = name == "iceberg"

            # Use msg_size=1 for Avro handle (bytes reported directly)
            handle = ScenarioHandle(name, sc.num_topics, sc.msg_size)
            self.handles.append(handle)

            for i, topic in enumerate(topics):
                if self._shutdown_flag:
                    break

                if is_iceberg and i == 0 and self.config.cluster.schema_registry_url:
                    # Topic 0: Avro producers for schema-based translation
                    for wid in range(sc.num_producers):
                        stats = multiprocessing.Array('d', AVRO_STATS_SIZE)
                        handle.add_stats(stats, avro=True)
                        p = multiprocessing.Process(
                            target=avro_worker,
                            args=(name, wid, sc.num_producers,
                                  self.config.cluster, sc, topic,
                                  self.shutdown_event, stats),
                            name=f"avro-{name}-{topic}-w{wid}",
                            daemon=True,
                        )
                        self.avro_procs.append(p)
                        p.start()
                else:
                    # kgo-verifier for everything else
                    per_topic_rate = max(1024, sc.rate_limit_bps // max(len(topics), 1))
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

        total = len(self.workers) + len(self.avro_procs)
        self.logger.info(f"Launched {total} producer process(es) "
                         f"({len(self.workers)} kgo-verifier, {len(self.avro_procs)} avro)")

        all_topics = [t for topics in topic_map.values() for t in topics]
        if not self.no_offset_tracker:
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

        # Backfill phase tracking
        backfill_toggled = False
        backfill_duration = None
        backfill_topics: list[str] = []
        if "iceberg_backfill" in enabled:
            bf_sc = self.config.get_scenario("iceberg_backfill")
            backfill_duration = parse_duration(bf_sc.produce_phase_duration)
            backfill_topics = topic_map.get("iceberg_backfill", [])
            if backfill_duration and backfill_topics:
                self.logger.info(
                    f"Backfill: producing for {bf_sc.produce_phase_duration} "
                    f"before enabling iceberg on {len(backfill_topics)} topic(s)"
                )

        self.logger.info(
            f"Running {'indefinitely' if duration is None else f'for {self.config.duration}'}"
        )

        while not self._shutdown_flag:
            elapsed = time.monotonic() - start_time
            if duration and elapsed >= duration:
                self.logger.info("Duration reached, shutting down...")
                break

            # Backfill: toggle iceberg on after produce phase
            if (not backfill_toggled and backfill_duration
                    and backfill_topics and elapsed >= backfill_duration):
                backfill_toggled = True
                self._enable_iceberg_on_topics(backfill_topics)

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

    def _enable_iceberg_on_topics(self, topics: list[str]) -> None:
        """Toggle iceberg mode on topics via rpk topic alter."""
        from compaction_stress.setup import _rpk_kafka_args, _run_rpk

        self.logger.info(f"Backfill: enabling iceberg key_value mode on {len(topics)} topic(s)...")
        base = _rpk_kafka_args(self.config.cluster)
        for topic in topics:
            args = base + [
                "topic", "alter-config", topic,
                "--set", "redpanda.iceberg.mode=key_value",
            ]
            ok = _run_rpk(args, warn_fn=self.logger.warn)
            if ok:
                self.logger.info(f"  {topic}: iceberg enabled")
            else:
                self.logger.warn(f"  {topic}: failed to enable iceberg")

        # Start iceberg tracker for the backfill topics
        if self.config.cluster.gcs_bucket and not self.iceberg_tracker:
            self.iceberg_tracker = IcebergTracker(
                self.config.cluster, topics,
                interval=self.config.report_interval,
            )
            self.iceberg_tracker.set_warn_callback(self.logger.warn)
            self.iceberg_tracker.start()
            self.logger.info(f"Iceberg tracker started for {len(topics)} backfill topic(s)")

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

        if self.avro_procs:
            self.logger.info("Shutting down Avro producers...")
            self.shutdown_event.set()
            for p in self.avro_procs:
                p.join(timeout=15)
                if p.is_alive():
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
            for p in self.avro_procs:
                try:
                    p.terminate()
                except Exception:
                    pass
            import sys
            sys.exit(1)
        self.logger.info(f"Received signal {signum}, shutting down gracefully... (repeat to force)")
        self._shutdown_flag = True
        self.shutdown_event.set()

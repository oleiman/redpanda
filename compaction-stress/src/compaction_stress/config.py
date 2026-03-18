"""Configuration dataclasses and YAML loading."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any

import yaml


@dataclass
class ClusterConfig:
    brokers: str = "localhost:9092"
    sasl_mechanism: str | None = None
    sasl_user: str | None = None
    sasl_password: str | None = None
    tls_enabled: bool = False
    admin_hosts: list[str] = field(default_factory=list)


@dataclass
class ScenarioConfig:
    enabled: bool = True
    key_count: int = 10000
    msg_size: int = 512
    rate_limit_bps: int = 100 * 1024 * 1024  # 100 MB/s
    partitions: int = 4
    replicas: int = 3
    num_topics: int = 1
    num_producers: int = 1
    tombstone_probability: float = 0.0
    topic_config: dict[str, str] = field(default_factory=dict)


# Built-in defaults per scenario (merged on top of global defaults)
# Scenario defaults designed to overwhelm compaction on a 6-node cluster.
# Key insight: compaction throughput is bounded by key_map_memory (controls
# how many keys can be deduplicated per pass) and scheduler concurrency
# (one job per partition at a time). To stress it, we need:
# - High key cardinality relative to key_map capacity (forces multi-pass)
# - Many partitions (saturates scheduler slots)
# - High write rate (dirty data accumulates faster than compaction drains)
# - Multiple producer processes (breaks single-process ~60 MB/s ceiling)
SCENARIO_DEFAULTS: dict[str, dict[str, Any]] = {
    "key_cardinality": {
        # 500K unique keys with default 128MB map = fits in one pass.
        # Set cloud_topics_compaction_key_map_memory to 1-4MB to force
        # multi-pass (each pass handles ~25K-100K keys).
        "key_count": 500_000,
        "msg_size": 512,
        "rate_limit_bps": 200 * 1024 * 1024,
        "num_producers": 3,
        "partitions": 8,
        "topic_config": {"min.cleanable.dirty.ratio": "0.0"},
    },
    "extreme_dedup": {
        # 100 keys, each updated millions of times. Each compaction pass
        # must read and discard enormous amounts of data for minimal output.
        "key_count": 100,
        "msg_size": 1024,
        "rate_limit_bps": 200 * 1024 * 1024,
        "num_producers": 3,
        "partitions": 4,
    },
    "continuous_write": {
        # High key count + compaction lag = compaction always has a moving
        # frontier it can't reach. Many partitions spread the load.
        "key_count": 200_000,
        "msg_size": 512,
        "rate_limit_bps": 200 * 1024 * 1024,
        "num_producers": 3,
        "partitions": 12,
        "topic_config": {
            "min.compaction.lag.ms": "15000",
            "min.cleanable.dirty.ratio": "0.0",
        },
    },
    "tombstone": {
        # High key churn with short delete.retention.ms — keys are
        # overwritten rapidly, and short retention means old versions
        # become eligible for removal quickly. This stresses the
        # tombstone/retention cleanup path in compaction.
        # NOTE: kgo-repeater doesn't support per-message tombstones
        # (it generates payload once at init), so we rely on key churn
        # + retention instead.
        "key_count": 5_000,
        "msg_size": 512,
        "rate_limit_bps": 150 * 1024 * 1024,
        "num_producers": 3,
        "partitions": 8,
        "topic_config": {
            "delete.retention.ms": "30000",
            "min.cleanable.dirty.ratio": "0.0",
        },
    },
    "multi_partition": {
        # 6 topics x 16 partitions = 96 partitions competing for
        # compaction scheduler slots across 6 nodes.
        "key_count": 50_000,
        "msg_size": 512,
        "rate_limit_bps": 300 * 1024 * 1024,
        "num_producers": 4,
        "num_topics": 6,
        "partitions": 16,
    },
}

ALL_SCENARIOS = list(SCENARIO_DEFAULTS.keys())


@dataclass
class Config:
    cluster: ClusterConfig = field(default_factory=ClusterConfig)
    cluster_config: dict[str, Any] = field(default_factory=lambda: {
        "cloud_topics_enabled": True,
        "cloud_topics_compaction_interval_ms": 5000,
        "cloud_topics_compaction_max_object_size": 128 * 1024 * 1024,
        "cloud_topics_compaction_key_map_memory": 128 * 1024 * 1024,
    })
    scenarios: dict[str, ScenarioConfig] = field(default_factory=dict)
    duration: str | None = "1h"  # None = indefinite
    no_setup: bool = False
    delete_existing_topics: bool = False
    rate_multiplier: float = 1.0
    log_dir: str = "./logs"
    report_interval: int = 30  # seconds between stdout reports

    def get_scenario(self, name: str) -> ScenarioConfig:
        if name in self.scenarios:
            sc = self.scenarios[name]
        else:
            sc = _build_scenario_config(name, {})
        if self.rate_multiplier != 1.0:
            sc.rate_limit_bps = int(sc.rate_limit_bps * self.rate_multiplier)
        return sc

    def enabled_scenarios(self, selected: str | None) -> list[str]:
        if selected and selected != "kitchen_sink":
            return [selected]
        return [
            name for name in ALL_SCENARIOS
            if self.get_scenario(name).enabled
        ]


def _build_scenario_config(
    name: str,
    overrides: dict[str, Any],
) -> ScenarioConfig:
    merged: dict[str, Any] = {}
    if name in SCENARIO_DEFAULTS:
        merged.update(SCENARIO_DEFAULTS[name])
    merged.update(overrides)
    topic_config = merged.pop("topic_config", {})
    sc = ScenarioConfig(**{
        k: v for k, v in merged.items()
        if k in ScenarioConfig.__dataclass_fields__
    })
    sc.topic_config = {
        **{"cleanup.policy": "compact", "min.cleanable.dirty.ratio": "0.01"},
        **topic_config,
    }
    return sc


def load_config(
    config_path: str | None,
    cli_overrides: dict[str, Any],
) -> Config:
    raw: dict[str, Any] = {}
    if config_path:
        with open(config_path) as f:
            raw = yaml.safe_load(f) or {}

    cluster_raw = raw.get("cluster", {})
    if cli_overrides.get("brokers"):
        cluster_raw["brokers"] = cli_overrides["brokers"]
    elif os.environ.get("REDPANDA_BROKERS"):
        cluster_raw["brokers"] = os.environ["REDPANDA_BROKERS"]
    if cli_overrides.get("admin_hosts"):
        cluster_raw["admin_hosts"] = cli_overrides["admin_hosts"]

    if os.environ.get("REDPANDA_SASL_USER"):
        cluster_raw["sasl_user"] = os.environ["REDPANDA_SASL_USER"]
    if os.environ.get("REDPANDA_SASL_PASSWORD"):
        cluster_raw["sasl_password"] = os.environ["REDPANDA_SASL_PASSWORD"]
    if os.environ.get("REDPANDA_SASL_MECHANISM"):
        cluster_raw["sasl_mechanism"] = os.environ["REDPANDA_SASL_MECHANISM"]

    cluster = ClusterConfig(**{
        k: v for k, v in cluster_raw.items()
        if k in ClusterConfig.__dataclass_fields__
    })

    scenarios_raw = raw.get("scenarios", {})
    defaults_raw = raw.get("defaults", {})
    scenarios: dict[str, ScenarioConfig] = {}
    for name in ALL_SCENARIOS:
        scenario_overrides = {**defaults_raw, **scenarios_raw.get(name, {})}
        scenarios[name] = _build_scenario_config(name, scenario_overrides)

    config = Config(
        cluster=cluster,
        cluster_config=raw.get("cluster_config", {
            "cloud_topics_enabled": True,
            "cloud_topics_compaction_interval_ms": 5000,
            "cloud_topics_compaction_max_object_size": 128 * 1024 * 1024,
            "cloud_topics_compaction_key_map_memory": 128 * 1024 * 1024,
        }),
        scenarios=scenarios,
        duration=cli_overrides.get("duration", raw.get("duration", "1h")),
        no_setup=cli_overrides.get("no_setup", False),
        delete_existing_topics=cli_overrides.get("delete_existing_topics", False),
        rate_multiplier=float(cli_overrides.get("rate_multiplier", raw.get("rate_multiplier", 1.0))),
        log_dir=cli_overrides.get("log_dir", raw.get("log_dir", "./logs")),
        report_interval=raw.get("report_interval", 30),
    )
    return config


def parse_duration(duration_str: str) -> float | None:
    """Parse duration string like '1h', '30m', '2h30m', '45s' to seconds.
    Returns None for 'indefinite' or empty string."""
    if not duration_str or duration_str.lower() == "indefinite":
        return None
    total = 0.0
    current = ""
    for ch in duration_str:
        if ch.isdigit() or ch == ".":
            current += ch
        elif ch == "h":
            total += float(current) * 3600
            current = ""
        elif ch == "m":
            total += float(current) * 60
            current = ""
        elif ch == "s":
            total += float(current)
            current = ""
    if current:
        total += float(current)
    return total if total > 0 else None

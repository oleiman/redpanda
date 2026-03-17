"""rpk-based cluster and topic setup."""

from __future__ import annotations

import subprocess
from typing import Any

from compaction_stress.config import ClusterConfig, Config, ScenarioConfig


def _rpk_base_args(cluster: ClusterConfig) -> list[str]:
    args = ["rpk", "--brokers", cluster.brokers]
    if cluster.sasl_mechanism and cluster.sasl_user:
        args += [
            "--sasl-mechanism", cluster.sasl_mechanism,
            "--user", cluster.sasl_user,
            "--password", cluster.sasl_password or "",
        ]
    if cluster.tls_enabled:
        args += ["--tls-enabled"]
    return args


def _run_rpk(args: list[str], warn_fn: Any = None) -> bool:
    try:
        result = subprocess.run(
            args,
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode != 0:
            stderr = result.stderr.strip()
            if "already exists" in stderr.lower() or "TOPIC_ALREADY_EXISTS" in stderr:
                if warn_fn:
                    warn_fn(f"Topic already exists (continuing): {stderr}")
                return True
            if warn_fn:
                warn_fn(f"rpk command failed: {' '.join(args)}\n{stderr}")
            return False
        return True
    except FileNotFoundError:
        if warn_fn:
            warn_fn("rpk not found on PATH — skipping setup")
        return False
    except subprocess.TimeoutExpired:
        if warn_fn:
            warn_fn(f"rpk command timed out: {' '.join(args)}")
        return False


def set_cluster_configs(
    cluster: ClusterConfig,
    configs: dict[str, Any],
    log_fn: Any = None,
    warn_fn: Any = None,
) -> None:
    base = _rpk_base_args(cluster)
    for key, value in configs.items():
        if log_fn:
            log_fn(f"Setting cluster config: {key}={value}")
        args = base + ["cluster", "config", "set", key, str(value)]
        _run_rpk(args, warn_fn=warn_fn)


def topic_name(scenario: str, index: int = 0) -> str:
    return f"ct-stress-{scenario}-{index}"


def create_topics(
    cluster: ClusterConfig,
    scenario_name: str,
    scenario_config: ScenarioConfig,
    log_fn: Any = None,
    warn_fn: Any = None,
) -> list[str]:
    base = _rpk_base_args(cluster)
    topics: list[str] = []
    num_topics = scenario_config.num_topics

    for i in range(num_topics):
        name = topic_name(scenario_name, i)
        topics.append(name)

        args = base + [
            "topic", "create", name,
            "--partitions", str(scenario_config.partitions),
            "--replicas", str(scenario_config.replicas),
        ]
        all_topic_config = {
            "cleanup.policy": "compact",
            **scenario_config.topic_config,
        }
        for k, v in all_topic_config.items():
            args += ["-c", f"{k}={v}"]

        if log_fn:
            log_fn(f"Creating topic: {name} (partitions={scenario_config.partitions}, replicas={scenario_config.replicas})")
        _run_rpk(args, warn_fn=warn_fn)

    return topics


def run_setup(
    config: Config,
    enabled_scenarios: list[str],
    log_fn: Any = None,
    warn_fn: Any = None,
) -> dict[str, list[str]]:
    """Run full setup: cluster configs + topics. Returns scenario -> topic names."""
    if config.no_setup:
        if log_fn:
            log_fn("Skipping setup (--no-setup)")
        result = {}
        for name in enabled_scenarios:
            sc = config.get_scenario(name)
            result[name] = [topic_name(name, i) for i in range(sc.num_topics)]
        return result

    set_cluster_configs(
        config.cluster, config.cluster_config,
        log_fn=log_fn, warn_fn=warn_fn,
    )

    result: dict[str, list[str]] = {}
    for name in enabled_scenarios:
        sc = config.get_scenario(name)
        result[name] = create_topics(
            config.cluster, name, sc,
            log_fn=log_fn, warn_fn=warn_fn,
        )
    return result

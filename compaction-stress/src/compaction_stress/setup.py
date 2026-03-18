"""rpk-based cluster and topic setup."""

from __future__ import annotations

import json
import subprocess
from typing import Any

from compaction_stress.config import ClusterConfig, Config, ScenarioConfig


def _rpk_kafka_args(cluster: ClusterConfig) -> list[str]:
    """Build rpk args for Kafka-protocol commands (topic create, etc.)."""
    args = ["rpk", "-X", f"brokers={cluster.brokers}"]
    if cluster.sasl_mechanism and cluster.sasl_user:
        args += [
            "-X", f"sasl.mechanism={cluster.sasl_mechanism}",
            "-X", f"user={cluster.sasl_user}",
            "-X", f"pass={cluster.sasl_password or ''}",
        ]
    if cluster.tls_enabled:
        args += ["-X", "tls.enabled=true"]
    return args


def _rpk_admin_args(cluster: ClusterConfig) -> list[str]:
    """Build rpk args for admin API commands (cluster config, etc.)."""
    if cluster.admin_hosts:
        hosts = ",".join(cluster.admin_hosts)
    else:
        # Derive admin host from broker address, default admin port 9644
        broker = cluster.brokers.split(",")[0]
        host = broker.rsplit(":", 1)[0]
        hosts = f"{host}:9644"
    args = ["rpk", "-X", f"admin.hosts={hosts}"]
    if cluster.sasl_mechanism and cluster.sasl_user:
        args += [
            "-X", f"sasl.mechanism={cluster.sasl_mechanism}",
            "-X", f"user={cluster.sasl_user}",
            "-X", f"pass={cluster.sasl_password or ''}",
        ]
    if cluster.tls_enabled:
        args += ["-X", "admin.tls.enabled=true"]
    return args


def discover_admin_hosts(
    cluster: ClusterConfig,
    admin_port: int = 9644,
    warn_fn: Any = None,
) -> list[str]:
    """Discover admin API hosts via `rpk cluster info`.

    Parses broker addresses from the cluster and replaces the Kafka port
    with the admin API port.
    """
    args = _rpk_kafka_args(cluster) + ["cluster", "info", "--format", "json"]
    try:
        result = subprocess.run(
            args, capture_output=True, text=True, timeout=30,
        )
        if result.returncode != 0:
            if warn_fn:
                warn_fn(f"rpk cluster info failed: {result.stderr.strip()}")
            return []
        info = json.loads(result.stdout)
        hosts: list[str] = []
        for broker in info.get("brokers", []):
            host = broker.get("host", "")
            if host:
                hosts.append(f"{host}:{admin_port}")
        return hosts
    except (FileNotFoundError, subprocess.TimeoutExpired, json.JSONDecodeError) as e:
        if warn_fn:
            warn_fn(f"Admin host discovery failed: {e}")
        return []


def _run_rpk(args: list[str], warn_fn: Any = None) -> bool:
    try:
        result = subprocess.run(
            args,
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode != 0:
            output = (result.stdout + result.stderr).strip()
            if "TOPIC_ALREADY_EXISTS" in output or "already exists" in output.lower():
                if warn_fn:
                    warn_fn(f"Topic already exists (continuing)")
                return True
            if warn_fn:
                warn_fn(f"rpk command failed: {' '.join(args)}\n{output}")
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
    base = _rpk_admin_args(cluster)
    for key, value in configs.items():
        if log_fn:
            log_fn(f"Setting cluster config: {key}={value}")
        str_value = str(value).lower() if isinstance(value, bool) else str(value)
        args = base + ["cluster", "config", "set", key, str_value]
        _run_rpk(args, warn_fn=warn_fn)


def topic_name(scenario: str, index: int = 0) -> str:
    return f"ct-stress-{scenario}-{index}"


def delete_topic(
    cluster: ClusterConfig,
    name: str,
    log_fn: Any = None,
    warn_fn: Any = None,
) -> None:
    args = _rpk_kafka_args(cluster) + ["topic", "delete", name]
    if log_fn:
        log_fn(f"Deleting topic: {name}")
    _run_rpk(args, warn_fn=warn_fn)


def create_topics(
    cluster: ClusterConfig,
    scenario_name: str,
    scenario_config: ScenarioConfig,
    log_fn: Any = None,
    warn_fn: Any = None,
) -> list[str]:
    base = _rpk_kafka_args(cluster)
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
            "redpanda.storage.mode": "cloud",
            **scenario_config.topic_config,
        }
        # For iceberg scenario: topic 0 gets Avro mode, rest get key_value
        if scenario_name == "iceberg" and i == 0:
            all_topic_config["redpanda.iceberg.mode"] = "value_schema_id_prefix"
        elif scenario_name == "iceberg":
            all_topic_config["redpanda.iceberg.mode"] = "key_value"
        for k, v in all_topic_config.items():
            args += ["-c", f"{k}={v}"]

        if log_fn:
            log_fn(f"Creating topic: {name} (partitions={scenario_config.partitions}, replicas={scenario_config.replicas})")
        _run_rpk(args, warn_fn=warn_fn)

    return topics


def prompt_for_cluster_configs(
    configs: dict[str, Any],
    log_fn: Any = None,
) -> None:
    """Print recommended cluster configs and wait for user confirmation."""
    if not configs:
        return
    if log_fn:
        log_fn("Recommended cluster configs for this workload:")
    for key, value in configs.items():
        if isinstance(value, bool):
            value = str(value).lower()
        print(f"  rpk cluster config set {key} {value}")
    print()
    input("Press Enter once cluster configs are set (or Ctrl-C to abort)...")


def run_setup(
    config: Config,
    enabled_scenarios: list[str],
    set_cluster_config: bool = False,
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

    if set_cluster_config:
        set_cluster_configs(
            config.cluster, config.cluster_config,
            log_fn=log_fn, warn_fn=warn_fn,
        )
    else:
        prompt_for_cluster_configs(
            config.cluster_config, log_fn=log_fn,
        )

    if config.delete_existing_topics:
        for name in enabled_scenarios:
            sc = config.get_scenario(name)
            for i in range(sc.num_topics):
                delete_topic(
                    config.cluster, topic_name(name, i),
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

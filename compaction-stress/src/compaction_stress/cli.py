"""CLI entry point."""

from __future__ import annotations

import argparse
import sys

from compaction_stress.config import ALL_SCENARIOS, load_config
from compaction_stress.runner import Runner


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="compaction-stress",
        description="Cloud topics compaction stress workload generator for Redpanda",
    )
    parser.add_argument(
        "--config", "-c",
        help="Path to YAML config file",
    )
    parser.add_argument(
        "--brokers", "-b",
        help="Kafka broker addresses (comma-separated), overrides config file",
    )
    parser.add_argument(
        "--admin-hosts",
        help="Admin API hosts for metrics scraping (comma-separated)",
    )
    parser.add_argument(
        "--scenario", "-s",
        choices=ALL_SCENARIOS + ["kitchen_sink"],
        default="kitchen_sink",
        help="Scenario to run (default: kitchen_sink = all enabled)",
    )
    parser.add_argument(
        "--duration", "-d",
        help="Run duration (e.g., '1h', '30m', '12h'). Default from config or '1h'",
    )
    parser.add_argument(
        "--indefinite",
        action="store_true",
        help="Run indefinitely until Ctrl-C",
    )
    parser.add_argument(
        "--no-setup",
        action="store_true",
        help="Skip cluster config and topic creation",
    )
    parser.add_argument(
        "--delete-existing-topics",
        action="store_true",
        help="Delete and recreate topics if they already exist",
    )
    parser.add_argument(
        "--set-cluster-config",
        action="store_true",
        help="Set cluster configs via admin API (requires admin API access)",
    )
    parser.add_argument(
        "--rate-multiplier",
        type=float,
        help="Multiply all scenario rate limits by this factor (e.g., 2.0 = double, 0.5 = half)",
    )
    parser.add_argument(
        "--log-dir",
        help="Directory for JSON log output (default: ./logs)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    cli_overrides: dict = {}
    if args.brokers:
        cli_overrides["brokers"] = args.brokers
    if args.admin_hosts:
        cli_overrides["admin_hosts"] = [h.strip() for h in args.admin_hosts.split(",")]
    if args.indefinite:
        cli_overrides["duration"] = "indefinite"
    elif args.duration:
        cli_overrides["duration"] = args.duration
    if args.no_setup:
        cli_overrides["no_setup"] = True
    if args.delete_existing_topics:
        cli_overrides["delete_existing_topics"] = True
    if args.rate_multiplier is not None:
        cli_overrides["rate_multiplier"] = args.rate_multiplier
    if args.log_dir:
        cli_overrides["log_dir"] = args.log_dir

    config = load_config(args.config, cli_overrides)

    scenario = args.scenario if args.scenario != "kitchen_sink" else None
    runner = Runner(
        config,
        scenario_name=scenario,
        set_cluster_config=args.set_cluster_config,
    )
    runner.run()


if __name__ == "__main__":
    main()

"""Kitchen sink: runs all enabled scenarios concurrently."""

from __future__ import annotations

import threading
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from compaction_stress.config import ClusterConfig, Config
    from compaction_stress.logging import DualLogger

from compaction_stress.scenarios.base import BaseScenario
from compaction_stress.scenarios.key_cardinality import KeyCardinalityScenario
from compaction_stress.scenarios.extreme_dedup import ExtremeDedupScenario
from compaction_stress.scenarios.continuous_write import ContinuousWriteScenario
from compaction_stress.scenarios.tombstone import TombstoneScenario
from compaction_stress.scenarios.multi_partition import MultiPartitionScenario

SCENARIO_CLASSES: dict[str, type[BaseScenario]] = {
    "key_cardinality": KeyCardinalityScenario,
    "extreme_dedup": ExtremeDedupScenario,
    "continuous_write": ContinuousWriteScenario,
    "tombstone": TombstoneScenario,
    "multi_partition": MultiPartitionScenario,
}


def create_scenario(
    name: str,
    cluster: "ClusterConfig",
    config: "Config",
    topics: list[str],
    logger: "DualLogger",
) -> BaseScenario:
    cls = SCENARIO_CLASSES.get(name, BaseScenario)
    return cls(
        cluster=cluster,
        config=config.get_scenario(name),
        topics=topics,
        logger=logger,
    )

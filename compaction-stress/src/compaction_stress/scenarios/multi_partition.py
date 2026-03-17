"""Multi-partition fan-out scenario."""

from compaction_stress.scenarios.base import BaseScenario


class MultiPartitionScenario(BaseScenario):
    name = "multi_partition"

    def _key_prefix(self, topic: str) -> str:
        idx = self.topics.index(topic) if topic in self.topics else 0
        return f"mp{idx}"

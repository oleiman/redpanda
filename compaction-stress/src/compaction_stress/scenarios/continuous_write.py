"""Continuous write pressure scenario."""

from compaction_stress.scenarios.base import BaseScenario


class ContinuousWriteScenario(BaseScenario):
    name = "continuous_write"

    def _key_prefix(self, topic: str) -> str:
        return "cw"

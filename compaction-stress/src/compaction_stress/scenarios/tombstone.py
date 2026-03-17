"""Tombstone pressure scenario."""

from compaction_stress.scenarios.base import BaseScenario


class TombstoneScenario(BaseScenario):
    name = "tombstone"

    def _key_prefix(self, topic: str) -> str:
        return "ts"

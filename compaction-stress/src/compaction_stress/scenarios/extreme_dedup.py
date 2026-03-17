"""Extreme dedup ratio scenario."""

from compaction_stress.scenarios.base import BaseScenario


class ExtremeDedupScenario(BaseScenario):
    name = "extreme_dedup"

    def _key_prefix(self, topic: str) -> str:
        return "ed"

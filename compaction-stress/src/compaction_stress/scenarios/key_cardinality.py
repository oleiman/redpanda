"""Key cardinality overflow scenario."""

from compaction_stress.scenarios.base import BaseScenario


class KeyCardinalityScenario(BaseScenario):
    name = "key_cardinality"

    def _key_prefix(self, topic: str) -> str:
        return "kc"

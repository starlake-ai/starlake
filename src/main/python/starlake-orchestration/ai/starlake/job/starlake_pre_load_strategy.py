from __future__ import annotations

from enum import Enum

class StarlakePreLoadStrategy(str, Enum):
    """Class with different pre load strategies."""

    IMPORTED = "imported"
    ACK = "ack"
    PENDING = "pending"
    NONE = "none"

    @classmethod
    def is_valid(cls, strategy: str) -> bool:
        """Validate a pre load strategy."""
        return strategy in cls.all_strategies()

    @classmethod
    def all_strategies(cls) -> set[str]:
        """Return all load strategies."""
        return set(cls.__members__.values())

    def __str__(self) -> str:
        return self.value

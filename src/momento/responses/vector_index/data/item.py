from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Item:
    """Represents an entry in the vector index."""

    id: str
    """The id of the item."""

    vector: list[float]
    """The vector of the item."""

    metadata: dict[str, str | int | float | bool | list[str]] = field(default_factory=dict)
    """The metadata of the item."""


@dataclass
class ItemWithoutVector:
    """Represents an entry in the vector index without a vector."""

    id: str
    """The id of the item."""

    metadata: dict[str, str | int | float | bool | list[str]] = field(default_factory=dict)
    """The metadata of the item."""

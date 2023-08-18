from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from momento_wire_types import vectorindex_pb2 as pb


# TODO: support other datatypes for the vector (np.array, pd.Series, etc.)
@dataclass
class Item:
    """Represents an entry in the vector index."""

    id: str
    """The id of the item."""

    vector: list[float]
    """The vector of the item."""

    metadata: Optional[dict[str, str]] = None
    """The metadata of the item."""

    def __post_init__(self) -> None:
        if self.metadata is None:
            self.metadata = {}

    def to_proto(self) -> pb._Item:
        vector = pb._Vector(elements=self.vector)
        metadata = (
            [pb._Metadata(field=k, string_value=v) for k, v in self.metadata.items()]
            if self.metadata is not None
            else []
        )
        return pb._Item(id=self.id, vector=vector, metadata=metadata)

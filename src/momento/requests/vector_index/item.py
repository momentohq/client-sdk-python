from __future__ import annotations

from dataclasses import dataclass, field

from momento_wire_types import vectorindex_pb2 as pb

from momento.errors.exceptions import InvalidArgumentException
from momento.internal.services import Service


# TODO: support other datatypes for the vector (np.array, pd.Series, etc.)
@dataclass
class Item:
    """Represents an entry in the vector index."""

    id: str
    """The id of the item."""

    vector: list[float]
    """The vector of the item."""

    metadata: dict[str, str] = field(default_factory=dict)
    """The metadata of the item."""

    def to_proto(self) -> pb._Item:
        vector = pb._Vector(elements=self.vector)
        metadata = []
        if self.metadata is not None:
            for k, v in self.metadata.items():
                if type(v) is not str:
                    raise InvalidArgumentException(
                        f"Metadata values must be strings. Field {k!r} has a value of type {type(v)!r} with value {v!r}.",  # noqa: E501
                        Service.INDEX,
                    )
                metadata.append(pb._Metadata(field=k, string_value=v))

        return pb._Item(id=self.id, vector=vector, metadata=metadata)

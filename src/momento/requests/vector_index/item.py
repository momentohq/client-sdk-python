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

    metadata: dict[str, str | int | float | bool | list[str]] = field(default_factory=dict)
    """The metadata of the item."""

    def to_proto(self) -> pb._Item:
        vector = pb._Vector(elements=self.vector)
        metadata = []
        if self.metadata is not None:
            for k, v in self.metadata.items():
                if type(v) == str:
                    metadata.append(pb._Metadata(field=k, string_value=v))
                elif type(v) == int:
                    metadata.append(pb._Metadata(field=k, integer_value=v))
                elif type(v) == float:
                    metadata.append(pb._Metadata(field=k, double_value=v))
                elif type(v) == bool:
                    metadata.append(pb._Metadata(field=k, boolean_value=v))
                elif type(v) == list and all(type(x) == str for x in v):
                    list_of_strings = pb._Metadata._ListOfStrings(values=v)
                    metadata.append(pb._Metadata(field=k, list_of_strings_value=list_of_strings))
                else:
                    raise InvalidArgumentException(
                        f"Metadata values must be either str, int, float, bool, or list[str]. Field {k!r} has a value of type {type(v)!r} with value {v!r}.",  # noqa: E501
                        Service.INDEX,
                    )

        return pb._Item(id=self.id, vector=vector, metadata=metadata)

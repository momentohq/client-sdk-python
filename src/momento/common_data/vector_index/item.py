from __future__ import annotations

from typing import Dict, List, Optional, Union

from momento_wire_types import vectorindex_pb2 as pb

from momento.errors.exceptions import InvalidArgumentException
from momento.internal.services import Service

Metadata = Dict[str, Union[str, int, float, bool, List[str]]]
"""The metadata of an item."""


class ItemMetadata:
    """Represents the id-metadata portion of an entry in the vector index.

    This is used in requests to update only the metadata of an item.
    """

    def __init__(self, id: str, metadata: Optional[Metadata] = None) -> None:
        """Represents the id-metadata portion of an entry in the vector index.

        Args:
            id (str): The id of the item.
            metadata (Optional[Metadata], optional): The metadata of the item. Defaults to None, ie empty metadata.
        """
        self.id = id
        self.metadata = metadata or {}

    def __eq__(self, other: object) -> bool:
        if isinstance(other, ItemMetadata):
            return self.id == other.id and self.metadata == other.metadata
        return False

    def __hash__(self) -> int:
        return hash((self.id, self.metadata))

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(id={self.id!r}, metadata={self.metadata!r})"


# TODO: support other datatypes for the vector (np.array, pd.Series, etc.)
class Item(ItemMetadata):
    """Represents a full entry in the vector index.

    This is used for `upsert_item_batch` requests.
    """

    def __init__(self, id: str, vector: list[float], metadata: Optional[Metadata] = None) -> None:
        """Represents an entry in the vector index.

        Args:
            id (str): The id of the item.
            vector (list[float]): The vector of the item.
            metadata (Optional[Metadata], optional): The metadata of the item. Defaults to None, ie empty metadata.
        """
        super().__init__(id, metadata)
        self.vector = vector

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Item):
            return super().__eq__(other) and self.vector == other.vector
        return False

    def __hash__(self) -> int:
        return hash((self.id, self.vector, self.metadata))

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(id={self.id!r}, vector={self.vector!r}, metadata={self.metadata!r})"

    def to_proto(self) -> pb._Item:
        vector = pb._Vector(elements=self.vector)
        metadata = []
        for k, v in self.metadata.items():
            if type(v) is str:
                metadata.append(pb._Metadata(field=k, string_value=v))
            elif type(v) is int:
                metadata.append(pb._Metadata(field=k, integer_value=v))
            elif type(v) is float:
                metadata.append(pb._Metadata(field=k, double_value=v))
            elif type(v) is bool:
                metadata.append(pb._Metadata(field=k, boolean_value=v))
            elif type(v) is list and all(type(x) is str for x in v):
                list_of_strings = pb._Metadata._ListOfStrings(values=v)
                metadata.append(pb._Metadata(field=k, list_of_strings_value=list_of_strings))
            else:
                raise InvalidArgumentException(
                    f"Metadata values must be either str, int, float, bool, or list[str]. Field {k!r} has a value of type {type(v)!r} with value {v!r}.",  # noqa: E501
                    Service.INDEX,
                )
        return pb._Item(id=self.id, vector=vector, metadata=metadata)

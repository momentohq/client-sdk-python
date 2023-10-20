from __future__ import annotations

from abc import ABC
from dataclasses import dataclass, field

from momento_wire_types import vectorindex_pb2 as vectorindex_pb

from momento.errors import UnknownException

from ...mixins import ErrorResponseMixin
from ..response import VectorIndexResponse


class SearchResponse(VectorIndexResponse):
    """Parent response type for a vector index `search` request.

    Its subtypes are:
    - `Search.Success`
    - `Search.Error`

    See `VectorIndexClient` for how to work with responses.
    """


@dataclass
class SearchHit:
    id: str
    distance: float
    metadata: dict[str, str | int | float | bool | list[str]] = field(default_factory=dict)

    @staticmethod
    def from_proto(hit: vectorindex_pb._SearchHit) -> SearchHit:
        metadata = {}
        for item in hit.metadata:
            type = item.WhichOneof("value")
            field = item.field
            if type == "string_value":
                metadata[field] = item.string_value
            elif type == "integer_value":
                metadata[field] = item.integer_value
            elif type == "double_value":
                metadata[field] = item.double_value
            elif type == "boolean_value":
                metadata[field] = item.boolean_value
            elif type == "list_of_strings_value":
                metadata[field] = item.list_of_strings_value.values
            else:
                raise UnknownException(f"Unknown metadata value: {type}")
        return SearchHit(id=hit.id, distance=hit.distance, metadata=metadata)


class Search(ABC):
    """Groups all `SearchResponse` derived types under a common namespace."""

    @dataclass
    class Success(SearchResponse):
        """Indicates the request was successful."""

        hits: list[SearchHit]

    class Error(SearchResponse, ErrorResponseMixin):
        """Contains information about an error returned from a request.

        This includes:
        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

from __future__ import annotations

from abc import ABC
from dataclasses import dataclass, field

from momento_wire_types import vectorindex_pb2 as vectorindex_pb

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
    metadata: dict[str, str] = field(default_factory=dict)

    @staticmethod
    def from_proto(hit: vectorindex_pb._SearchHit) -> SearchHit:
        metadata = {  # type: ignore
            item.field: item.string_value  # type: ignore
            for item in hit.metadata  # type: ignore
        }
        return SearchHit(id=hit.id, distance=hit.distance, metadata=metadata)  # type: ignore


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

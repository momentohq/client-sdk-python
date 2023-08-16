from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from typing import Optional

from momento_wire_types import vectorindex_pb2 as vectorindex_pb

from ..mixins import ErrorResponseMixin
from ..response import VectorIndexResponse


class VectorIndexSearchResponse(VectorIndexResponse):
    """Parent response type for a vector index `search` request.

    Its subtypes are:
    - `VectorIndexSearch.Success`
    - `VectorIndexSearch.Error`

    See `VectorIndexClient` for how to work with responses.
    """


@dataclass
class SearchHit:
    id: str
    distance: float
    metadata: Optional[dict[str, str]] = None

    def __post_init__(self) -> None:
        self.metadata = self.metadata or {}

    @staticmethod
    def from_proto(hit: vectorindex_pb.SearchHit) -> SearchHit:
        metadata = {  # type: ignore
            item.field: item.string_value  # type: ignore
            for item in hit.metadata  # type: ignore
        }
        return SearchHit(id=hit.id, distance=hit.distance, metadata=metadata)  # type: ignore


class VectorIndexSearch(ABC):
    """Groups all `VectorIndexSearchResponse` derived types under a common namespace."""

    @dataclass
    class Success(VectorIndexSearchResponse):
        """Indicates the request was successful."""

        hits: list[SearchHit]

    class Error(VectorIndexSearchResponse, ErrorResponseMixin):
        """Contains information about an error returned from a request.

        This includes:
        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

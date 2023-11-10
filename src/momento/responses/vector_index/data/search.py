from __future__ import annotations

from abc import ABC
from dataclasses import dataclass, field

from momento_wire_types import vectorindex_pb2 as vectorindex_pb

from ...mixins import ErrorResponseMixin
from ..response import VectorIndexResponse
from .utils import pb_metadata_to_dict


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
    score: float
    metadata: dict[str, str | int | float | bool | list[str]] = field(default_factory=dict)

    @property
    def distance(self) -> float:
        """Alias for `score`."""
        return self.score

    @staticmethod
    def from_proto(hit: vectorindex_pb._SearchHit) -> SearchHit:
        metadata = pb_metadata_to_dict(hit.metadata)
        return SearchHit(id=hit.id, score=hit.score, metadata=metadata)


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

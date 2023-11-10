from __future__ import annotations

from abc import ABC
from dataclasses import dataclass, field

from momento_wire_types import vectorindex_pb2 as vectorindex_pb

from ...mixins import ErrorResponseMixin
from ..response import VectorIndexResponse
from .search import SearchHit
from .utils import pb_metadata_to_dict


class SearchAndFetchVectorsResponse(VectorIndexResponse):
    """Parent response type for a vector index `search_and_fetch_vectors` request.

    Its subtypes are:
    - `SearchAndFetchVectors.Success`
    - `SearchAndFetchVectors.Error`

    See `VectorIndexClient` for how to work with responses.
    """


@dataclass
class SearchAndFetchVectorsHit(SearchHit):
    vector: list[float] = field(default_factory=list)

    @staticmethod
    def from_search_hit(hit: SearchHit, vector: list[float]) -> SearchAndFetchVectorsHit:
        return SearchAndFetchVectorsHit(id=hit.id, score=hit.score, metadata=hit.metadata, vector=vector)

    @staticmethod
    def from_proto(hit: vectorindex_pb._SearchAndFetchVectorsHit) -> SearchAndFetchVectorsHit:
        metadata = pb_metadata_to_dict(hit.metadata)
        return SearchAndFetchVectorsHit(id=hit.id, score=hit.score, metadata=metadata, vector=list(hit.vector.elements))


class SearchAndFetchVectors(ABC):
    """Groups all `SearchAndFetchVectorsResponse` derived types under a common namespace."""

    @dataclass
    class Success(SearchAndFetchVectorsResponse):
        """Indicates the request was successful."""

        hits: list[SearchAndFetchVectorsHit]

    class Error(SearchAndFetchVectorsResponse, ErrorResponseMixin):
        """Contains information about an error returned from a request.

        This includes:
        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

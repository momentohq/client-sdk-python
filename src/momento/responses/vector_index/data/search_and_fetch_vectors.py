from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from typing import Optional

from momento_wire_types import vectorindex_pb2 as vectorindex_pb

from momento.common_data.vector_index.item import Metadata

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


class SearchAndFetchVectorsHit(SearchHit):
    def __init__(self, score: float, id: str, vector: list[float], metadata: Optional[Metadata] = None):
        """Initializes a new instance of `SearchAndFetchVectorsHit`.

        Args:
            score (float): The similarity of the hit to the query.
            id (str): The id of the hit.
            vector (list[float]): The vector of the hit.
            metadata (Optional[Metadata], optional): The metadata of the hit. Defaults to None, ie empty metadata.
        """
        super().__init__(score, id, metadata)
        self.vector = vector

    def __eq__(self, other: object) -> bool:
        if isinstance(other, SearchAndFetchVectorsHit):
            return super().__eq__(other) and self.vector == other.vector
        return False

    def __hash__(self) -> int:
        return hash((self.score, self.id, self.vector, self.metadata))

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(score={self.score!r}, id={self.id!r}, vector={self.vector!r}, metadata={self.metadata!r})"

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

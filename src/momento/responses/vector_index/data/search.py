from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from typing import Optional

from momento_wire_types import vectorindex_pb2 as vectorindex_pb

from momento.common_data.vector_index.item import Metadata

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


class SearchHit:
    """A single search hit."""

    def __init__(self, score: float, id: str, metadata: Optional[Metadata] = None):
        """Initializes a new instance of `SearchHit`.

        Args:
            score (float): The similarity of the hit to the query.
            id (str): The id of the hit.
            metadata (Optional[Metadata], optional): The metadata of the hit. Defaults to None, ie empty metadata.
        """
        self.score = score
        self.id = id
        self.metadata = metadata or {}

    def __eq__(self, other: object) -> bool:
        if isinstance(other, SearchHit):
            return self.score == other.score and self.id == other.id and self.metadata == other.metadata
        return False

    def __hash__(self) -> int:
        return hash((self.score, self.id, self.metadata))

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(score={self.score!r}, id={self.id!r}, metadata={self.metadata!r})"

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

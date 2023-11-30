from __future__ import annotations

from abc import ABC
from dataclasses import dataclass

from momento_wire_types import vectorindex_pb2 as pb

from momento.common_data.vector_index.item import Item
from momento.errors.exceptions import UnknownException

from ...mixins import ErrorResponseMixin
from ..response import VectorIndexResponse
from .utils import pb_metadata_to_dict


class GetItemAndFetchVectorsBatchResponse(VectorIndexResponse):
    """Parent response type for a `get_item_and_fetch_vectors_batch` request.

    Its subtypes are:
    - `GetItemAndFetchVectorsBatch.Success`
    - `GetItemAndFetchVectorsBatch.Error`

    See `PreviewVectorIndexClient` for how to work with responses.
    """


class GetItemAndFetchVectorsBatch(ABC):
    """Groups all `GetItemAndFetchVectorsBatchResponse` derived types under a common namespace."""

    @dataclass
    class Success(GetItemAndFetchVectorsBatchResponse):
        """Contains the result of a `get_item_and_fetch_vectors_batch` request."""

        hits: dict[str, Item]
        """The items that were found."""

        @staticmethod
        def from_proto(response: pb._GetItemAndFetchVectorsBatchResponse) -> "GetItemAndFetchVectorsBatch.Success":
            """Converts a proto hit to a `GetItemAndFetchVectorsBatch.Success`."""
            hits = {}
            for item in response.item_with_vector_response:
                type = item.WhichOneof("response")
                if type == "hit":
                    id_, metadata = item.hit.id, pb_metadata_to_dict(item.hit.metadata)
                    hits[id_] = Item(id=id_, vector=list(item.hit.vector.elements), metadata=metadata)
                elif type == "miss":
                    pass
                else:
                    raise UnknownException(f"Unknown response type {type!r}.")

            return GetItemAndFetchVectorsBatch.Success(hits=hits)

    class Error(GetItemAndFetchVectorsBatchResponse, ErrorResponseMixin):
        """Contains information about an error returned from a request.

        This includes:
        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

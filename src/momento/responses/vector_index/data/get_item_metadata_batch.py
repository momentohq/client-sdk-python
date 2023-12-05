from __future__ import annotations

from abc import ABC
from dataclasses import dataclass

from momento_wire_types import vectorindex_pb2 as pb

from momento.common_data.vector_index.item import Metadata
from momento.errors.exceptions import UnknownException

from ...mixins import ErrorResponseMixin
from ..response import VectorIndexResponse
from .utils import pb_metadata_to_dict


class GetItemMetadataBatchResponse(VectorIndexResponse):
    """Parent response type for a `get_item_metadata_batch` request.

    Its subtypes are:
    - `GetItemMetadataBatch.Success`
    - `GetItemMetadataBatch.Error`

    See `PreviewVectorIndexClient` for how to work with responses.
    """


class GetItemMetadataBatch(ABC):
    """Groups all `GetItemMetadataBatchResponse` derived types under a common namespace."""

    @dataclass
    class Success(GetItemMetadataBatchResponse):
        """Contains the result of a `get_item_metadata_batch` request."""

        hits: dict[str, Metadata]
        """The metadata of the items that were found."""

        @staticmethod
        def from_proto(response: pb._GetItemMetadataBatchResponse) -> "GetItemMetadataBatch.Success":
            """Converts a proto hit to a `GetItemMetadataBatch.Success`."""
            hits = {}
            for item in response.item_metadata_response:
                type = item.WhichOneof("response")
                if type == "hit":
                    hits[item.hit.id] = pb_metadata_to_dict(item.hit.metadata)
                elif type == "miss":
                    pass
                else:
                    raise UnknownException(f"Unknown response type {type!r}.")

            return GetItemMetadataBatch.Success(hits=hits)

    class Error(GetItemMetadataBatchResponse, ErrorResponseMixin):
        """Contains information about an error returned from a request.

        This includes:
        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

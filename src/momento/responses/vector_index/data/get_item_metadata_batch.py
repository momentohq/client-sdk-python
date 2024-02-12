from __future__ import annotations

from abc import ABC
from dataclasses import dataclass

from momento_wire_types import vectorindex_pb2 as pb

from momento.common_data.vector_index.item import Metadata

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

        values: dict[str, Metadata]
        """The metadata of the items that were found."""

        @staticmethod
        def from_proto(response: pb._GetItemMetadataBatchResponse) -> "GetItemMetadataBatch.Success":
            """Converts a proto _GetItemMetadataBatchResponse to a `GetItemMetadataBatch.Success`."""
            values = {item.id: pb_metadata_to_dict(item.metadata) for item in response.item_metadata_response}
            return GetItemMetadataBatch.Success(values=values)

    class Error(GetItemMetadataBatchResponse, ErrorResponseMixin):
        """Contains information about an error returned from a request.

        This includes:
        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

from __future__ import annotations

from abc import ABC
from dataclasses import dataclass

from momento_wire_types import vectorindex_pb2 as pb

from momento.common_data.vector_index.item import Item
from momento.errors.exceptions import UnknownException

from ...mixins import ErrorResponseMixin
from ..response import VectorIndexResponse
from .utils import pb_metadata_to_dict


class GetItemBatchResponse(VectorIndexResponse):
    """Parent response type for a `get_item_batch` request.

    Its subtypes are:
    - `GetItemBatch.Success`
    - `GetItemBatch.Error`

    See `PreviewVectorIndexClient` for how to work with responses.
    """


class GetItemBatch(ABC):
    """Groups all `GetItemBatchResponse` derived types under a common namespace."""

    @dataclass
    class Success(GetItemBatchResponse):
        """Contains the result of a `get_item_batch` request."""

        values: dict[str, Item]
        """The items that were found."""

        @staticmethod
        def from_proto(response: pb._GetItemBatchResponse) -> "GetItemBatch.Success":
            """Converts a proto hit to a `GetItemBatch.Success`."""
            values = {}
            for item in response.item_response:
                type = item.WhichOneof("response")
                if type == "hit":
                    id_, metadata = item.hit.id, pb_metadata_to_dict(item.hit.metadata)
                    values[id_] = Item(id=id_, vector=list(item.hit.vector.elements), metadata=metadata)
                elif type == "miss":
                    pass
                else:
                    raise UnknownException(f"Unknown response type {type!r}.")

            return GetItemBatch.Success(values=values)

    class Error(GetItemBatchResponse, ErrorResponseMixin):
        """Contains information about an error returned from a request.

        This includes:
        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

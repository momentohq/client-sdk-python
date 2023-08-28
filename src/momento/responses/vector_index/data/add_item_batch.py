from abc import ABC

from ...mixins import ErrorResponseMixin
from ..response import VectorIndexResponse


class AddItemBatchResponse(VectorIndexResponse):
    """Parent response type for a vector index `add_item_batch` request.

    Its subtypes are:
    - `AddItemBatch.Success`
    - `AddItemBatch.Error`

    See `VectorIndexClient` for how to work with responses.
    """


class AddItemBatch(ABC):
    """Groups all `AddItemBatchResponse` derived types under a common namespace."""

    class Success(AddItemBatchResponse):
        """Indicates the request was successful."""

    class Error(AddItemBatchResponse, ErrorResponseMixin):
        """Contains information about an error returned from a request.

        This includes:
        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

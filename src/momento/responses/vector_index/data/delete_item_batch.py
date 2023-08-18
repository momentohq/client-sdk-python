from abc import ABC

from ...mixins import ErrorResponseMixin
from ..response import VectorIndexResponse


class DeleteItemBatchResponse(VectorIndexResponse):
    """Parent response type for a vector index `delete_item_batch` request.

    Its subtypes are:
    - `DeleteItemBatch.Success`
    - `DeleteItemBatch.Error`

    See `VectorIndexClient` for how to work with responses.
    """


class DeleteItemBatch(ABC):
    """Groups all `DeleteItemBatchResponse` derived types under a common namespace."""

    class Success(DeleteItemBatchResponse):
        """Indicates the request was successful."""

    class Error(DeleteItemBatchResponse, ErrorResponseMixin):
        """Contains information about an error returned from a request.

        This includes:
        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

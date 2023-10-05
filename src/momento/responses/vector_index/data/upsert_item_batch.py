from abc import ABC

from ...mixins import ErrorResponseMixin
from ..response import VectorIndexResponse


class UpsertItemBatchResponse(VectorIndexResponse):
    """Parent response type for a vector index `add_item_batch` request.

    Its subtypes are:
    - `UpsertItemBatch.Success`
    - `UpsertItemBatch.Error`

    See `VectorIndexClient` for how to work with responses.
    """


class UpsertItemBatch(ABC):
    """Groups all `UpsertItemBatchResponse` derived types under a common namespace."""

    class Success(UpsertItemBatchResponse):
        """Indicates the request was successful."""

    class Error(UpsertItemBatchResponse, ErrorResponseMixin):
        """Contains information about an error returned from a request.

        This includes:
        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

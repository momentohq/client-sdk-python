from abc import ABC

from ..mixins import ErrorResponseMixin
from ..response import VectorIndexResponse


class VectorIndexUpsertItemBatchResponse(VectorIndexResponse):
    """Parent response type for a vector index `upsert_item_batch` request.

    Its subtypes are:
    - `VectorIndexUpsertItemBatch.Success`
    - `VectorIndexUpsertItemBatch.Error`

    See `VectorIndexClient` for how to work with responses.
    """


class VectorIndexUpsertItemBatch(ABC):
    """Groups all `VectorIndexUpsertItemBatchResponse` derived types under a common namespace."""

    class Success(VectorIndexUpsertItemBatchResponse):
        """Indicates the request was successful."""

    class Error(VectorIndexUpsertItemBatchResponse, ErrorResponseMixin):
        """Contains information about an error returned from a request.

        This includes:
        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

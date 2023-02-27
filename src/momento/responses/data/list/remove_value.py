from abc import ABC

from ...mixins import ErrorResponseMixin
from ...response import CacheResponse


class CacheListRemoveValueResponse(CacheResponse):
    """Response type for a `list_remove_value` request.

    Its subtypes are:
    - `CacheListRemoveValue.Success`
    - `CacheListRemoveValue.Error`

    See `CacheClient` for how to work with responses.
    """


class CacheListRemoveValue(ABC):
    """Groups all `CacheListRemoveValueResponse` derived types under a common namespace."""

    class Success(CacheListRemoveValueResponse):
        """Indicates removing the values was successful."""

    class Error(CacheListRemoveValueResponse, ErrorResponseMixin):
        """Indicates an error occured in the request.

        This includes:
        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

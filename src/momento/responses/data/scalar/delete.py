from abc import ABC

from ...mixins import ErrorResponseMixin
from ...response import CacheResponse


class CacheDeleteResponse(CacheResponse):
    """Parent response type for a cache `delete` request.

    Its subtypes are:
    - `CacheDelete.Success`
    - `CacheDelete.Error`

    See `CacheClient` for how to work with responses.
    """


class CacheDelete(ABC):
    """Groups all `CacheDeleteResponse` derived types under a common namespace."""

    class Success(CacheDeleteResponse):
        """Indicates the request was successful."""

    class Error(CacheDeleteResponse, ErrorResponseMixin):
        """Contains information about an error returned from a request.

        This includes:
        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

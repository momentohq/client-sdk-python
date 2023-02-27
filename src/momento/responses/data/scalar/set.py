from abc import ABC

from ...mixins import ErrorResponseMixin
from ...response import CacheResponse


class CacheSetResponse(CacheResponse):
    """Parent response type for a cache `set` request.

    Its subtypes are:
    - `CacheSet.Success`
    - `CacheSet.Error`

    See `CacheClient` for how to work with responses.
    """


class CacheSet(ABC):
    """Groups all `CacheSetResponse` derived types under a common namespace."""

    class Success(CacheSetResponse):
        """Indicates the request was successful."""

    class Error(CacheSetResponse, ErrorResponseMixin):
        """Contains information about an error returned from a request.

        This includes:
        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

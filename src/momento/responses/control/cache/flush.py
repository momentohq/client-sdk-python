from abc import ABC

from momento.responses.mixins import ErrorResponseMixin
from momento.responses.response import ControlResponse


class CacheFlushResponse(ControlResponse):
    """Parent response type for a cache `flush` request.

    Its subtypes are:
    - `CacheFlush.Success`
    - `CacheFlush.Error`

    See `CacheClient` for how to work with responses.
    """


class CacheFlush(ABC):
    """Groups all `CacheFlushResponse` derived types under a common namespace."""

    class Success(CacheFlushResponse):
        """Indicates the request was successful."""

    class Error(CacheFlushResponse, ErrorResponseMixin):
        """Contains information about an error returned from a request.

        This includes:
        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

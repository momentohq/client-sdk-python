from abc import ABC
from dataclasses import dataclass

from momento.errors import SdkException

from ..mixins import ErrorResponseMixin
from ..response import CacheResponse


class CacheDeleteResponse(CacheResponse):
    """Parent response type for a cache `delete` request. Its subtypes are:

    - `CacheDelete.Success`
    - `CacheDelete.Error`

    See `SimpleCacheClient` for how to work with responses.
    """


class CacheDelete(ABC):
    """Groups all `CacheDeleteResponse` derived types under a common namespace."""

    @dataclass
    class Success(CacheDeleteResponse):
        """Indicates the request was successful."""

    @dataclass
    class Error(CacheDeleteResponse, ErrorResponseMixin):
        """Contains information about an error returned from a request:

        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

        _error: SdkException

        def __init__(self, _error: SdkException):
            self._error = _error

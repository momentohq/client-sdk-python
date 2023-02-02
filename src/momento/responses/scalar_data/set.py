from abc import ABC
from dataclasses import dataclass

from momento.errors import SdkException

from ..mixins import ErrorResponseMixin
from ..response import CacheResponse


class CacheSetResponse(CacheResponse):
    """Parent response type for a cache `set` request. Its subtypes are:

    - `CacheSet.Success`
    - `CacheSet.Error`

    See `SimpleCacheClient` for how to work with responses.
    """


class CacheSet(ABC):
    """Groups all `CacheSetResponse` derived types under a common namespace."""

    @dataclass
    class Success(CacheSetResponse):
        """Indicates the request was successful."""

    @dataclass
    class Error(CacheSetResponse, ErrorResponseMixin):
        """Contains information about an error returned from a request:

        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

        _error: SdkException

        def __init__(self, _error: SdkException):
            self._error = _error

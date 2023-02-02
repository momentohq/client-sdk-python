from abc import ABC
from dataclasses import dataclass

from momento.errors import SdkException

from ..mixins import ErrorResponseMixin, ValueStringMixin
from ..response import CacheResponse


class CacheGetResponse(CacheResponse):
    """Parent response type for a cache `get` request. Its subtypes are:

    - `CacheGet.Hit`
    - `CacheGet.Miss`
    - `CacheGet.Error`

    See `SimpleCacheClient` for how to work with responses.
    """


class CacheGet(ABC):
    """Groups all `CacheGetResponse` derived types under a common namespace."""

    @dataclass
    class Hit(CacheGetResponse, ValueStringMixin):
        """Contains the result of a cache hit."""

        value_bytes: bytes
        """The value returned from the cache for the specified key. Use the
        `value_string` property to access the value as a string."""

    @dataclass
    class Miss(CacheGetResponse):
        """Contains the results of a cache miss"""

    @dataclass
    class Error(CacheGetResponse, ErrorResponseMixin):
        """Contains information about an error returned from a request:

        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

        _error: SdkException

        def __init__(self, _error: SdkException):
            self._error = _error

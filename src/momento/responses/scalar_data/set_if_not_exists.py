from abc import ABC
from dataclasses import dataclass

from momento.errors import SdkException

from ..mixins import ErrorResponseMixin
from ..response import CacheResponse


class CacheSetIfNotExistsResponse(CacheResponse):
    """Parent response type for a cache `set_if_not_exists` request. Its subtypes are:

    - `CacheSetIfNotExists.Stored`
    - `CacheSetIfNotExists.NotStored`
    - `CacheSetIfNotExists.Error`

    See `SimpleCacheClient` for how to work with responses.
    """


class CacheSetIfNotExists(ABC):
    """Groups all `CacheSetIfNotExistsResponse` derived types under a common namespace."""

    @dataclass
    class Stored(CacheSetIfNotExistsResponse):
        """Indicates the key did not exist and the value was set."""

    @dataclass
    class NotStored(CacheSetIfNotExistsResponse):
        """Indicates the key existed and no value was set."""

    @dataclass
    class Error(CacheSetIfNotExistsResponse, ErrorResponseMixin):
        """Contains information about an error returned from a request:

        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

        _error: SdkException

        def __init__(self, _error: SdkException):
            self._error = _error

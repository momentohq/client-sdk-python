from abc import ABC
from dataclasses import dataclass

from momento.errors import SdkException

from ..mixins import ErrorResponseMixin
from ..response import CacheResponse


class CacheListLengthResponse(CacheResponse):
    """Response type for a `list_length` request. Its subtypes are:

    - `CacheListLength.Hit`
    - `CacheListLength.Miss`
    - `CacheListLength.Error`

    See `SimpleCacheClient` for how to work with responses.
    """


class CacheListLength(ABC):
    """Groups all `CacheListLengthResponse` derived types under a common namespace."""

    @dataclass
    class Hit(CacheListLengthResponse):
        """Indicates the list exists and its length was fetched."""

        length: int
        """The number of values in the list."""

    @dataclass
    class Miss(CacheListLengthResponse):
        """Indicates the list does not exist."""

    @dataclass
    class Error(CacheListLengthResponse, ErrorResponseMixin):
        """Indicates an error occured in the request:

        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

        _error: SdkException

        def __init__(self, _error: SdkException):
            self._error = _error

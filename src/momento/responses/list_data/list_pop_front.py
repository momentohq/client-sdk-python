from abc import ABC
from dataclasses import dataclass

from momento.errors import SdkException

from ..mixins import ErrorResponseMixin, ValueStringMixin
from ..response import CacheResponse


class CacheListPopFrontResponse(CacheResponse):
    """Parent response type for a `list_pop_front` request. Its subtypes are:

    - `CacheListPopFront.Hit`
    - `CacheListPopFront.Miss`
    - `CacheListPopFront.Error`

    See `SimpleCacheClient` for how to work with responses.
    """


class CacheListPopFront(ABC):
    """Groups all `CacheListPopFront` derived types under a common namespace."""

    @dataclass
    class Hit(CacheListPopFrontResponse, ValueStringMixin):
        """Indicates the request was successful."""

        value_bytes: bytes
        """The value popped from the list. Use the `value_string` property to access the value as a string."""

    @dataclass
    class Miss(CacheListPopFrontResponse):
        """Indicates the list does not exist."""

    @dataclass
    class Error(CacheListPopFrontResponse, ErrorResponseMixin):
        """Contains information about an error returned from a request:

        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

        _error: SdkException

        def __init__(self, _error: SdkException):
            self._error = _error

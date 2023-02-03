from abc import ABC
from dataclasses import dataclass

from momento.errors import SdkException

from ..mixins import ErrorResponseMixin, ValueStringMixin
from ..response import CacheResponse


class CacheListPopBackResponse(CacheResponse):
    """Parent response type for a `list_pop_back` request. Its subtypes are:

    - `CacheListPopBack.Hit`
    - `CacheListPopBack.Miss`
    - `CacheListPopBack.Error`

    See `SimpleCacheClient` for how to work with responses.
    """


class CacheListPopBack(ABC):
    """Groups all `CacheListPopBack` derived types under a common namespace."""

    @dataclass
    class Hit(CacheListPopBackResponse, ValueStringMixin):
        """Indicates the request was successful."""

        value_bytes: bytes
        """The value popped from the list. Use the `value_string` property to access the value as a string."""

    @dataclass
    class Miss(CacheListPopBackResponse):
        """Indicates the list does not exist."""

    @dataclass
    class Error(CacheListPopBackResponse, ErrorResponseMixin):
        """Contains information about an error returned from a request:

        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

        _error: SdkException

        def __init__(self, _error: SdkException):
            self._error = _error

from abc import ABC
from dataclasses import dataclass

from momento.errors import SdkException

from ..mixins import ErrorResponseMixin
from ..response import CacheResponse


class CacheListPushFrontResponse(CacheResponse):
    """Response type for a `list_push_front` request. Its subtypes are:

    - `CacheListPushFront.Success`
    - `CacheListPushFront.Error`

    See `SimpleCacheClient` for how to work with responses.
    """


class CacheListPushFront(ABC):
    """Groups all `CacheListPushFrontResponse` derived types under a common namespace."""

    @dataclass
    class Success(CacheListPushFrontResponse):
        """Indicates the push was successful."""

        list_length: int
        """The number of values in the list after this push"""

    @dataclass
    class Error(CacheListPushFrontResponse, ErrorResponseMixin):
        """Indicates an error occured in the request:

        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

        _error: SdkException

        def __init__(self, _error: SdkException):
            self._error = _error

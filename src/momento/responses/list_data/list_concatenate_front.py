from abc import ABC
from dataclasses import dataclass

from momento.errors import SdkException

from ..mixins import ErrorResponseMixin
from ..response import CacheResponse


class CacheListConcatenateFrontResponse(CacheResponse):
    """Response type for a `list_concatenate_front` request. Its subtypes are:

    - `CacheListConcatenateFront.Success`
    - `CacheListConcatenateFront.Error`

    See `SimpleCacheClient` for how to work with responses.
    """


class CacheListConcatenateFront(ABC):
    """Groups all `CacheListConcatenateFrontResponse` derived types under a common namespace."""

    @dataclass
    class Success(CacheListConcatenateFrontResponse):
        """Indicates the concatenation was successful."""

        list_length: int
        """The number of values in the list after this concatenation"""

    @dataclass
    class Error(CacheListConcatenateFrontResponse, ErrorResponseMixin):
        """Indicates an error occured in the request:

        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

        _error: SdkException

        def __init__(self, _error: SdkException):
            self._error = _error

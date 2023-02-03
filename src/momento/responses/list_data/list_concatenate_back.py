from abc import ABC
from dataclasses import dataclass

from momento.errors import SdkException

from ..mixins import ErrorResponseMixin
from ..response import CacheResponse


class CacheListConcatenateBackResponse(CacheResponse):
    """Response type for a `list_concatenate_back` request. Its subtypes are:

    - `CacheListConcatenateBack.Success`
    - `CacheListConcatenateBack.Error`

    See `SimpleCacheClient` for how to work with responses.
    """


class CacheListConcatenateBack(ABC):
    """Groups all `CacheListConcatenateBackResponse` derived types under a common namespace."""

    @dataclass
    class Success(CacheListConcatenateBackResponse):
        """Indicates the concatenation was successful."""

        list_length: int
        """The number of values in the list after this concatenation"""

    @dataclass
    class Error(CacheListConcatenateBackResponse, ErrorResponseMixin):
        """Indicates an error occured in the request:

        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

        _error: SdkException

        def __init__(self, _error: SdkException):
            self._error = _error

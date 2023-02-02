from abc import ABC
from dataclasses import dataclass

from momento.errors import SdkException

from ..mixins import ErrorResponseMixin
from ..response import CacheResponse


class CacheListRemoveValueResponse(CacheResponse):
    """Response type for a `list_remove_value` request. Its subtypes are:

    - `CacheListRemoveValue.Success`
    - `CacheListRemoveValue.Error`

    See `SimpleCacheClient` for how to work with responses.
    """


class CacheListRemoveValue(ABC):
    """Groups all `CacheListRemoveValueResponse` derived types under a common namespace."""

    @dataclass
    class Success(CacheListRemoveValueResponse):
        """Indicates removing the values was successful."""

    @dataclass
    class Error(CacheListRemoveValueResponse, ErrorResponseMixin):
        """Indicates an error occured in the request:

        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

        _error: SdkException

        def __init__(self, _error: SdkException):
            self._error = _error

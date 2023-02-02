from abc import ABC
from dataclasses import dataclass

from momento.errors import SdkException

from ..mixins import ErrorResponseMixin
from ..response import CacheResponse


class CacheSetAddElementsResponse(CacheResponse):
    """Parent response type for a `set_add_elements` request. Its subtypes are:

    - `CacheSetAddElements.Success`
    - `CacheSetAddElements.Error`

    See `SimpleCacheClient` for how to work with responses.
    """


class CacheSetAddElements(ABC):
    """Groups all `CacheSetAddElementsResponse` derived types under a common namespace."""

    @dataclass
    class Success(CacheSetAddElementsResponse):
        """Indicates the elements were added."""

    @dataclass
    class Error(CacheSetAddElementsResponse, ErrorResponseMixin):
        """Contains information about an error returned from a request:

        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

        _error: SdkException

        def __init__(self, _error: SdkException):
            self._error = _error

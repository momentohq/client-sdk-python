from abc import ABC
from dataclasses import dataclass

from momento.errors import SdkException

from ..mixins import ErrorResponseMixin
from ..response import CacheResponse


class CacheSetAddElementResponse(CacheResponse):
    """Parent response type for a `set_add_element` request. Its subtypes are:

    - `CacheSetAddElement.Success`
    - `CacheSetAddElement.Error`

    See `SimpleCacheClient` for how to work with responses.
    """


class CacheSetAddElement(ABC):
    """Groups all `CacheSetAddElementResponse` derived types under a common namespace."""

    @dataclass
    class Success(CacheSetAddElementResponse):
        """Indicates the element was added."""

    @dataclass
    class Error(CacheSetAddElementResponse, ErrorResponseMixin):
        """Contains information about an error returned from a request:

        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

        _error: SdkException

        def __init__(self, _error: SdkException):
            self._error = _error

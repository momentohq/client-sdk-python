from abc import ABC
from dataclasses import dataclass

from momento.errors import SdkException

from ..mixins import ErrorResponseMixin
from ..response import CacheResponse


class CacheSetRemoveElementResponse(CacheResponse):
    """Parent response type for a `set_remove_element` request. Its subtypes are:

    - `CacheSetRemoveElement.Success`
    - `CacheSetRemoveElement.Error`

    See `SimpleCacheClient` for how to work with responses.
    """


class CacheSetRemoveElement(ABC):
    """Groups all `CacheSetRemoveElementResponse` derived types under a common namespace."""

    @dataclass
    class Success(CacheSetRemoveElementResponse):
        """Indicates the element was removed."""

    @dataclass
    class Error(CacheSetRemoveElementResponse, ErrorResponseMixin):
        """Contains information about an error returned from a request:

        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

        _error: SdkException

        def __init__(self, _error: SdkException):
            self._error = _error

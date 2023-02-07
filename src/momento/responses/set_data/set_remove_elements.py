from abc import ABC
from dataclasses import dataclass

from momento.errors import SdkException

from ..mixins import ErrorResponseMixin
from ..response import CacheResponse


class CacheSetRemoveElementsResponse(CacheResponse):
    """Parent response type for a `set_remove_elements` request. Its subtypes are:

    - `CacheSetRemoveElements.Success`
    - `CacheSetRemoveElements.Error`

    See `SimpleCacheClient` for how to work with responses.
    """


class CacheSetRemoveElements(ABC):
    """Groups all `CacheSetRemoveElementsResponse` derived types under a common namespace."""

    @dataclass
    class Success(CacheSetRemoveElementsResponse):
        """Indicates the elements were removed."""

    @dataclass
    class Error(CacheSetRemoveElementsResponse, ErrorResponseMixin):
        """Contains information about an error returned from a request:

        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

        _error: SdkException

        def __init__(self, _error: SdkException):
            self._error = _error

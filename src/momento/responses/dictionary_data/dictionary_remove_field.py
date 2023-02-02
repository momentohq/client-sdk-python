from abc import ABC
from dataclasses import dataclass

from momento.errors import SdkException

from ..mixins import ErrorResponseMixin
from ..response import CacheResponse


class CacheDictionaryRemoveFieldResponse(CacheResponse):
    """Parent response type for a cache `dictionary_remove_field` request. Its subtypes are:

    - `CacheDictionaryRemoveField.Success`
    - `CacheDictionaryRemoveField.Error`

    See `SimpleCacheClient` for how to work with responses.
    """


class CacheDictionaryRemoveField(ABC):
    """Groups all `CacheDictionaryRemoveFieldResponse` derived types under a common namespace."""

    @dataclass
    class Success(CacheDictionaryRemoveFieldResponse):
        """Indicates the request was successful."""

    @dataclass
    class Error(CacheDictionaryRemoveFieldResponse, ErrorResponseMixin):
        """Contains information about an error returned from a request:

        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

        _error: SdkException

        def __init__(self, _error: SdkException):
            self._error = _error

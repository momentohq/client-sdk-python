from abc import ABC
from dataclasses import dataclass

from momento.errors import SdkException

from ..mixins import ErrorResponseMixin
from ..response import CacheResponse


class CacheDictionarySetFieldsResponse(CacheResponse):
    """Parent response type for a cache `dictionary_set_fields` request. Its subtypes are:

    - `CacheDictionarySetFields.Success`
    - `CacheDictionarySetFields.Error`

    See `SimpleCacheClient` for how to work with responses.
    """


class CacheDictionarySetFields(ABC):
    """Groups all `CacheDictionarySetFieldsResponse` derived types under a common namespace."""

    @dataclass
    class Success(CacheDictionarySetFieldsResponse):
        """Indicates the request was successful."""

    @dataclass
    class Error(CacheDictionarySetFieldsResponse, ErrorResponseMixin):
        """Contains information about an error returned from a request:

        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

        _error: SdkException

        def __init__(self, _error: SdkException):
            self._error = _error

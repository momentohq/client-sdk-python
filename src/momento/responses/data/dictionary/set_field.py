from abc import ABC

from ...mixins import ErrorResponseMixin
from ...response import CacheResponse


class CacheDictionarySetFieldResponse(CacheResponse):
    """Parent response type for a cache `dictionary_set_field` request.

    Its subtypes are:
    - `CacheDictionarySetField.Success`
    - `CacheDictionarySetField.Error`

    See `CacheClient` for how to work with responses.
    """


class CacheDictionarySetField(ABC):
    """Groups all `CacheDictionarySetFieldResponse` derived types under a common namespace."""

    class Success(CacheDictionarySetFieldResponse):
        """Indicates the request was successful."""

    class Error(CacheDictionarySetFieldResponse, ErrorResponseMixin):
        """Contains information about an error returned from a request.

        This includes:
        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

from abc import ABC

from ...mixins import ErrorResponseMixin
from ...response import CacheResponse


class CacheDictionaryRemoveFieldResponse(CacheResponse):
    """Parent response type for a cache `dictionary_remove_field` request.

    Its subtypes are:
    - `CacheDictionaryRemoveField.Success`
    - `CacheDictionaryRemoveField.Error`

    See `CacheClient` for how to work with responses.
    """


class CacheDictionaryRemoveField(ABC):
    """Groups all `CacheDictionaryRemoveFieldResponse` derived types under a common namespace."""

    class Success(CacheDictionaryRemoveFieldResponse):
        """Indicates the request was successful."""

    class Error(CacheDictionaryRemoveFieldResponse, ErrorResponseMixin):
        """Contains information about an error returned from a request.

        This includes:
        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

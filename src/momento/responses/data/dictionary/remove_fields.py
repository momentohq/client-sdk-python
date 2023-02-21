from abc import ABC

from ...mixins import ErrorResponseMixin
from ...response import CacheResponse


class CacheDictionaryRemoveFieldsResponse(CacheResponse):
    """Parent response type for a cache `dictionary_remove_fields` request. Its subtypes are:

    - `CacheDictionaryRemoveFields.Success`
    - `CacheDictionaryRemoveFields.Error`

    See `SimpleCacheClient` for how to work with responses.
    """


class CacheDictionaryRemoveFields(ABC):
    """Groups all `CacheDictionaryRemoveFieldsResponse` derived types under a common namespace."""

    class Success(CacheDictionaryRemoveFieldsResponse):
        """Indicates the request was successful."""

    class Error(CacheDictionaryRemoveFieldsResponse, ErrorResponseMixin):
        """Contains information about an error returned from a request:

        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

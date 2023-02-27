from abc import ABC

from ...mixins import ErrorResponseMixin
from ...response import CacheResponse


class CacheSetRemoveElementsResponse(CacheResponse):
    """Parent response type for a `set_remove_elements` request.

    Its subtypes are:
    - `CacheSetRemoveElements.Success`
    - `CacheSetRemoveElements.Error`

    See `CacheClient` for how to work with responses.
    """


class CacheSetRemoveElements(ABC):
    """Groups all `CacheSetRemoveElementsResponse` derived types under a common namespace."""

    class Success(CacheSetRemoveElementsResponse):
        """Indicates the elements were removed."""

    class Error(CacheSetRemoveElementsResponse, ErrorResponseMixin):
        """Contains information about an error returned from a request.

        This includes:
        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

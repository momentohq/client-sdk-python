from abc import ABC

from ...mixins import ErrorResponseMixin
from ...response import CacheResponse


class CacheSetRemoveElementResponse(CacheResponse):
    """Parent response type for a `set_remove_element` request.

    Its subtypes are:
    - `CacheSetRemoveElement.Success`
    - `CacheSetRemoveElement.Error`

    See `CacheClient` for how to work with responses.
    """


class CacheSetRemoveElement(ABC):
    """Groups all `CacheSetRemoveElementResponse` derived types under a common namespace."""

    class Success(CacheSetRemoveElementResponse):
        """Indicates the element was removed."""

    class Error(CacheSetRemoveElementResponse, ErrorResponseMixin):
        """Contains information about an error returned from a request.

        This includes:
        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

from abc import ABC

from ...mixins import ErrorResponseMixin
from ...response import CacheResponse


class CacheSetAddElementResponse(CacheResponse):
    """Parent response type for a `set_add_element` request.

    Its subtypes are:
    - `CacheSetAddElement.Success`
    - `CacheSetAddElement.Error`

    See `CacheClient` for how to work with responses.
    """


class CacheSetAddElement(ABC):
    """Groups all `CacheSetAddElementResponse` derived types under a common namespace."""

    class Success(CacheSetAddElementResponse):
        """Indicates the element was added."""

    class Error(CacheSetAddElementResponse, ErrorResponseMixin):
        """Contains information about an error returned from a request.

        This includes:
        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

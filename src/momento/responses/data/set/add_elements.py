from abc import ABC

from ...mixins import ErrorResponseMixin
from ...response import CacheResponse


class CacheSetAddElementsResponse(CacheResponse):
    """Parent response type for a `set_add_elements` request.

    Its subtypes are:
    - `CacheSetAddElements.Success`
    - `CacheSetAddElements.Error`

    See `CacheClient` for how to work with responses.
    """


class CacheSetAddElements(ABC):
    """Groups all `CacheSetAddElementsResponse` derived types under a common namespace."""

    class Success(CacheSetAddElementsResponse):
        """Indicates the elements were added."""

    class Error(CacheSetAddElementsResponse, ErrorResponseMixin):
        """Contains information about an error returned from a request.

        This includes:
        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

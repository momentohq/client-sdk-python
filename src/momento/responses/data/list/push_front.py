from abc import ABC
from dataclasses import dataclass

from ...mixins import ErrorResponseMixin
from ...response import CacheResponse


class CacheListPushFrontResponse(CacheResponse):
    """Response type for a `list_push_front` request.

    Its subtypes are:
    - `CacheListPushFront.Success`
    - `CacheListPushFront.Error`

    See `CacheClient` for how to work with responses.
    """


class CacheListPushFront(ABC):
    """Groups all `CacheListPushFrontResponse` derived types under a common namespace."""

    @dataclass
    class Success(CacheListPushFrontResponse):
        """Indicates the push was successful."""

        list_length: int
        """The number of values in the list after this push"""

    class Error(CacheListPushFrontResponse, ErrorResponseMixin):
        """Indicates an error occured in the request.

        This includes:
        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

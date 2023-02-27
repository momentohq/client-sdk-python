from abc import ABC
from dataclasses import dataclass

from ...mixins import ErrorResponseMixin
from ...response import CacheResponse


class CacheListConcatenateFrontResponse(CacheResponse):
    """Response type for a `list_concatenate_front` request.

    Its subtypes are:
    - `CacheListConcatenateFront.Success`
    - `CacheListConcatenateFront.Error`

    See `CacheClient` for how to work with responses.
    """


class CacheListConcatenateFront(ABC):
    """Groups all `CacheListConcatenateFrontResponse` derived types under a common namespace."""

    @dataclass
    class Success(CacheListConcatenateFrontResponse):
        """Indicates the concatenation was successful."""

        list_length: int
        """The number of values in the list after this concatenation"""

    class Error(CacheListConcatenateFrontResponse, ErrorResponseMixin):
        """Indicates an error occurred in the request.

        This includes:
        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

from abc import ABC
from dataclasses import dataclass

from ..mixins import ErrorResponseMixin
from ..response import CacheResponse


class CacheListPushBackResponse(CacheResponse):
    """Response type for a `list_push_back` request. Its subtypes are:

    - `CacheListPushBack.Success`
    - `CacheListPushBack.Error`

    See `SimpleCacheClient` for how to work with responses.
    """


class CacheListPushBack(ABC):
    """Groups all `CacheListPushBackResponse` derived types under a common namespace."""

    @dataclass(repr=False)
    class Success(CacheListPushBackResponse):
        """Indicates the push was successful."""

        list_length: int
        """The number of values in the list after this push"""

    class Error(CacheListPushBackResponse, ErrorResponseMixin):
        """Indicates an error occured in the request:

        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

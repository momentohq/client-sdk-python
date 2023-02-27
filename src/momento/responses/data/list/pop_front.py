from abc import ABC
from dataclasses import dataclass

from ...mixins import ErrorResponseMixin, ValueStringMixin
from ...response import CacheResponse


class CacheListPopFrontResponse(CacheResponse):
    """Parent response type for a `list_pop_front` request.

    Its subtypes are:
    - `CacheListPopFront.Hit`
    - `CacheListPopFront.Miss`
    - `CacheListPopFront.Error`

    See `CacheClient` for how to work with responses.
    """


class CacheListPopFront(ABC):
    """Groups all `CacheListPopFront` derived types under a common namespace."""

    @dataclass
    class Hit(CacheListPopFrontResponse, ValueStringMixin):
        """Indicates the request was successful."""

        value_bytes: bytes
        """The value popped from the list. Use the `value_string` property to access the value as a string."""

    class Miss(CacheListPopFrontResponse):
        """Indicates the list does not exist."""

    class Error(CacheListPopFrontResponse, ErrorResponseMixin):
        """Contains information about an error returned from a request.

        This includes:
        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

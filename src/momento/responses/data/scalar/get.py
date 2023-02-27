from abc import ABC
from dataclasses import dataclass

from ...mixins import ErrorResponseMixin, ValueStringMixin
from ...response import CacheResponse


class CacheGetResponse(CacheResponse):
    """Parent response type for a cache `get` request.

    Its subtypes are:
    - `CacheGet.Hit`
    - `CacheGet.Miss`
    - `CacheGet.Error`

    See `CacheClient` for how to work with responses.
    """


class CacheGet(ABC):
    """Groups all `CacheGetResponse` derived types under a common namespace."""

    @dataclass
    class Hit(CacheGetResponse, ValueStringMixin):
        """Contains the result of a cache hit."""

        value_bytes: bytes
        """The value returned from the cache for the specified key. Use the
        `value_string` property to access the value as a string."""

    class Miss(CacheGetResponse):
        """Contains the results of a cache miss."""

    class Error(CacheGetResponse, ErrorResponseMixin):
        """Contains information about an error returned from a request.

        This includes:
        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

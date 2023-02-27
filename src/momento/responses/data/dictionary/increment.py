from abc import ABC
from dataclasses import dataclass

from ...mixins import ErrorResponseMixin
from ...response import CacheResponse


class CacheDictionaryIncrementResponse(CacheResponse):
    """Parent response type for a cache `dictionary_increment` request.

    Its subtypes are:
    - `CacheDictionaryIncrement.Success`
    - `CacheDictionaryIncrement.Error`

    See `CacheClient` for how to work with responses.
    """


class CacheDictionaryIncrement(ABC):
    """Groups all `CacheDictionaryIncrementResponse` derived types under a common namespace."""

    @dataclass
    class Success(CacheDictionaryIncrementResponse):
        """Indicates the request was successful."""

        value: int
        """The value of the field post-increment."""

    class Error(CacheDictionaryIncrementResponse, ErrorResponseMixin):
        """Contains information about an error returned from a request.

        This includes:
        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

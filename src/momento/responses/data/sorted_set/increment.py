from abc import ABC
from dataclasses import dataclass

from ...mixins import ErrorResponseMixin
from ...response import CacheResponse


class CacheSortedSetIncrementResponse(CacheResponse):
    """Parent response type for a cache `sorted_set_increment` request.

    Its subtypes are:
    - `CacheSortedSetIncrement.Success`
    - `CacheSortedSetIncrement.Error`

    See `CacheClient` for how to work with responses.
    """


class CacheSortedSetIncrement(ABC):
    """Groups all `CacheSortedSetIncrementResponse` derived types under a common namespace."""

    @dataclass
    class Success(CacheSortedSetIncrementResponse):
        """Indicates the request was successful."""

        score: float
        """The score of the element post-increment."""

    class Error(CacheSortedSetIncrementResponse, ErrorResponseMixin):
        """Contains information about an error returned from a request.

        This includes:
        - `error_code`: `MomentoErrorCode` value for the error.
        - `message`: a detailed error message.
        """

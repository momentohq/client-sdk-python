from abc import ABC
from dataclasses import dataclass

from ...mixins import ErrorResponseMixin
from ...response import CacheResponse


class CacheSortedSetGetRankResponse(CacheResponse):
    """Parent response type for a cache `sorted_set_get_rank` request.

    Its subtypes are:
    - `CacheSortedSetGetRank.Hit`
    - `CacheSortedSetGetRank.Miss`
    - `CacheSortedSetGetRank.Error`

    See `CacheClient` for how to work with responses.
    """


class CacheSortedSetGetRank(ABC):
    """Groups all `CacheSortedSetGetRankResponse` derived types under a common namespace."""

    @dataclass
    class Hit(CacheSortedSetGetRankResponse):
        """Contains the result of a cache hit."""

        rank: float
        """The rank of the sorted set value that was queried."""

    @dataclass
    class Miss(CacheSortedSetGetRankResponse):
        """Indicates the sorted set or sorted set element does not exist."""

    class Error(CacheSortedSetGetRankResponse, ErrorResponseMixin):
        """Contains information about an error returned from a request.

        This includes:
        - `error_code`: `MomentoErrorCode` value for the error.
        - `message`: a detailed error message.
        """

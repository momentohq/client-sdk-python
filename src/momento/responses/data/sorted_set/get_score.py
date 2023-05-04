from abc import ABC
from dataclasses import dataclass

from ...mixins import ErrorResponseMixin, ValueStringMixin
from ...response import CacheResponse


class CacheSortedSetGetScoreResponse(CacheResponse):
    """Parent response type for a cache `sorted_set_get_score` request.

    Its subtypes are:
    - `CacheSortedSetGetScore.Hit`
    - `CacheSortedSetGetScore.Miss`
    - `CacheSortedSetGetScore.Error`

    See `CacheClient` for how to work with responses.
    """


class CacheSortedSetGetScore(ABC):
    """Groups all `CacheSortedSetGetScoreResponse` derived types under a common namespace."""

    @dataclass
    class Hit(CacheSortedSetGetScoreResponse, ValueStringMixin):
        """Contains the result of a cache hit."""

        value_bytes: bytes
        """The value for which the score was queried. Use the
        `value_string` property to access the value as a string."""

        score: float
        """The score of the sorted set value that was queried."""

    @dataclass
    class Miss(CacheSortedSetGetScoreResponse, ValueStringMixin):
        """Indicates the sorted set or sorted set element does not exist."""

        value_bytes: bytes
        """The value that did not have a score. Use the
        `value_string` property to access the value as a string."""

    class Error(CacheSortedSetGetScoreResponse, ErrorResponseMixin):
        """Contains information about an error returned from a request.

        This includes:
        - `error_code`: `MomentoErrorCode` value for the error.
        - `message`: a detailed error message.
        """

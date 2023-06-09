from abc import ABC
from dataclasses import dataclass

from ...mixins import ErrorResponseMixin
from ...response import CacheResponse


class CacheSortedSetIncrementScoreResponse(CacheResponse):
    """Parent response type for a cache `sorted_set_increment_score` request.

    Its subtypes are:
    - `CacheSortedSetIncrementScore.Success`
    - `CacheSortedSetIncrementScore.Error`

    See `CacheClient` for how to work with responses.
    """


class CacheSortedSetIncrementScore(ABC):
    """Groups all `CacheSortedSetIncrementScoreResponse` derived types under a common namespace."""

    @dataclass
    class Success(CacheSortedSetIncrementScoreResponse):
        """Indicates the request was successful."""

        score: float
        """The score of the element post-increment."""

    class Error(CacheSortedSetIncrementScoreResponse, ErrorResponseMixin):
        """Contains information about an error returned from a request.

        This includes:
        - `error_code`: `MomentoErrorCode` value for the error.
        - `message`: a detailed error message.
        """

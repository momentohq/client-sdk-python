from __future__ import annotations

from abc import ABC
from dataclasses import dataclass

from ...mixins import ErrorResponseMixin
from ...response import CacheResponse
from .get_score import CacheSortedSetGetScore, CacheSortedSetGetScoreResponse


class CacheSortedSetGetScoresResponse(CacheResponse):
    """Parent response type for a cache `sorted_set_get_scores` request.

    Its subtypes are:
    - `CacheSortedSetGetScores.Hit`
    - `CacheSortedSetGetScores.Miss`
    - `CacheSortedSetGetScores.Error`

    See `CacheClient` for how to work with responses.
    """


class CacheSortedSetGetScores(ABC):
    """Groups all `CacheSortedSetGetScoresResponse` derived types under a common namespace."""

    @dataclass
    class Hit(CacheSortedSetGetScoresResponse):
        """Contains the result of a cache hit."""

        responses: list[CacheSortedSetGetScoreResponse]
        """The value returned from the cache for the specified field. Use the
        `value_string` property to access the value as a string."""

        @property
        def value_dictionary_bytes(self) -> dict[bytes, float]:
            """The values for the fetched sorted set, as bytes to their float scores.

            Returns:
                dict[bytes, float]
            """
            return {
                response.value_bytes: response.score
                for response in self.responses
                if isinstance(response, CacheSortedSetGetScore.Hit)
            }

        @property
        def value_dictionary_string(self) -> dict[str, float]:
            """The values for the fetched sorted set, as utf-8 encoded strings to their float scores.

            Returns:
                dict[str, float]
            """
            return {
                response.value_string: response.score
                for response in self.responses
                if isinstance(response, CacheSortedSetGetScore.Hit)
            }

    class Miss(CacheSortedSetGetScoresResponse):
        """Indicates the sorted set does not exist."""

    class Error(CacheSortedSetGetScoresResponse, ErrorResponseMixin):
        """Contains information about an error returned from a request.

        This includes:
        - `error_code`: `MomentoErrorCode` value for the error.
        - `message`: a detailed error message.
        """

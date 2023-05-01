from __future__ import annotations

from abc import ABC
from dataclasses import dataclass

from ...mixins import ErrorResponseMixin
from ...response import CacheResponse


class CacheSortedSetFetchResponse(CacheResponse):
    """Response type for a `sorted_set_fetch` request.

    Its subtypes are:
    - `CacheSortedSetFetch.Hit`
    - `CacheSortedSetFetch.Miss`
    - `CacheSortedSetFetch.Error`

    See `CacheClient` for how to work with responses.
    """


class CacheSortedSetFetch(ABC):
    """Groups all `CacheSortedSetFetchResponse` derived types under a common namespace."""

    @dataclass
    class Hit(CacheSortedSetFetchResponse):
        """Indicates the sorted set exists and its values were fetched."""

        value_dictionary_bytes: dict[bytes, float]
        """The values for the fetched sorted set, as bytes to their float scores."""

        @property
        def value_dictionary_string(self) -> dict[str, float]:
            """The values for the fetched sorted set, as utf-8 encoded strings and their int scores.

            Returns:
                dict[str, float]
            """
            return {key.decode("utf-8"): value for key, value in self.value_dictionary_bytes.items()}

    class Miss(CacheSortedSetFetchResponse):
        """Indicates the sorted set does not exist."""

    class Error(CacheSortedSetFetchResponse, ErrorResponseMixin):
        """Indicates an error occurred in the request.

        This includes:
        - `error_code`: `MomentoErrorCode` value for the error.
        - `message`: a detailed error message.
        """

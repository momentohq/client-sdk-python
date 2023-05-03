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

        value_list_bytes: list[tuple[bytes, float]]
        """The values for the fetched sorted set, as a list of bytes values to float score tuples."""

        @property
        def value_list_string(self) -> list[tuple[str, float]]:
            """The values for the fetched sorted set, as a list of utf-8 encoded string values to float score tuples.

            Returns:
                list[tuple[str, float]]
            """
            return [(key.decode("utf-8"), value) for key, value in self.value_list_bytes]

    class Miss(CacheSortedSetFetchResponse):
        """Indicates the sorted set does not exist."""

    class Error(CacheSortedSetFetchResponse, ErrorResponseMixin):
        """Indicates an error occurred in the request.

        This includes:
        - `error_code`: `MomentoErrorCode` value for the error.
        - `message`: a detailed error message.
        """

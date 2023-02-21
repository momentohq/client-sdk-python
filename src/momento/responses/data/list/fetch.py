from __future__ import annotations

from abc import ABC
from dataclasses import dataclass

from ...mixins import ErrorResponseMixin
from ...response import CacheResponse


class CacheListFetchResponse(CacheResponse):
    """Response type for a `list_fetch` request. Its subtypes are:

    - `CacheListFetch.Hit`
    - `CacheListFetch.Miss`
    - `CacheListFetch.Error`

    See `SimpleCacheClient` for how to work with responses.
    """


class CacheListFetch(ABC):
    """Groups all `CacheListFetchResponse` derived types under a common namespace."""

    @dataclass
    class Hit(CacheListFetchResponse):
        """Indicates the list exists and its values were fetched."""

        values_bytes: list[bytes]
        """The values for the fetched list, as bytes.

        Returns:
            list[bytes]
        """

        @property
        def values_string(self) -> list[str]:
            """The values for the fetched list, as utf-8 encoded strings.

            Returns:
                list[str]
            """

            return [v.decode("utf-8") for v in self.values_bytes]

    class Miss(CacheListFetchResponse):
        """Indicates the list does not exist."""

    class Error(CacheListFetchResponse, ErrorResponseMixin):
        """Indicates an error occured in the request:

        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

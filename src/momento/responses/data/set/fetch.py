from __future__ import annotations

from abc import ABC
from dataclasses import dataclass

from ...mixins import ErrorResponseMixin
from ...response import CacheResponse


class CacheSetFetchResponse(CacheResponse):
    """Parent response type for a `set_fetch` request.

    Its subtypes are:
    - `CacheSetFetch.Hit`
    - `CacheSetFetch.Miss`
    - `CacheSetFetch.Error`

    See `CacheClient` for how to work with responses.
    """


class CacheSetFetch(ABC):
    """Groups all `CacheSetFetchResponse` derived types under a common namespace."""

    @dataclass
    class Hit(CacheSetFetchResponse):
        """Indicates the set exists and its values were fetched."""

        value_set_bytes: set[bytes]
        """The elements as a Python set.

        Use value_set_string to get the elements as a set.
        """

        @property
        def value_set_string(self) -> set[str]:
            """The elements of the set, as utf-8 encoded strings.

            Returns:
                TSetElementsOutputStr
            """
            return {v.decode("utf-8") for v in self.value_set_bytes}

    class Miss(CacheSetFetchResponse):
        """Indicates the set does not exist."""

    class Error(CacheSetFetchResponse, ErrorResponseMixin):
        """Indicates an error occured in the request.

        This includes:
        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

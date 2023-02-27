from __future__ import annotations

from abc import ABC
from dataclasses import dataclass

from ...mixins import ErrorResponseMixin
from ...response import CacheResponse


class CacheDictionaryFetchResponse(CacheResponse):
    """Response type for a `dictionary_fetch` request.

    Its subtypes are:
    - `CacheDictionaryFetch.Hit`
    - `CacheDictionaryFetch.Miss`
    - `CacheDictionaryFetch.Error`

    See `CacheClient` for how to work with responses.
    """


class CacheDictionaryFetch(ABC):
    """Groups all `CacheDictionaryFetchResponse` derived types under a common namespace."""

    @dataclass
    class Hit(CacheDictionaryFetchResponse):
        """Indicates the dictionary exists and its items were fetched."""

        value_dictionary_bytes_bytes: dict[bytes, bytes]
        """The items for the fetched dictionary, as a mapping from bytes to bytes.

        Returns:
            dict[bytes, bytes]
        """

        @property
        def value_dictionary_string_bytes(self) -> dict[str, bytes]:
            """The items for the fetched dictionary, as a mapping from utf-8 encoded strings to bytes.

            Returns:
                dict[str, bytes]
            """
            return {k.decode("utf-8"): v for k, v in self.value_dictionary_bytes_bytes.items()}

        @property
        def value_dictionary_string_string(self) -> dict[str, str]:
            """The items for the fetched dictionary, as a mapping from utf-8 encoded strings to utf-8 encoded strings.

            Returns:
                dict[str, str]
            """
            return {k.decode("utf-8"): v.decode("utf-8") for k, v in self.value_dictionary_bytes_bytes.items()}

    class Miss(CacheDictionaryFetchResponse):
        """Indicates the dictionary does not exist."""

    class Error(CacheDictionaryFetchResponse, ErrorResponseMixin):
        """Indicates an error occured in the request.

        This includes:
        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

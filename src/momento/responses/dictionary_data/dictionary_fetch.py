from abc import ABC
from dataclasses import dataclass

from momento.errors import SdkException
from momento.typing import TDictionaryBytesBytes, TDictionaryStrBytes, TDictionaryStrStr

from ..mixins import ErrorResponseMixin
from ..response import CacheResponse


class CacheDictionaryFetchResponse(CacheResponse):
    """Response type for a `dictionary_fetch` request. Its subtypes are:

    - `CacheDictionaryFetch.Hit`
    - `CacheDictionaryFetch.Miss`
    - `CacheDictionaryFetch.Error`

    See `SimpleCacheClient` for how to work with responses.
    """


class CacheDictionaryFetch(ABC):
    """Groups all `CacheDictionaryFetchResponse` derived types under a common namespace."""

    @dataclass
    class Hit(CacheDictionaryFetchResponse):
        """Indicates the dictionary exists and its items were fetched."""

        value_dictionary_bytes_bytes: TDictionaryBytesBytes
        """The items for the fetched dictionary, as a mapping from bytes to bytes.

        Returns:
            TDictionaryBytesBytes
        """

        @property
        def value_dictionary_string_bytes(self) -> TDictionaryStrBytes:
            """The items for the fetched dictionary, as a mapping from utf-8 encoded strings to bytes.

            Returns:
                TDictionaryStrBytes
            """

            return {k.decode("utf-8"): v for k, v in self.value_dictionary_bytes_bytes.items()}

        @property
        def value_dictionary_string_string(self) -> TDictionaryStrStr:
            """The items for the fetched dictionary, as a mapping from utf-8 encoded strings to utf-8 encoded strings.

            Returns:
                TDictionaryStrStr
            """

            return {k.decode("utf-8"): v.decode("utf-8") for k, v in self.value_dictionary_bytes_bytes.items()}

    @dataclass
    class Miss(CacheDictionaryFetchResponse):
        """Indicates the dictionary does not exist."""

    @dataclass
    class Error(CacheDictionaryFetchResponse, ErrorResponseMixin):
        """Indicates an error occured in the request:

        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

        _error: SdkException

        def __init__(self, _error: SdkException):
            self._error = _error

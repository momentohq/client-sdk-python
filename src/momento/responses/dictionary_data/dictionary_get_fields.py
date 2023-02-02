from abc import ABC
from dataclasses import dataclass
from typing import List

from momento.errors import SdkException
from momento.typing import (
    TDictionaryBytesBytes,
    TDictionaryBytesStr,
    TDictionaryStrBytes,
    TDictionaryStrStr,
)

from ..mixins import ErrorResponseMixin, ValueStringMixin
from ..response import CacheResponse
from .dictionary_get_field import (
    CacheDictionaryGetField,
    CacheDictionaryGetFieldResponse,
)


class CacheDictionaryGetFieldsResponse(CacheResponse):
    """Parent response type for a cache `dictionary_get_fields` request. Its subtypes are:

    - `CacheDictionaryGetFields.Hit`
    - `CacheDictionaryGetFields.Miss`
    - `CacheDictionaryGetFields.Error`

    See `SimpleCacheClient` for how to work with responses.
    """


class CacheDictionaryGetFields(ABC):
    """Groups all `CacheDictionaryGetFieldsResponse` derived types under a common namespace."""

    @dataclass
    class Hit(CacheDictionaryGetFieldsResponse, ValueStringMixin):
        """Contains the result of a cache hit."""

        responses: List[CacheDictionaryGetFieldResponse]
        """The value returned from the cache for the specified field. Use the
        `value_string` property to access the value as a string."""

        @property
        def value_dictionary_bytes_bytes(self) -> TDictionaryBytesBytes:
            """The items for the dictionary fields, as a mapping from bytes to bytes.

            Returns:
                TDictionaryBytesBytes
            """
            return {
                response.field_bytes: response.value_bytes
                for response in self.responses
                if isinstance(response, CacheDictionaryGetField.Hit)
            }

        @property
        def value_dictionary_bytes_string(self) -> TDictionaryBytesStr:
            """The items for the dictionary fields, as a mapping from bytes to utf-8 encoded strings.

            Returns:
                TDictionaryBytesStr
            """
            return {
                response.field_bytes: response.value_string
                for response in self.responses
                if isinstance(response, CacheDictionaryGetField.Hit)
            }

        @property
        def value_dictionary_string_bytes(self) -> TDictionaryStrBytes:
            """The items for the dictionary fields, as a mapping from utf-8 encoded strings to bytes.

            Returns:
                TDictionaryStrBytes
            """
            return {
                response.field_string: response.value_bytes
                for response in self.responses
                if isinstance(response, CacheDictionaryGetField.Hit)
            }

        @property
        def value_dictionary_string_string(self) -> TDictionaryStrStr:
            """The items for the dictionary fields, as a mapping from utf-8 encoded strings to utf-8 encoded strings.

            Returns:
                TDictionaryStrStr
            """
            return {
                response.field_string: response.value_string
                for response in self.responses
                if isinstance(response, CacheDictionaryGetField.Hit)
            }

    @dataclass
    class Miss(CacheDictionaryGetFieldsResponse):
        """Indicates the dictionary does not exist."""

    @dataclass
    class Error(CacheDictionaryGetFieldsResponse, ErrorResponseMixin):
        """Contains information about an error returned from a request:

        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

        _error: SdkException

        def __init__(self, _error: SdkException):
            self._error = _error

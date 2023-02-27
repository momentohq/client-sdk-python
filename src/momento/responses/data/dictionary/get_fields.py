from __future__ import annotations

from abc import ABC
from dataclasses import dataclass

from ...mixins import ErrorResponseMixin
from ...response import CacheResponse
from .get_field import CacheDictionaryGetField, CacheDictionaryGetFieldResponse


class CacheDictionaryGetFieldsResponse(CacheResponse):
    """Parent response type for a cache `dictionary_get_fields` request.

    Its subtypes are:
    - `CacheDictionaryGetFields.Hit`
    - `CacheDictionaryGetFields.Miss`
    - `CacheDictionaryGetFields.Error`

    See `CacheClient` for how to work with responses.
    """


class CacheDictionaryGetFields(ABC):
    """Groups all `CacheDictionaryGetFieldsResponse` derived types under a common namespace."""

    @dataclass
    class Hit(CacheDictionaryGetFieldsResponse):
        """Contains the result of a cache hit."""

        responses: list[CacheDictionaryGetFieldResponse]
        """The value returned from the cache for the specified field. Use the
        `value_string` property to access the value as a string."""

        @property
        def value_dictionary_bytes_bytes(self) -> dict[bytes, bytes]:
            """The items for the dictionary fields, as a mapping from bytes to bytes.

            Returns:
                dict[bytes, bytes]
            """
            return {
                response.field_bytes: response.value_bytes
                for response in self.responses
                if isinstance(response, CacheDictionaryGetField.Hit)
            }

        @property
        def value_dictionary_bytes_string(self) -> dict[bytes, str]:
            """The items for the dictionary fields, as a mapping from bytes to utf-8 encoded strings.

            Returns:
                dict[bytes, str]
            """
            return {
                response.field_bytes: response.value_string
                for response in self.responses
                if isinstance(response, CacheDictionaryGetField.Hit)
            }

        @property
        def value_dictionary_string_bytes(self) -> dict[str, bytes]:
            """The items for the dictionary fields, as a mapping from utf-8 encoded strings to bytes.

            Returns:
                dict[str, bytes]
            """
            return {
                response.field_string: response.value_bytes
                for response in self.responses
                if isinstance(response, CacheDictionaryGetField.Hit)
            }

        @property
        def value_dictionary_string_string(self) -> dict[str, str]:
            """The items for the dictionary fields, as a mapping from utf-8 encoded strings to utf-8 encoded strings.

            Returns:
                dict[str, str]
            """
            return {
                response.field_string: response.value_string
                for response in self.responses
                if isinstance(response, CacheDictionaryGetField.Hit)
            }

    class Miss(CacheDictionaryGetFieldsResponse):
        """Indicates the dictionary does not exist."""

    class Error(CacheDictionaryGetFieldsResponse, ErrorResponseMixin):
        """Contains information about an error returned from a request.

        This includes:
        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

from abc import ABC
from dataclasses import dataclass

from ...mixins import ErrorResponseMixin, ValueStringMixin
from ...response import CacheResponse


class CacheDictionaryGetFieldResponse(CacheResponse):
    """Parent response type for a cache `dictionary_get_field` request.

    Its subtypes are:
    - `CacheDictionaryGetField.Hit`
    - `CacheDictionaryGetField.Miss`
    - `CacheDictionaryGetField.Error`

    See `CacheClient` for how to work with responses.
    """


class CacheDictionaryGetField(ABC):
    """Groups all `CacheDictionaryGetFieldResponse` derived types under a common namespace."""

    @dataclass
    class Hit(CacheDictionaryGetFieldResponse, ValueStringMixin):
        """Contains the result of a cache hit."""

        value_bytes: bytes
        """The value returned from the cache for the specified field. Use the
        `value_string` property to access the value as a string."""

        field_bytes: bytes
        """The dictionary field that was queried."""

        @property
        def field_string(self) -> str:
            """Convert the bytes `field` to a UTF-8 string.

            Returns:
                str: UTF-8 representation of the `field`
            """
            return self.field_bytes.decode("utf-8")

    class Miss(CacheDictionaryGetFieldResponse):
        """Indicates the dictionary or dictionary field does not exist."""

    class Error(CacheDictionaryGetFieldResponse, ErrorResponseMixin):
        """Contains information about an error returned from a request.

        This includes:
        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

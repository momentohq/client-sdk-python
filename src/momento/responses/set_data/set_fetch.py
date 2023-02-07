from abc import ABC
from dataclasses import dataclass

from momento.errors import SdkException
from momento.typing import TSetElementsOutputBytes, TSetElementsOutputStr

from ..mixins import ErrorResponseMixin
from ..response import CacheResponse


class CacheSetFetchResponse(CacheResponse):
    """Response type for a `set_fetch` request. Its subtypes are:

    - `CacheSetFetch.Hit`
    - `CacheSetFetch.Miss`
    - `CacheSetFetch.Error`

    See `SimpleCacheClient` for how to work with responses.
    """


class CacheSetFetch(ABC):
    """Groups all `CacheSetFetchResponse` derived types under a common namespace."""

    @dataclass
    class Hit(CacheSetFetchResponse):
        """Indicates the set exists and its values were fetched."""

        value_set_bytes: TSetElementsOutputBytes
        """The elements as a Python set.

        Use value_set_string to get the elements as a set.
        """

        @property
        def value_set_string(self) -> TSetElementsOutputStr:
            """The elements of the set, as utf-8 encoded strings.

            Returns:
                TSetElementsOutputStr
            """

            return {v.decode("utf-8") for v in self.value_set_bytes}

    @dataclass
    class Miss(CacheSetFetchResponse):
        """Indicates the set does not exist."""

    @dataclass
    class Error(CacheSetFetchResponse, ErrorResponseMixin):
        """Indicates an error occured in the request:

        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

        _error: SdkException

        def __init__(self, _error: SdkException):
            self._error = _error
from abc import ABC
from dataclasses import dataclass

from momento.errors import SdkException

from ...mixins import ErrorResponseMixin
from ...response import CacheResponse


class CacheIncrementResponse(CacheResponse):
    """Parent response type for a cache `increment` request.

    Its subtypes are:
    - `CacheIncrement.Success`
    - `CacheIncrement.Error`

    See `CacheClient` for how to work with responses.
    """


class CacheIncrement(ABC):
    """Groups all `CacheIncrementResponse` derived types under a common namespace."""

    @dataclass
    class Success(CacheIncrementResponse):
        """Indicates the increment was successful."""

        value: int
        """The value of the field post-increment."""

    @dataclass
    class Error(CacheIncrementResponse, ErrorResponseMixin):
        """Contains information about an error returned from a request.

        This includes:
        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

        _error: SdkException

        def __init__(self, _error: SdkException):
            self._error = _error

from abc import ABC

from ...mixins import ErrorResponseMixin
from ...response import CacheResponse


class CacheSetIfNotExistsResponse(CacheResponse):
    """Parent response type for a cache `set_if_not_exists` request.

    Its subtypes are:
    - `CacheSetIfNotExists.Stored`
    - `CacheSetIfNotExists.NotStored`
    - `CacheSetIfNotExists.Error`

    See `CacheClient` for how to work with responses.
    """


class CacheSetIfNotExists(ABC):
    """Groups all `CacheSetIfNotExistsResponse` derived types under a common namespace."""

    class Stored(CacheSetIfNotExistsResponse):
        """Indicates the key did not exist and the value was set."""

    class NotStored(CacheSetIfNotExistsResponse):
        """Indicates the key existed and no value was set."""

    class Error(CacheSetIfNotExistsResponse, ErrorResponseMixin):
        """Contains information about an error returned from a request.

        This includes:
        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

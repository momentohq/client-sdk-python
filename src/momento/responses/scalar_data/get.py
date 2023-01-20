from abc import ABC
from dataclasses import dataclass

from momento.errors import SdkException

from ..mixins import ErrorResponseMixin, ValueStringMixin


class CacheGetResponse(ABC):
    """Parent response type for a create get request. The
    response object is resolved to a type-safe object of one of
    the following subtypes:

    - `CacheGet.Hit`
    - `CacheGet.Miss`
    - `CacheGet.Error`

    Pattern matching can be used to operate on the appropriate subtype.
    For example, in python 3.10+:

        match response:
            case CacheGet.Hit() as hit:
                return hit.value_string
            case CacheGet.Miss():
                ... # Handle miss
            case CacheGet.Error():
                ...

    or equivalently in earlier versions of python:

        if isinstance(response, CacheGet.Hit):
            ...
        elif isinstance(response, CacheGet.Miss):
            ...
        elif isinstance(response, CacheGet.Error):
            ...
        else:
            # Shouldn't happen
    """


class CacheGet(ABC):
    """Groups all `CacheGetResponse` derived types under a common namespace."""

    @dataclass
    class Hit(CacheGetResponse, ValueStringMixin):
        """Indicates the request was successful."""

        value_bytes: bytes
        """The value returned from the cache for the specified key. Use the
        `value_string` property to access the value as a string."""

    @dataclass
    class Miss(CacheGetResponse):
        """Contains the results of a cache miss"""

    @dataclass
    class Error(CacheGetResponse, ErrorResponseMixin):
        """Contains information about an error returned from a request:

        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

        _error: SdkException

        def __init__(self, _error: SdkException):
            self._error = _error

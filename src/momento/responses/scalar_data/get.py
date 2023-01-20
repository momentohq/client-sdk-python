from abc import ABC
from dataclasses import dataclass

from momento.errors import SdkException

from ..mixins import ErrorResponseMixin, ValueStringMixin


class CacheGetResponseBase(ABC):
    """Parent response type for a create get request. The
    response object is resolved to a type-safe object of one of
    the following subtypes:
    - `CacheGetResponse.Hit`
    - `CacheGetResponse.Miss`
    - `CacheGetResponse.Error`

    Pattern matching can be used to operate on the appropriate subtype.
    For example, in python 3.10+:
    ```
    match response:
        case CacheGetResponse.Hit() as hit:
            return hit.value_string
        case CacheGetResponse.Miss():
            ... # Handle miss
        case CacheGetResponse.Error():
            ...
    ```
    or equivalently in earlier versions of python:
    ```
    if isinstance(response, CacheGetResponse.Hit):
        ...
    elif isinstance(response, CacheGetResponse.Miss):
        ...
    elif isinstance(response, CacheGetResponse.Error):
        ...
    else:
        # Shouldn't happen
    ```
    """


class CacheGetResponse(ABC):
    """Groups all `CacheGetResponseBase` derived types under a common namespace."""

    @dataclass
    class Hit(CacheGetResponseBase, ValueStringMixin):
        """Indicates the request was successful."""

        value_bytes: bytes
        """The value returned from the cache for the specified key. Use the
        `value_string` property to access the value as a string."""

    @dataclass
    class Miss(CacheGetResponseBase):
        """Contains the results of a cache miss"""

    @dataclass
    class Error(CacheGetResponseBase, ErrorResponseMixin):
        """Contains information about an error returned from a request:

        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

        _error: SdkException

        def __init__(self, _error: SdkException):
            self._error = _error

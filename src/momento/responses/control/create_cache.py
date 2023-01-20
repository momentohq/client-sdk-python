from abc import ABC
from dataclasses import dataclass

from momento.errors import SdkException

from ..mixins import ErrorResponseMixin


class CreateCacheResponseBase(ABC):
    """Parent response type for a create cache request. The
    response object is resolved to a type-safe object of one of
    the following subtypes:

    - `CreateCacheResponse.Success`
    - `CreateCacheResponse.CacheAlreadyExists`
    - `CreateCacheResponse.Error`

    Pattern matching can be used to operate on the appropriate subtype.
    For example, in python 3.10+:

        match response:
            case CreateCacheResponse.Success():
                ...
            case CreateCacheResponse.CacheAlreadyExists():
                ...
            case CreateCacheResponse.Error():
                ...
            case _:
                # Shouldn't happen

    or equivalently in earlier versions of python:

        if isinstance(response, CreateCacheResponse.Success):
            ...
        elif isinstance(response, CreateCacheResponse.AlreadyExists):
            ...
        elif isinstance(response, CreateCacheResponse.Error):
            ...
        else:
            # Shouldn't happen
    """


class CreateCacheResponse(ABC):
    """Groups all `CreateCacheResponseBase` derived types under a common namespace."""

    @dataclass
    class Success(CreateCacheResponseBase):
        """Indicates the request was successful."""

    @dataclass
    class CacheAlreadyExists(CreateCacheResponseBase):
        """Indicates that a cache with the requested name has already been created in the requesting account."""

    @dataclass
    class Error(CreateCacheResponseBase, ErrorResponseMixin):
        """Contains information about an error returned from a request:

        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

        _error: SdkException

        def __init__(self, _error: SdkException):
            self._error = _error

from abc import ABC
from dataclasses import dataclass

from momento.errors import SdkException

from ..mixins import ErrorResponseMixin


class DeleteCacheResponse(ABC):
    """Parent response type for a delete cache request. The
    response object is resolved to a type-safe object of one of
    the following subtypes:

    - `DeleteCache.Success`
    - `DeleteCache.Error`

    Pattern matching can be used to operate on the appropriate subtype.
    For example, in python 3.10+:

        match response:
            case DeleteCache.Success():
                ...
            case DeleteCache.Error():
                ...
            case _:
                # Shouldn't happen

    or equivalently in earlier versions of python:

        if isinstance(response, DeleteCache.Success):
            ...
        elif isinstance(response, DeleteCache.Error):
            ...
        else:
            # Shouldn't happen
    """


class DeleteCache(ABC):
    """Groups all `DeleteCacheResponse` derived types under a common namespace."""

    @dataclass
    class Success(DeleteCacheResponse):
        """Indicates the request was successful."""

    @dataclass
    class Error(DeleteCacheResponse, ErrorResponseMixin):
        """Contains information about an error returned from a request:

        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

        _error: SdkException

        def __init__(self, _error: SdkException):
            self._error = _error

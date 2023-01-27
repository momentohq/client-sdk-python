from abc import ABC
from dataclasses import dataclass

from momento.errors import SdkException

from ..mixins import ErrorResponseMixin


class CacheDeleteResponse(ABC):
    """Parent response type for a create set request. The
    response object is resolved to a type-safe object of one of
    the following subtypes:

    - `CacheDelete.Success`
    - `CacheDelete.Error`

    Pattern matching can be used to operate on the appropriate subtype.
    For example, in python 3.10+:

        match response:
            case CacheDelete.Success():
                ...
            case CacheDelete.Error():
                ...

    or equivalently in earlier versions of python:

        if isinstance(response, CacheDelete.Success):
            ...
        elif isinstance(response, CacheDelete.Error):
            ...
        else:
            # Shouldn't happen
    """


class CacheDelete(ABC):
    """Groups all `CacheDeleteResponse` derived types under a common namespace."""

    @dataclass
    class Success(CacheDeleteResponse):
        """Indicates the request was successful."""

    @dataclass
    class Error(CacheDeleteResponse, ErrorResponseMixin):
        """Contains information about an error returned from a request:

        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

        _error: SdkException

        def __init__(self, _error: SdkException):
            self._error = _error

from abc import ABC
from dataclasses import dataclass

from momento.errors import SdkException

from ..mixins import ErrorResponseMixin


class CacheSetResponse(ABC):
    """Parent response type for a create set request. The
    response object is resolved to a type-safe object of one of
    the following subtypes:

    - `CacheSet.Success`
    - `CacheSet.Error`

    Pattern matching can be used to operate on the appropriate subtype.
    For example, in python 3.10+:

        match response:
            case CacheSet.Success():
                ...
            case CacheSet.Error():
                ...

    or equivalently in earlier versions of python:

        if isinstance(response, CacheSet.Success):
            ...
        elif isinstance(response, CacheSet.Error):
            ...
        else:
            # Shouldn't happen
    """


class CacheSet(ABC):
    """Groups all `CacheSetResponse` derived types under a common namespace."""

    @dataclass
    class Success(CacheSetResponse):
        """Indicates the request was successful."""

    @dataclass
    class Error(CacheSetResponse, ErrorResponseMixin):
        """Contains information about an error returned from a request:

        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

        _error: SdkException

        def __init__(self, _error: SdkException):
            self._error = _error

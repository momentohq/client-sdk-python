from abc import ABC
from dataclasses import dataclass

from momento.errors import SdkException
from ..mixins import ErrorResponseMixin


class DeleteCacheResponseBase(ABC):
    """Parent response type for a delete cache request. The
    response object is resolved to a type-safe object of one of
    the following subtypes:
    - `DeleteCacheResponse.Success`
    - `DeleteCacheResponse.Error`

    Pattern matching can be used to operate on the appropriate subtype.
    For example:
    ```
    match response:
        case DeleteCacheResponse.Success:
            ...
        case DeleteCacheResponse.Error:
            ...
    ```
    """


class DeleteCacheResponse(ABC):
    @dataclass
    class Success(DeleteCacheResponseBase):
        """Indicates the request was successful."""

    @dataclass
    class Error(DeleteCacheResponseBase, ErrorResponseMixin):
        """Contains information about an error returned from a request:

        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

        _error: SdkException

        def __init__(self, _error: SdkException):
            self._error = _error
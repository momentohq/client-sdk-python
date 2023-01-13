from abc import ABC
from dataclasses import dataclass

from momento.errors import SdkException

from ..mixins import ErrorResponseMixin


class CacheSetResponseBase(ABC):
    """Parent response type for a create set request. The
    response object is resolved to a type-safe object of one of
    the following subtypes:
    - `CacheSetResponse.Success`
    - `CacheSetResponse.Error`

    Pattern matching can be used to operate on the appropriate subtype.
    For example:
    ```
    match response:
        case CacheSetResponse.Success:
            ...
        case CacheSetResponse.Error:
            ...
    ```
    """


class CacheSetResponse(ABC):
    """Groups all `CacheSetResponseBase` derived types under a common namespace."""

    @dataclass
    class Success(CacheSetResponseBase):
        """Indicates the request was successful."""

    @dataclass
    class Error(CacheSetResponseBase, ErrorResponseMixin):
        """Contains information about an error returned from a request:

        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

        _error: SdkException

        def __init__(self, _error: SdkException):
            self._error = _error

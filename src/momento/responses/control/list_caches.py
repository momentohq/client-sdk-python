from abc import ABC
from dataclasses import dataclass
from typing import List

from momento.errors import SdkException

from ..mixins import ErrorResponseMixin


class ListCachesResponseBase(ABC):
    """Parent response type for a list caches request. The
    response object is resolved to a type-safe object of one of
    the following subtypes:
    - `ListCachesResponse.Success`
    - `ListCachesResponse.Error`

    Pattern matching can be used to operate on the appropriate subtype.
    For example:
    ```
    match response:
        case ListCachesResponse.Success:
            ...
        case ListCachesResponse.Error:
            ...
    ```
    """


@dataclass
class CacheInfo:
    """Contains a Momento cache's name."""

    name: str
    """Holds the name of the cache."""


class ListCachesResponse(ABC):
    """Groups all `ListCachesResponseBase` derived types under a common namespace."""

    @dataclass
    class Success(ListCachesResponseBase):
        """Indicates the request was successful."""

        caches: List[CacheInfo]
        """The list of caches available to the user."""
        next_token: str
        """A token to specify where to start paging. This is the `NextToken` from a previous response."""

    @dataclass
    class Error(ListCachesResponseBase, ErrorResponseMixin):
        """Contains information about an error returned from a request:

        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

        _error: SdkException

        def __init__(self, _error: SdkException):
            self._error = _error

from abc import ABC
from dataclasses import dataclass

from ...mixins import ErrorResponseMixin
from ...response import CacheResponse


class CacheSortedSetRemoveElementResponse(CacheResponse):
    """Response type for a `sorted_set_remove_element` request.

    Its subtypes are:
    - `CacheSortedSetRemoveElement.Success`
    - `CacheSortedSetRemoveElement.Error`

    See `CacheClient` for how to work with responses.
    """


class CacheSortedSetRemoveElement(ABC):
    """Groups all `CacheSortedSetRemoveElementResponse` derived types under a common namespace."""

    @dataclass
    class Success(CacheSortedSetRemoveElementResponse):
        """Indicates the remove was successful."""

    class Error(CacheSortedSetRemoveElementResponse, ErrorResponseMixin):
        """Indicates an error occurred in the request.

        This includes:
        - `error_code`: `MomentoErrorCode` value for the error.
        - `message`: a detailed error message.
        """

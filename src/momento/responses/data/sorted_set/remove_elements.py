from abc import ABC
from dataclasses import dataclass

from ...mixins import ErrorResponseMixin
from ...response import CacheResponse


class CacheSortedSetRemoveElementsResponse(CacheResponse):
    """Response type for a `sorted_set_remove_elements` request.

    Its subtypes are:
    - `CacheSortedSetRemoveElements.Success`
    - `CacheSortedSetRemoveElements.Error`

    See `CacheClient` for how to work with responses.
    """


class CacheSortedSetRemoveElements(ABC):
    """Groups all `CacheSortedSetRemoveElementsResponse` derived types under a common namespace."""

    @dataclass
    class Success(CacheSortedSetRemoveElementsResponse):
        """Indicates the remove was successful."""

    class Error(CacheSortedSetRemoveElementsResponse, ErrorResponseMixin):
        """Indicates an error occurred in the request.

        This includes:
        - `error_code`: `MomentoErrorCode` value for the error.
        - `message`: a detailed error message.
        """

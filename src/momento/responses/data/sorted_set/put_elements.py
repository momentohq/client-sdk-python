from abc import ABC
from dataclasses import dataclass

from ...mixins import ErrorResponseMixin
from ...response import CacheResponse


class CacheSortedSetPutElementsResponse(CacheResponse):
    """Response type for a `sorted_set_put_elements` request.

    Its subtypes are:
    - `CacheSortedSetPutElements.Success`
    - `CacheSortedSetPutElements.Error`

    See `CacheClient` for how to work with responses.
    """


class CacheSortedSetPutElements(ABC):
    """Groups all `CacheSortedSetPutElementsResponse` derived types under a common namespace."""

    @dataclass
    class Success(CacheSortedSetPutElementsResponse):
        """Indicates the put was successful."""

    class Error(CacheSortedSetPutElementsResponse, ErrorResponseMixin):
        """Indicates an error occurred in the request.

        This includes:
        - `error_code`: `MomentoErrorCode` value for the error.
        - `message`: a detailed error message.
        """

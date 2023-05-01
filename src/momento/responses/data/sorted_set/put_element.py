from abc import ABC
from dataclasses import dataclass

from ...mixins import ErrorResponseMixin
from ...response import CacheResponse


class CacheSortedSetPutElementResponse(CacheResponse):
    """Response type for a `sorted_set_put_element` request.

    Its subtypes are:
    - `CacheSortedSetPutElement.Success`
    - `CacheSortedSetPutElement.Error`

    See `CacheClient` for how to work with responses.
    """


class CacheSortedSetPutElement(ABC):
    """Groups all `CacheSortedSetPutElementResponse` derived types under a common namespace."""

    @dataclass
    class Success(CacheSortedSetPutElementResponse):
        """Indicates the put was successful."""

    class Error(CacheSortedSetPutElementResponse, ErrorResponseMixin):
        """Indicates an error occurred in the request.

        This includes:
        - `error_code`: `MomentoErrorCode` value for the error.
        - `message`: a detailed error message.
        """

from abc import ABC
from dataclasses import dataclass

from momento.errors import SdkException
from ..mixins import ErrorResponseMixin


class CreateCacheResponseBase(ABC):
    pass


class CreateCacheResponse(ABC):
    @dataclass
    class Success(CreateCacheResponseBase):
        """The request was successful."""

    @dataclass
    class CacheAlreadyExists(CreateCacheResponseBase):
        """Indicates that a cache with the requested name has already been created in the requesting account."""

    @dataclass
    class Error(CreateCacheResponseBase, ErrorResponseMixin):
        """Contains information about an error returned from a request."""

        _error: SdkException

        def __init__(self, _error: SdkException):
            self._error = _error

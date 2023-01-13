from abc import ABC
from dataclasses import dataclass

from momento.errors import SdkException
from ..mixins import ErrorResponseMixin


class CreateCacheResponse(ABC):
    pass


@dataclass
class CreateCacheResponseSuccess(CreateCacheResponse):
    pass


@dataclass
class CreateCacheResponseCacheAlreadyExists(CreateCacheResponse):
    pass


@dataclass
class CreateCacheResponseError(CreateCacheResponse, ErrorResponseMixin):
    """An error occurred."""

    _error: SdkException

    def __init__(self, _error: SdkException):
        self._error = _error

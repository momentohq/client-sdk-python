from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from typing import List

from momento_wire_types import controlclient_pb2 as ctrl_pb

from momento.responses.response import ControlResponse

from ...mixins import ErrorResponseMixin


class ListCachesResponse(ControlResponse):
    """Parent response type for a list caches request.

    The response object is resolved to a type-safe object of one of
    the following subtypes:
    - `ListCaches.Success`
    - `ListCaches.Error`

    Pattern matching can be used to operate on the appropriate subtype.
    For example, in python 3.10+::

        match response:
            case ListCaches.Success():
                ...
            case ListCaches.Error():
                ...
            case _:
                # Shouldn't happen

    or equivalently in earlier versions of python::

        if isinstance(response, ListCaches.Success):
            ...
        elif isinstance(response, ListCaches.Error):
            ...
        else:
            # Shouldn't happen
    """


@dataclass
class CacheInfo:
    """Contains a Momento cache's name."""

    name: str
    """Holds the name of the cache."""


class ListCaches(ABC):
    """Groups all `ListCachesResponse` derived types under a common namespace."""

    @dataclass
    class Success(ListCachesResponse):
        """Indicates the request was successful."""

        caches: List[CacheInfo]
        """The list of caches available to the user."""

        @staticmethod
        def from_grpc_response(grpc_list_cache_response: ctrl_pb._ListCachesResponse) -> ListCaches.Success:
            """Initializes ListCacheResponse to handle list cache response.

            Args:
                grpc_list_cache_response: Protobuf based response returned by Scs.
            """
            caches = [CacheInfo(cache.cache_name) for cache in grpc_list_cache_response.cache]  # type: ignore[misc]
            return ListCaches.Success(caches=caches)

    class Error(ListCachesResponse, ErrorResponseMixin):
        """Contains information about an error returned from a request.

        This includes:
        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

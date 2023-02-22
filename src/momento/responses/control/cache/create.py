from abc import ABC

from momento.responses.response import ControlResponse

from ...mixins import ErrorResponseMixin


class CreateCacheResponse(ControlResponse):
    """Parent response type for a create cache request.

    The response object is resolved to a type-safe object of one of
    the following subtypes:
    - `CreateCache.Success`
    - `CreateCache.CacheAlreadyExists`
    - `CreateCache.Error`

    Pattern matching can be used to operate on the appropriate subtype.
    For example, in python 3.10+::

        match response:
            case CreateCache.Success():
                ...
            case CreateCache.CacheAlreadyExists():
                ...
            case CreateCache.Error():
                ...
            case _:
                # Shouldn't happen

    or equivalently in earlier versions of python::

        if isinstance(response, CreateCache.Success):
            ...
        elif isinstance(response, CreateCache.AlreadyExists):
            ...
        elif isinstance(response, CreateCache.Error):
            ...
        else:
            # Shouldn't happen
    """


class CreateCache(ABC):
    """Groups all `CreateCacheResponse` derived types under a common namespace."""

    class Success(CreateCacheResponse):
        """Indicates the request was successful."""

    class CacheAlreadyExists(CreateCacheResponse):
        """Indicates that a cache with the requested name has already been created in the requesting account."""

    class Error(CreateCacheResponse, ErrorResponseMixin):
        """Contains information about an error returned from a request.

        This includes:
        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

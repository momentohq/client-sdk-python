from abc import ABC

from ...mixins import ErrorResponseMixin
from ...response import ControlResponse


class CreateIndexResponse(ControlResponse):
    """Parent response type for a create index request.

    The response object is resolved to a type-safe object of one of
    the following subtypes:
    - `CreateIndex.Success`
    - `CreateIndex.IndexAlreadyExists`
    - `CreateIndex.Error`

    Pattern matching can be used to operate on the appropriate subtype.
    For example, in python 3.10+::

        match response:
            case CreateIndex.Success():
                ...
            case CreateIndex.IndexAlreadyExists():
                ...
            case CreateIndex.Error():
                ...
            case _:
                # Shouldn't happen

    or equivalently in earlier versions of python::

        if isinstance(response, CreateIndex.Success):
            ...
        elif isinstance(response, CreateIndex.AlreadyExists):
            ...
        elif isinstance(response, CreateIndex.Error):
            ...
        else:
            # Shouldn't happen
    """


class CreateIndex(ABC):
    """Groups all `CreateIndexResponse` derived types under a common namespace."""

    class Success(CreateIndexResponse):
        """Indicates the request was successful."""

    class IndexAlreadyExists(CreateIndexResponse):
        """Indicates that a index with the requested name has already been created in the requesting account."""

    class Error(CreateIndexResponse, ErrorResponseMixin):
        """Contains information about an error returned from a request.

        This includes:
        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

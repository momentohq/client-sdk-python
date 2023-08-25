from abc import ABC

from ...mixins import ErrorResponseMixin
from ...response import ControlResponse


class DeleteIndexResponse(ControlResponse):
    """Parent response type for a delete index request.

    The response object is resolved to a type-safe object of one of
    the following subtypes:
    - `DeleteIndex.Success`
    - `DeleteIndex.Error`

    Pattern matching can be used to operate on the appropriate subtype.
    For example, in python 3.10+::

        match response:
            case DeleteIndex.Success():
                ...
            case DeleteIndex.Error():
                ...
            case _:
                # Shouldn't happen

    or equivalently in earlier versions of python::

        if isinstance(response, DeleteIndex.Success):
            ...
        elif isinstance(response, DeleteIndex.Error):
            ...
        else:
            # Shouldn't happen
    """


class DeleteIndex(ABC):
    """Groups all `DeleteIndexResponse` derived types under a common namespace."""

    class Success(DeleteIndexResponse):
        """Indicates the request was successful."""

    class Error(DeleteIndexResponse, ErrorResponseMixin):
        """Contains information about an error returned from a request.

        This includes:
        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

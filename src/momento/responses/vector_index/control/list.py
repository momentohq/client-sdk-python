from __future__ import annotations

from abc import ABC
from dataclasses import dataclass

from momento_wire_types import controlclient_pb2 as ctrl_pb

from ...mixins import ErrorResponseMixin
from ...response import ControlResponse


class ListIndexesResponse(ControlResponse):
    """Parent response type for a list indexes request.

    The response object is resolved to a type-safe object of one of
    the following subtypes:
    - `ListIndexes.Success`
    - `ListIndexes.Error`

    Pattern matching can be used to operate on the appropriate subtype.
    For example, in python 3.10+::

        match response:
            case ListIndexes.Success():
                ...
            case ListIndexes.Error():
                ...
            case _:
                # Shouldn't happen

    or equivalently in earlier versions of python::

        if isinstance(response, ListIndexes.Success):
            ...
        elif isinstance(response, ListIndexes.Error):
            ...
        else:
            # Shouldn't happen
    """


class ListIndexes(ABC):
    """Groups all `ListIndexesResponse` derived types under a common namespace."""

    @dataclass
    class Success(ListIndexesResponse):
        """Indicates the request was successful."""

        index_names: list[str]
        """The list of indexes available to the user."""

        @staticmethod
        def from_grpc_response(grpc_list_index_response: ctrl_pb._ListIndexesResponse) -> ListIndexes.Success:
            """Initializes ListIndexResponse to handle list index response.

            Args:
                grpc_list_index_response: Protobuf based response returned by Scs.
            """
            return ListIndexes.Success(
                index_names=[index_name for index_name in grpc_list_index_response.index_names]  # type: ignore[misc]
            )

    class Error(ListIndexesResponse, ErrorResponseMixin):
        """Contains information about an error returned from a request.

        This includes:
        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

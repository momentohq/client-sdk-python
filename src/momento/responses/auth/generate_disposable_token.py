from __future__ import annotations

from abc import ABC

from momento_wire_types import token_pb2 as token_pb

from momento.responses.response import AuthResponse
from momento.utilities.expiration import ExpiresAt

from ..mixins import ErrorResponseMixin


class GenerateDisposableTokenResponse(AuthResponse):
    """Parent response type for a generate disposable token request.

    The response object is resolved to a type-safe object of one of
    the following subtypes:
    - `GenerateDisposableToken.Success`
    - `GenerateDisposableToken.Error`

    Pattern matching can be used to operate on the appropriate subtype.
    For example, in python 3.10+::

        match response:
            case GenerateDisposableToken.Success():
                ...
            case GenerateDisposableToken.Error():
                ...
            case _:
                # Shouldn't happen

    or equivalently in earlier versions of python::

        if isinstance(response, GenerateDisposableToken.Success):
            ...
        elif isinstance(response, GenerateDisposableToken.Error):
            ...
        else:
            # Shouldn't happen
    """


class GenerateDisposableToken(ABC):
    """Groups all `GenerateDisposableTokenResponse` derived types under a common namespace."""

    class Success(GenerateDisposableTokenResponse):
        """Indicates the request was successful."""

        authToken: str
        """The generated disposable token."""

        endpoint: str
        """The endpoint the Momento client should use when making requests."""

        expiresAt: ExpiresAt
        """The time at which the disposable token will expire."""

        def __init__(self, authToken: str, endpoint: str, expiresAt: ExpiresAt):
            self.authToken = authToken
            self.endpoint = endpoint
            self.expiresAt = expiresAt

        @staticmethod
        def from_grpc_response(grpc_response: token_pb._GenerateDisposableTokenResponse) -> GenerateDisposableToken.Success:
            """Initializes GenerateDisposableTokenResponse to handle generate disposable token response.

            Args:
                grpc_response: Protobuf based response returned by token service.
            """
            return GenerateDisposableToken.Success(
                grpc_response.authToken, grpc_response.endpoint, ExpiresAt(grpc_response.valid_until)
            )

    class Error(GenerateDisposableTokenResponse, ErrorResponseMixin):
        """Contains information about an error returned from a request.

        This includes:
        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

from __future__ import annotations

from abc import ABC
from dataclasses import dataclass

from momento.responses.response import ControlResponse

from ...mixins import ErrorResponseMixin


class RevokeSigningKeyResponse(ControlResponse):
    """Parent response type for a cache `revoke_signing_key` request.

    Its subtypes are:
    - `RevokeSigningKey.Success`
    - `RevokeSigningKey.Error`

    See `CacheClient` for how to work with responses.
    """


class RevokeSigningKey(ABC):
    """Groups all `RevokeSigningKeyResponse` derived types under a common namespace."""

    @dataclass
    class Success(RevokeSigningKeyResponse):
        """The response from revoking a signing key."""

    class Error(RevokeSigningKeyResponse, ErrorResponseMixin):
        """Contains information about an error returned from a request.

        This includes:
        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

from __future__ import annotations

import json
from abc import ABC
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from momento.responses.response import ControlResponse

from ...mixins import ErrorResponseMixin


class CreateSigningKeyResponse(ControlResponse):
    """Parent response type for a cache `create_signing_key` request.

    Its subtypes are:
    - `CreateSigningKey.Success`
    - `CreateSigningKey.Error`

    See `CacheClient` for how to work with responses.
    """


class CreateSigningKey(ABC):
    """Groups all `CreateSigningKeyResponse` derived types under a common namespace."""

    @dataclass
    class Success(CreateSigningKeyResponse):
        """The response from creating a signing key."""

        key_id: str
        """The ID of the signing key"""
        endpoint: str
        """The endpoint of the signing key"""
        key: str
        """The signing key as a JSON string"""
        expires_at: datetime
        """When the key expires"""

        @staticmethod
        def from_grpc_response(grpc_create_signing_key_response: Any, endpoint: str) -> CreateSigningKey.Success:  # type: ignore[misc] # noqa: E501
            key: str = grpc_create_signing_key_response.key
            key_id: str = json.loads(key)["kid"]
            expires_at: datetime = datetime.fromtimestamp(grpc_create_signing_key_response.expires_at)
            return CreateSigningKey.Success(key_id, endpoint, key, expires_at)

    class Error(CreateSigningKeyResponse, ErrorResponseMixin):
        """Contains information about an error returned from a request.

        This includes:
        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

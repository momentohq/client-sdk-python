from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

import google

from momento.responses.response import ControlResponse


@dataclass
class CreateSigningKeyResponse(ControlResponse):
    """The response from creating a signing key.

    Args:
        key_id: str - the ID of the signing key
        endpoint: str - endpoint of the signing key
        key: str - the signing key as a JSON string
        expires_at: datetime - when the key expires
    """

    key_id: str
    endpoint: str
    key: str
    expires_at: datetime

    @staticmethod
    def from_grpc_response(  # type: ignore[misc]
        grpc_create_signing_key_response: Any, endpoint: str
    ) -> CreateSigningKeyResponse:
        key_id: str = json.loads(grpc_create_signing_key_response.key)["kid"]
        key: str = grpc_create_signing_key_response.key
        expires_at: datetime = datetime.fromtimestamp(grpc_create_signing_key_response.expires_at)
        return CreateSigningKeyResponse(key_id, endpoint, key, expires_at)


class RevokeSigningKeyResponse(ControlResponse):
    ...


@dataclass
class SigningKey:
    """Signing keys returned from requesting list signing keys.

    Args:
        key_id: str - the ID of the signing key
        expires_at: datetime - when the key expires
        endpoint: str - endpoint of the signing key
    """

    key_id: str
    expires_at: datetime
    endpoint: str

    @staticmethod
    def from_grpc_response(grpc_listed_signing_key: Any, endpoint: str) -> SigningKey:  # type: ignore[misc]
        key_id: str = grpc_listed_signing_key.key_id
        expires_at: datetime = datetime.fromtimestamp(grpc_listed_signing_key.expires_at)
        return SigningKey(key_id, expires_at, endpoint)


@dataclass
class ListSigningKeysResponse(ControlResponse):
    """A list signing keys response.

    Responses are paginated.

    Args:
        next_token: Optional[str] - the token to get the next page
        signing_keys: list[SigningKey] - all signing keys in this page
    """

    next_token: Optional[str]
    signing_keys: list[SigningKey]

    @staticmethod
    def from_grpc_response(  # type:ignore[misc]
        grpc_list_signing_keys_response: google.protobuf.message.Message, endpoint: str
    ) -> ListSigningKeysResponse:
        """Creates a ListSigningKeysResponse from a grpc response.

        Args:
            grpc_list_signing_keys_response: google.protobuf.message.Message
        """
        print(f"Name: {grpc_list_signing_keys_response.__class__.__bases__}")
        next_token: Optional[str] = (
            grpc_list_signing_keys_response.next_token if grpc_list_signing_keys_response.next_token != "" else None
        )
        signing_keys: list[SigningKey] = [
            SigningKey.from_grpc_response(signing_key, endpoint)
            for signing_key in grpc_list_signing_keys_response.signing_key
        ]
        return ListSigningKeysResponse(next_token, signing_keys)

from __future__ import annotations

import json
from abc import ABC
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

import google

from momento.responses.response import ControlResponse

from ..mixins import ErrorResponseMixin


class CreateSigningKeyResponse(ControlResponse):
    """Parent response type for a cache `create_signing_key` request. Its subtypes are:

    - `CreateSigningKey.Success`
    - `CreateSigningKey.Error`

    See `SimpleCacheClient` for how to work with responses.
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
        """Contains information about an error returned from a request:

        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """


class RevokeSigningKeyResponse(ControlResponse):
    """Parent response type for a cache `revoke_signing_key` request. Its subtypes are:

    - `RevokeSigningKey.Success`
    - `RevokeSigningKey.Error`

    See `SimpleCacheClient` for how to work with responses.
    """


class RevokeSigningKey(ABC):
    """Groups all `RevokeSigningKeyResponse` derived types under a common namespace."""

    @dataclass
    class Success(RevokeSigningKeyResponse):
        """The response from revoking a signing key."""

    class Error(RevokeSigningKeyResponse, ErrorResponseMixin):
        """Contains information about an error returned from a request:

        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """


@dataclass
class SigningKey:
    """Signing keys returned from requesting list signing keys."""

    key_id: str
    """the ID of the signing key"""
    expires_at: datetime
    """when the key expires"""
    endpoint: str
    """endpoint of the signing key"""

    @staticmethod
    def from_grpc_response(grpc_listed_signing_key: Any, endpoint: str) -> SigningKey:  # type: ignore[misc]
        key_id: str = grpc_listed_signing_key.key_id
        expires_at: datetime = datetime.fromtimestamp(grpc_listed_signing_key.expires_at)
        return SigningKey(key_id, expires_at, endpoint)


class ListSigningKeysResponse(ControlResponse):
    """Parent response type for a cache `list_signing_key` request. Its subtypes are:

    - `ListSigningKeys.Success`
    - `ListSigningKeys.Error`

    See `SimpleCacheClient` for how to work with responses.
    """


class ListSigningKeys(ABC):
    """Groups all `ListSigningKeysResponse` derived types under a common namespace."""

    @dataclass
    class Success(ListSigningKeysResponse):
        """The response from listing signing keys."""

        next_token: Optional[str]
        """the token to get the next page"""
        signing_keys: list[SigningKey]
        """all signing keys in this page"""

        @staticmethod
        def from_grpc_response(  # type:ignore[misc]
            grpc_list_signing_keys_response: google.protobuf.message.Message, endpoint: str
        ) -> ListSigningKeys.Success:
            """Creates a ListSigningKeysResponse from a grpc response.

            Args:
                grpc_list_signing_keys_response (google.protobuf.message.Message):
                endpoint (str): _description_

            Returns:
                ListSigningKeysResponse
            """
            next_token: Optional[str] = (
                grpc_list_signing_keys_response.next_token if grpc_list_signing_keys_response.next_token != "" else None
            )
            signing_keys: list[SigningKey] = [
                SigningKey.from_grpc_response(signing_key, endpoint)
                for signing_key in grpc_list_signing_keys_response.signing_key
            ]
            return ListSigningKeys.Success(next_token, signing_keys)

    class Error(ListSigningKeysResponse, ErrorResponseMixin):
        """Contains information about an error returned from a request:

        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

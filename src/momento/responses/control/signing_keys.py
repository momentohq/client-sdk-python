import json
from datetime import datetime
from typing import Any, List, Optional


class CreateSigningKeyResponse:
    def __init__(self, key_id: str, endpoint: str, key: str, expires_at: datetime):
        """Initializes CreateSigningKeyResponse to handle create signing key response.

        Args:
            grpc_create_signing_key_response: Protobuf based response returned by Scs.
        """
        self._key_id = key_id
        self._endpoint = endpoint
        self._key = key
        self._expires_at = expires_at

    def key_id(self) -> str:
        """Returns the id of the signing key"""
        return self._key_id

    def endpoint(self) -> str:
        """Returns the endpoint of the signing key"""
        return self._endpoint

    def key(self) -> str:
        """Returns the JSON string of the key itself"""
        return self._key

    def expires_at(self) -> datetime:
        """Returns the datetime representation of when the key expires"""
        return self._expires_at

    @staticmethod
    def from_grpc_response(  # type: ignore[misc]
        grpc_create_signing_key_response: Any, endpoint: str
    ) -> "CreateSigningKeyResponse":
        key_id: str = json.loads(grpc_create_signing_key_response.key)["kid"]
        key: str = grpc_create_signing_key_response.key
        expires_at: datetime = datetime.fromtimestamp(grpc_create_signing_key_response.expires_at)
        return CreateSigningKeyResponse(key_id, endpoint, key, expires_at)

    def __str__(self) -> str:
        return self.__repr__()

    def __repr__(self) -> str:
        return (
            f"CreateSigningKeyResponse(key_id={self._key_id!r}, endpoint={self._endpoint!r}, "
            f"key={self._key!r}, expires_at={self._expires_at!r})"
        )


class RevokeSigningKeyResponse:
    def __init__(self) -> None:
        pass

    def __str__(self) -> str:
        return self.__repr__()

    def __repr__(self) -> str:
        return "RevokeSigningKeyResponse()"


class SigningKey:
    def __init__(self, key_id: str, expires_at: datetime, endpoint: str):
        """Initializes SigningKey to handle signing keys returned from list signing keys operation.

        Args:
            grpc_listed_signing_key: Protobuf based response returned by Scs.
        """
        self._key_id = key_id
        self._expires_at = expires_at
        self._endpoint = endpoint

    def key_id(self) -> str:
        """Returns the id of the Momento signing key"""
        return self._key_id

    def expires_at(self) -> datetime:
        """Returns the time the key expires"""
        return self._expires_at

    def endpoint(self) -> str:
        """Returns the endpoint of the Momento signing key"""
        return self._endpoint

    @staticmethod
    def from_grpc_response(grpc_listed_signing_key: Any, endpoint: str) -> "SigningKey":  # type: ignore[misc]
        key_id: str = grpc_listed_signing_key.key_id
        expires_at: datetime = datetime.fromtimestamp(grpc_listed_signing_key.expires_at)
        return SigningKey(key_id, expires_at, endpoint)

    def __str__(self) -> str:
        return self.__repr__()

    def __repr__(self) -> str:
        return f"SigningKey(key_id={self._key_id!r}, expires_at={self._expires_at!r}, endpoint={self._endpoint!r})"


class ListSigningKeysResponse:
    def __init__(self, next_token: Optional[str], signing_keys: List[SigningKey]):
        """Initializes ListSigningKeysResponse to handle list signing keys response.

        Args:
            grpc_list_signing_keys_response: Protobuf based response returned by Scs.
        """
        self._next_token = next_token
        self._signing_keys = signing_keys

    def next_token(self) -> Optional[str]:
        """Returns next token."""
        return self._next_token

    def signing_keys(self) -> List[SigningKey]:
        """Returns all signing keys."""
        return self._signing_keys

    @staticmethod
    def from_grpc_response(  # type: ignore[misc]
        grpc_list_signing_keys_response: Any, endpoint: str
    ) -> "ListSigningKeysResponse":
        next_token: Optional[str] = (
            grpc_list_signing_keys_response.next_token if grpc_list_signing_keys_response.next_token != "" else None
        )
        signing_keys: List[SigningKey] = [
            SigningKey.from_grpc_response(signing_key, endpoint)
            for signing_key in grpc_list_signing_keys_response.signing_key
        ]
        return ListSigningKeysResponse(next_token, signing_keys)

    def __str__(self) -> str:
        return self.__repr__()

    def __repr__(self) -> str:
        return f"ListSigningKeysResponse(next_token={self._next_token!r}, signing_keys={self._signing_keys!r})"

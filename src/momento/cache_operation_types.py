import json
from datetime import datetime
from enum import Enum
from typing import cast, Any, Optional, List, Union
from dataclasses import dataclass

from momento_wire_types import cacheclient_pb2 as cache_client_types
from . import _cache_service_errors_converter as error_converter
from . import _momento_logger


class CacheGetStatus(Enum):
    HIT = 1
    MISS = 2


class CacheSetResponse:
    def __init__(self, grpc_set_response: Any, key: bytes, value: bytes):  # type: ignore[misc]
        """Initializes CacheSetResponse to handle gRPC set response.

        Args:
            grpc_set_response: Protobuf based response returned by Scs.
            key (bytes): The value of the key of item that was stored in cache..
            value (bytes): The value of item that was stored in the cache.

        Raises:
            InternalServerError: If server encountered an unknown error while trying to store the item.
        """
        self._value = value
        self._key = key

    def value(self) -> str:
        """Decodes string value set in cache to a utf-8 string."""
        return self._value.decode("utf-8")

    def value_as_bytes(self) -> bytes:
        """Returns byte value set in cache."""
        return self._value

    def key(self) -> str:
        """Decodes key of item set in cache to a utf-8 string."""
        return self._key.decode("utf-8")

    def key_as_bytes(self) -> bytes:
        """Returns key of item stored in cache as bytes."""
        return self._key


@dataclass
class CacheMultiSetFailureResponse:
    key: bytes
    value: bytes
    ttl_seconds: int
    failure: Exception


@dataclass
class CacheMultiGetFailureResponse:
    key: bytes
    failure: Exception


class CacheMultiSetResponse:
    def __init__(
        self,
        successful_responses: List[CacheSetResponse],
        failed_responses: List[CacheMultiSetFailureResponse],
    ):
        self._success_responses = successful_responses
        self._failed_responses = failed_responses

    def get_successful_responses(self) -> List[CacheSetResponse]:
        """Returns list of responses of items successfully stored in cache"""
        return self._success_responses

    def get_failed_responses(self) -> List[CacheMultiSetFailureResponse]:
        """Returns list of set responses of items that an error occurred while trying to store in cache"""
        return self._failed_responses


@dataclass
class CacheMultiSetOperation:
    key: Union[str, bytes]
    value: Union[str, bytes]
    ttl_seconds: Optional[int] = None


@dataclass
class CacheMultiGetOperation:
    key: Union[str, bytes]


class CacheGetResponse:
    def __init__(self, grpc_get_response: Any):  # type: ignore[misc]
        """Initializes CacheGetResponse to handle gRPC get response.

        Args:
            grpc_get_response: Protobuf based response returned by Scs.

        Raises:
            InternalServerError: If server encountered an unknown error while trying to retrieve the item.
        """
        self._value: bytes = grpc_get_response.cache_body  # type: ignore[misc]

        if grpc_get_response.result == cache_client_types.Hit:  # type: ignore[misc]
            self._result = CacheGetStatus.HIT
        elif grpc_get_response.result == cache_client_types.Miss:  # type: ignore[misc]
            self._result = CacheGetStatus.MISS
        else:
            _momento_logger.debug(
                f"Get received unsupported ECacheResult: {grpc_get_response.result}"  # type: ignore[misc]
            )
            raise error_converter.convert_ecache_result(
                grpc_get_response.result, grpc_get_response.message, "GET"  # type: ignore[misc]
            )

    def value(self) -> Optional[str]:
        """Returns value stored in cache as utf-8 string if there was Hit. Returns None otherwise."""
        if self._result == CacheGetStatus.HIT:
            return self._value.decode("utf-8")
        return None

    def value_as_bytes(self) -> Optional[bytes]:
        """Returns value stored in cache as bytes if there was Hit. Returns None otherwise."""
        if self._result == CacheGetStatus.HIT:
            return self._value
        return None

    def status(self) -> CacheGetStatus:
        """Returns get operation result such as HIT or MISS."""
        return self._result


class CacheMultiGetResponse:
    def __init__(
        self,
        successful_responses: List[CacheGetResponse],
        failed_responses: List[CacheMultiGetFailureResponse],
    ):
        self._success_responses = successful_responses
        self._failed_responses = failed_responses

    def get_successful_responses(self) -> List[CacheGetResponse]:
        """Returns list of responses of items successfully fetched from cache"""
        return self._success_responses

    def get_failed_responses(self) -> List[CacheMultiGetFailureResponse]:
        """Returns list of set responses of items that an error occurred while trying to store in cache"""
        return self._failed_responses

    def values(self) -> List[Optional[str]]:
        """Returns list of values as utf-8 string for each Hit. Each item in list is None if was a Miss."""
        r_values = []
        for r in self._success_responses:
            r_values.append(r.value())
        return r_values

    def values_as_bytes(self) -> List[Optional[bytes]]:
        """Returns list of values as bytes for each Hit. Each item in list is None if was a Miss."""
        r_values = []
        for r in self._success_responses:
            r_values.append(r.value_as_bytes())
        return r_values


class CacheDeleteResponse:
    def __init__(self, grpc_create_cache_response: Any):  # type: ignore[misc]
        pass


class CreateCacheResponse:
    def __init__(self, grpc_create_cache_response: Any):  # type: ignore[misc]
        pass


class DeleteCacheResponse:
    def __init__(self, grpc_delete_cache_response: Any):  # type: ignore[misc]
        pass


class CacheInfo:
    def __init__(self, grpc_listed_cache: Any):  # type: ignore[misc]
        """Initializes CacheInfo to handle caches returned from list cache operation.

        Args:
            grpc_listed_cache: Protobuf based response returned by Scs.
        """
        self._name: str = grpc_listed_cache.cache_name  # type: ignore[misc]

    def name(self) -> str:
        """Returns all cache's name."""
        return self._name


class ListCachesResponse:
    def __init__(self, grpc_list_cache_response: Any):  # type: ignore[misc]
        """Initializes ListCacheResponse to handle list cache response.

        Args:
            grpc_list_cache_response: Protobuf based response returned by Scs.
        """
        self._next_token: Optional[str] = (
            grpc_list_cache_response.next_token  # type: ignore[misc]
            if grpc_list_cache_response.next_token != ""  # type: ignore[misc]
            else None
        )
        self._caches = []
        for cache in grpc_list_cache_response.cache:  # type: ignore[misc]
            self._caches.append(CacheInfo(cache))  # type: ignore[misc]

    def next_token(self) -> Optional[str]:
        """Returns next token."""
        return self._next_token

    def caches(self) -> List[CacheInfo]:
        """Returns all caches."""
        return self._caches


class CreateSigningKeyResponse:
    def __init__(self, grpc_create_signing_key_response: Any, endpoint: str):  # type: ignore[misc]
        """Initializes CreateSigningKeyResponse to handle create signing key response.

        Args:
            grpc_create_signing_key_response: Protobuf based response returned by Scs.
        """
        self._key_id: str = json.loads(grpc_create_signing_key_response.key)["kid"]  # type: ignore[misc]
        self._endpoint: str = endpoint
        self._key: str = grpc_create_signing_key_response.key  # type: ignore[misc]
        self._expires_at: datetime = datetime.fromtimestamp(
            grpc_create_signing_key_response.expires_at  # type: ignore[misc]
        )

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


class RevokeSigningKeyResponse:
    def __init__(self, grpc_revoke_signing_key_response: Any):  # type: ignore[misc]
        pass


class SigningKey:
    def __init__(self, grpc_listed_signing_key: Any, endpoint: str):  # type: ignore[misc]
        """Initializes SigningKey to handle signing keys returned from list signing keys operation.

        Args:
            grpc_listed_signing_key: Protobuf based response returned by Scs.
        """
        self._key_id: str = grpc_listed_signing_key.key_id  # type: ignore[misc]
        self._expires_at: datetime = datetime.fromtimestamp(
            grpc_listed_signing_key.expires_at  # type: ignore[misc]
        )
        self._endpoint: str = endpoint

    def key_id(self) -> str:
        """Returns the id of the Momento signing key"""
        return self._key_id

    def expires_at(self) -> datetime:
        """Returns the time the key expires"""
        return self._expires_at

    def endpoint(self) -> str:
        """Returns the endpoint of the Momento signing key"""
        return self._endpoint


class ListSigningKeysResponse:
    def __init__(self, grpc_list_signing_keys_response: Any, endpoint: str):  # type: ignore[misc]
        """Initializes ListSigningKeysResponse to handle list signing keys response.

        Args:
            grpc_list_signing_keys_response: Protobuf based response returned by Scs.
        """
        self._next_token: Optional[str] = (
            grpc_list_signing_keys_response.next_token  # type: ignore[misc]
            if grpc_list_signing_keys_response.next_token != ""  # type: ignore[misc]
            else None
        )
        self._signing_keys: List[SigningKey] = [  # type: ignore[misc]
            SigningKey(signing_key, endpoint)  # type: ignore[misc]
            for signing_key in grpc_list_signing_keys_response.signing_key  # type: ignore[misc]
        ]

    def next_token(self) -> Optional[str]:
        """Returns next token."""
        return self._next_token

    def signing_keys(self) -> List[SigningKey]:
        """Returns all signing keys."""
        return self._signing_keys

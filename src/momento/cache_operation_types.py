import json
from datetime import datetime
from enum import Enum
from typing import Any, Optional, List, Mapping

from momento_wire_types import cacheclient_pb2 as cache_client_types
from . import _cache_service_errors_converter as error_converter
from . import _momento_logger


class CacheGetStatus(Enum):
    HIT = 1
    MISS = 2


class CacheSetResponse:
    def __init__(self, key: bytes, value: bytes):
        """Initializes CacheSetResponse to handle gRPC set response.

        Args:
            key (bytes): The value of the key of item that was stored in cache..
            value (bytes): The value of item that was stored in the cache.
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

    def __str__(self) -> str:
        return self.__repr__()

    def __repr__(self) -> str:
        return f"CacheSetResponse(key={self._key!r}, value={self._value!r})"


class CacheSetMultiResponse:
    def __init__(self, items: Mapping[bytes, bytes]):
        self._items = items

    def items(self) -> Mapping[str, str]:
        return {
            key.decode("utf-8"): value.decode("utf-8")
            for key, value in self._items.items()
        }

    def items_as_bytes(self) -> Mapping[bytes, bytes]:
        return self._items

    def __str__(self) -> str:
        return self.__repr__()

    def __repr__(self) -> str:
        return f"CacheSetMultiResponse(items={self._items!r})"


class CacheGetResponse:
    def __init__(self, value: bytes, status: CacheGetStatus):
        self._value = value
        self._status = status

    @staticmethod
    def from_grpc_response(grpc_get_response: Any) -> "CacheGetResponse":  # type: ignore[misc]
        """Initializes CacheGetResponse to handle gRPC get response.

        Args:
            grpc_get_response: Protobuf based response returned by Scs.

        Raises:
            InternalServerError: If server encountered an unknown error while trying to retrieve the item.
        """
        value: bytes = grpc_get_response.cache_body  # type: ignore[misc]

        if grpc_get_response.result == cache_client_types.Hit:  # type: ignore[misc]
            status = CacheGetStatus.HIT
        elif grpc_get_response.result == cache_client_types.Miss:  # type: ignore[misc]
            status = CacheGetStatus.MISS
        else:
            _momento_logger.debug(
                f"Get received unsupported ECacheResult: {grpc_get_response.result}"  # type: ignore[misc]
            )
            raise error_converter.convert_ecache_result(
                grpc_get_response.result, grpc_get_response.message, "GET"  # type: ignore[misc]
            )
        return CacheGetResponse(value=value, status=status)

    def value(self) -> Optional[str]:
        """Returns value stored in cache as utf-8 string if there was Hit. Returns None otherwise."""
        if self._status == CacheGetStatus.HIT:
            return self._value.decode("utf-8")
        return None

    def value_as_bytes(self) -> Optional[bytes]:
        """Returns value stored in cache as bytes if there was Hit. Returns None otherwise."""
        if self._status == CacheGetStatus.HIT:
            return self._value
        return None

    def status(self) -> CacheGetStatus:
        """Returns get operation result such as HIT or MISS."""
        return self._status

    def __str__(self) -> str:
        return self.__repr__()

    def __repr__(self) -> str:
        return f"CacheGetResponse(value={self._value!r}, status={self._status!r})"


class CacheGetMultiResponse:
    def __init__(self, responses: List[CacheGetResponse]):
        self._responses = responses

    def status(self) -> List[CacheGetStatus]:
        return [response.status() for response in self._responses]

    def values(self) -> List[Optional[str]]:
        """Returns list of values as utf-8 string for each Hit. Each item in list is None if was a Miss."""
        return [response.value() for response in self._responses]

    def values_as_bytes(self) -> List[Optional[bytes]]:
        """Returns list of values as bytes for each Hit. Each item in list is None if was a Miss."""
        return [response.value_as_bytes() for response in self._responses]

    def to_list(self) -> List[CacheGetResponse]:
        return self._responses

    def __str__(self) -> str:
        return self.__repr__()

    def __repr__(self) -> str:
        return f"CacheGetMultiResponse(responses={self._responses!r})"


class CacheDeleteResponse:
    def __init__(self) -> None:
        pass

    def __str__(self) -> str:
        return self.__repr__()

    def __repr__(self) -> str:
        return f"CacheDeleteResponse()"


class CreateCacheResponse:
    def __init__(self) -> None:
        pass

    def __str__(self) -> str:
        return self.__repr__()

    def __repr__(self) -> str:
        return f"CreateCacheResponse()"


class DeleteCacheResponse:
    def __init__(self) -> None:
        pass

    def __str__(self) -> str:
        return self.__repr__()

    def __repr__(self) -> str:
        return f"DeleteCacheResponse()"


class CacheInfo:
    def __init__(self, name: str):
        """Initializes CacheInfo to handle caches returned from list cache operation.

        Args:
            name (str): Name of the cache.
        """
        self._name = name

    def name(self) -> str:
        """Returns the cache's name."""
        return self._name

    @staticmethod
    def from_grpc_response(grpc_listed_cache: Any) -> "CacheInfo":  # type: ignore[misc]
        """Initializes CacheInfo to handle caches returned from list cache operation.

        Args:
            grpc_listed_cache: Protobuf based response returned by Scs.
        """
        return CacheInfo(name=grpc_listed_cache.cache_name)  # type: ignore[misc]

    def __str__(self) -> str:
        return self.__repr__()

    def __repr__(self) -> str:
        return f"CacheInfo(name={self._name!r})"


class ListCachesResponse:
    def __init__(self, next_token: Optional[str], caches: List[CacheInfo]):
        """Initializes ListCacheResponse to handle list cache response.

        Args:
            next_token (Optional[str]): Next list caches page token.
            caches (List[CacheInfo]): Cache info from this page of results.
        """
        self._next_token = next_token
        self._caches = caches

    def next_token(self) -> Optional[str]:
        """Returns next token."""
        return self._next_token

    def caches(self) -> List[CacheInfo]:
        """Returns all caches."""
        return self._caches

    @staticmethod
    def from_grpc_response(grpc_list_cache_response: Any) -> "ListCachesResponse":  # type: ignore[misc]
        """Initializes ListCacheResponse to handle list cache response.

        Args:
            grpc_list_cache_response: Protobuf based response returned by Scs.
        """
        next_token: Optional[str] = (
            grpc_list_cache_response.next_token  # type: ignore[misc]
            if grpc_list_cache_response.next_token != ""  # type: ignore[misc]
            else None
        )
        caches = [CacheInfo.from_grpc_response(cache) for cache in grpc_list_cache_response.cache]  # type: ignore[misc]
        return ListCachesResponse(next_token=next_token, caches=caches)

    def __str__(self) -> str:
        return self.__repr__()

    def __repr__(self) -> str:
        return f"ListCachesResponse(next_token={self._next_token!r}, caches={self._caches!r})"


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
    def from_grpc_response(grpc_create_signing_key_response: Any, endpoint: str) -> "CreateSigningKeyResponse":  # type: ignore[misc]
        key_id: str = json.loads(grpc_create_signing_key_response.key)["kid"]  # type: ignore[misc]
        key: str = grpc_create_signing_key_response.key  # type: ignore[misc]
        expires_at: datetime = datetime.fromtimestamp(
            grpc_create_signing_key_response.expires_at  # type: ignore[misc]
        )
        return CreateSigningKeyResponse(key_id, endpoint, key, expires_at)

    def __str__(self) -> str:
        return self.__repr__()

    def __repr__(self) -> str:
        return f"CreateSigningKeyResponse(key_id={self._key_id!r}, endpoint={self._endpoint!r}, key={self._key!r}, expires_at={self._expires_at!r})"


class RevokeSigningKeyResponse:
    def __init__(self) -> None:
        pass

    def __str__(self) -> str:
        return self.__repr__()

    def __repr__(self) -> str:
        return f"RevokeSigningKeyResponse()"


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
        key_id: str = grpc_listed_signing_key.key_id  # type: ignore[misc]
        expires_at: datetime = datetime.fromtimestamp(
            grpc_listed_signing_key.expires_at  # type: ignore[misc]
        )
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
    def from_grpc_response(grpc_list_signing_keys_response: Any, endpoint: str) -> "ListSigningKeysResponse":  # type: ignore[misc]
        next_token: Optional[str] = (
            grpc_list_signing_keys_response.next_token  # type: ignore[misc]
            if grpc_list_signing_keys_response.next_token != ""  # type: ignore[misc]
            else None
        )
        signing_keys: List[SigningKey] = [  # type: ignore[misc]
            SigningKey.from_grpc_response(signing_key, endpoint)  # type: ignore[misc]
            for signing_key in grpc_list_signing_keys_response.signing_key  # type: ignore[misc]
        ]
        return ListSigningKeysResponse(next_token, signing_keys)

    def __str__(self) -> str:
        return self.__repr__()

    def __repr__(self) -> str:
        return f"ListSigningKeysResponse(next_token={self._next_token!r}, signing_keys={self._signing_keys!r})"

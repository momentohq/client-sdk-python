from typing import Optional

from momento_wire_types.controlclient_pb2 import _CreateCacheRequest
from momento_wire_types.controlclient_pb2 import _DeleteCacheRequest
from momento_wire_types.controlclient_pb2 import _ListCachesRequest
from momento_wire_types.controlclient_pb2 import _CreateSigningKeyRequest
from momento_wire_types.controlclient_pb2 import _RevokeSigningKeyRequest
from momento_wire_types.controlclient_pb2 import _ListSigningKeysRequest

from .._utilities._data_validation import _validate_cache_name
from .._utilities._data_validation import _validate_ttl_minutes
from ..cache_operation_types import (
    CreateCacheResponse,
    ListSigningKeysResponse,
    RevokeSigningKeyResponse,
)
from ..cache_operation_types import DeleteCacheResponse
from ..cache_operation_types import ListCachesResponse
from ..cache_operation_types import CreateSigningKeyResponse

from .. import _cache_service_errors_converter
from .. import _momento_logger
from . import _scs_grpc_manager

_DEADLINE_SECONDS = 60.0  # 1 minute


class _ScsControlClient:
    """Momento Internal."""

    def __init__(self, auth_token: str, endpoint: str):
        self._grpc_manager = _scs_grpc_manager._ControlGrpcManager(auth_token, endpoint)

    async def create_cache(self, cache_name: str) -> CreateCacheResponse:
        _validate_cache_name(cache_name)
        try:
            _momento_logger.debug(f"Creating cache with name: {cache_name}")
            request = _CreateCacheRequest()
            request.cache_name = cache_name
            await self._grpc_manager.async_stub().CreateCache(
                request, timeout=_DEADLINE_SECONDS
            )
            return CreateCacheResponse()
        except Exception as e:
            _momento_logger.debug(
                f"Failed to create cache: {cache_name} with exception:{e}"
            )
            raise _cache_service_errors_converter.convert(e) from None

    async def delete_cache(self, cache_name: str) -> DeleteCacheResponse:
        _validate_cache_name(cache_name)
        try:
            _momento_logger.debug(f"Deleting cache with name: {cache_name}")
            request = _DeleteCacheRequest()
            request.cache_name = cache_name
            await self._grpc_manager.async_stub().DeleteCache(
                request, timeout=_DEADLINE_SECONDS
            )
            return DeleteCacheResponse()
        except Exception as e:
            _momento_logger.debug(
                f"Failed to delete cache: {cache_name} with exception:{e}"
            )
            raise _cache_service_errors_converter.convert(e) from None

    async def list_caches(self, next_token: Optional[str] = None) -> ListCachesResponse:
        try:
            list_caches_request = _ListCachesRequest()
            list_caches_request.next_token = (
                next_token if next_token is not None else ""
            )
            return ListCachesResponse.from_grpc_response(
                await self._grpc_manager.async_stub().ListCaches(
                    list_caches_request, timeout=_DEADLINE_SECONDS
                )
            )
        except Exception as e:
            raise _cache_service_errors_converter.convert(e)

    async def create_signing_key(
        self, ttl_minutes: int, endpoint: str
    ) -> CreateSigningKeyResponse:
        _validate_ttl_minutes(ttl_minutes)
        try:
            _momento_logger.debug(
                f"Creating signing key with ttl (in minutes): {ttl_minutes}"
            )
            create_signing_key_request = _CreateSigningKeyRequest()
            create_signing_key_request.ttl_minutes = ttl_minutes
            return CreateSigningKeyResponse.from_grpc_response(
                await self._grpc_manager.async_stub().CreateSigningKey(
                    create_signing_key_request, timeout=_DEADLINE_SECONDS
                ),
                endpoint,
            )
        except Exception as e:
            _momento_logger.debug(f"Failed to create signing key with exception: {e}")
            raise _cache_service_errors_converter.convert(e)

    async def revoke_signing_key(self, key_id: str) -> RevokeSigningKeyResponse:
        try:
            _momento_logger.debug(f"Revoking signing key with key_id {key_id}")
            request = _RevokeSigningKeyRequest()
            request.key_id = key_id
            await self._grpc_manager.async_stub().RevokeSigningKey(
                request, timeout=_DEADLINE_SECONDS
            )
            return RevokeSigningKeyResponse()
        except Exception as e:
            _momento_logger.debug(
                f"Failed to revoke signing key with key_id {key_id} exception: {e}"
            )
            raise _cache_service_errors_converter.convert(e)

    async def list_signing_keys(
        self, endpoint: str, next_token: Optional[str] = None
    ) -> ListSigningKeysResponse:
        try:
            list_signing_keys_request = _ListSigningKeysRequest()
            list_signing_keys_request.next_token = (
                next_token if next_token is not None else ""
            )
            return ListSigningKeysResponse.from_grpc_response(
                await self._grpc_manager.async_stub().ListSigningKeys(
                    list_signing_keys_request, timeout=_DEADLINE_SECONDS
                ),
                endpoint,
            )
        except Exception as e:
            raise _cache_service_errors_converter.convert(e)

    async def close(self) -> None:
        await self._grpc_manager.close()

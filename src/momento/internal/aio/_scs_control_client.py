from typing import Optional

import grpc
from momento_wire_types.controlclient_pb2 import (
    _CreateCacheRequest,
    _CreateSigningKeyRequest,
    _DeleteCacheRequest,
    _ListCachesRequest,
    _ListSigningKeysRequest,
    _RevokeSigningKeyRequest,
)
from momento_wire_types.controlclient_pb2_grpc import ScsControlStub

from momento import logs
from momento._utilities._data_validation import (
    _validate_cache_name,
    _validate_ttl_minutes,
)
from momento.cache_operation_types import (
    CreateSigningKeyResponse,
    ListSigningKeysResponse,
    RevokeSigningKeyResponse,
)
from momento.errors import cache_service_errors_converter, new_convert
from momento.internal.aio._scs_grpc_manager import _ControlGrpcManager
from momento.responses import (
    CreateCacheResponse,
    CreateCacheResponseBase,
    DeleteCacheResponse,
    DeleteCacheResponseBase,
    ListCachesResponse,
    ListCachesResponseBase,
)

_DEADLINE_SECONDS = 60.0  # 1 minute


class _ScsControlClient:
    """Momento Internal."""

    def __init__(self, auth_token: str, endpoint: str):
        self._logger = logs.logger
        self._logger.debug("Simple cache control client instantiated with endpoint: %s", endpoint)
        self._grpc_manager = _ControlGrpcManager(auth_token, endpoint)
        self._endpoint = endpoint

    @property
    def endpoint(self) -> str:
        return self._endpoint

    async def create_cache(self, cache_name: str) -> CreateCacheResponseBase:
        try:
            self._logger.info(f"Creating cache with name: {cache_name}")
            _validate_cache_name(cache_name)
            request = _CreateCacheRequest()
            request.cache_name = cache_name
            await self._getStub().CreateCache(request, timeout=_DEADLINE_SECONDS)
        except Exception as e:
            self._logger.debug("Failed to create cache: %s with exception: %s", cache_name, e)
            if isinstance(e, grpc.RpcError) and e.code() == grpc.StatusCode.ALREADY_EXISTS:
                return CreateCacheResponse.CacheAlreadyExists()
            return CreateCacheResponse.Error(new_convert(e))
        return CreateCacheResponse.Success()

    async def delete_cache(self, cache_name: str) -> DeleteCacheResponseBase:
        try:
            self._logger.info(f"Deleting cache with name: {cache_name}")
            _validate_cache_name(cache_name)
            request = _DeleteCacheRequest()
            request.cache_name = cache_name
            await self._getStub().DeleteCache(request, timeout=_DEADLINE_SECONDS)
        except Exception as e:
            self._logger.debug("Failed to delete cache: %s with exception: %s", cache_name, e)
            return DeleteCacheResponse.Error(new_convert(e))
        return DeleteCacheResponse.Success()

    async def list_caches(self, next_token: Optional[str] = None) -> ListCachesResponseBase:
        try:
            list_caches_request = _ListCachesRequest()
            list_caches_request.next_token = next_token if next_token is not None else ""
            response = await self._getStub().ListCaches(list_caches_request, timeout=_DEADLINE_SECONDS)
            return ListCachesResponse.Success.from_grpc_response(response)
        except Exception as e:
            return ListCachesResponse.Error(new_convert(e))

    async def create_signing_key(self, ttl_minutes: int, endpoint: str) -> CreateSigningKeyResponse:
        _validate_ttl_minutes(ttl_minutes)
        try:
            self._logger.info(f"Creating signing key with ttl (in minutes): {ttl_minutes}")
            create_signing_key_request = _CreateSigningKeyRequest()
            create_signing_key_request.ttl_minutes = ttl_minutes
            return CreateSigningKeyResponse.from_grpc_response(
                await self._getStub().CreateSigningKey(create_signing_key_request, timeout=_DEADLINE_SECONDS),
                endpoint,
            )
        except Exception as e:
            self._logger.warning(f"Failed to create signing key with exception: {e}")
            raise cache_service_errors_converter.convert(e)

    async def revoke_signing_key(self, key_id: str) -> RevokeSigningKeyResponse:
        try:
            self._logger.info(f"Revoking signing key with key_id {key_id}")
            request = _RevokeSigningKeyRequest()
            request.key_id = key_id
            await self._getStub().RevokeSigningKey(request, timeout=_DEADLINE_SECONDS)
            return RevokeSigningKeyResponse()
        except Exception as e:
            self._logger.warning(f"Failed to revoke signing key with key_id {key_id} exception: {e}")
            raise cache_service_errors_converter.convert(e)

    async def list_signing_keys(self, endpoint: str, next_token: Optional[str] = None) -> ListSigningKeysResponse:
        try:
            list_signing_keys_request = _ListSigningKeysRequest()
            list_signing_keys_request.next_token = next_token if next_token is not None else ""
            return ListSigningKeysResponse.from_grpc_response(
                await self._getStub().ListSigningKeys(list_signing_keys_request, timeout=_DEADLINE_SECONDS),
                endpoint,
            )
        except Exception as e:
            raise cache_service_errors_converter.convert(e)

    def _getStub(self) -> ScsControlStub:
        return self._grpc_manager.async_stub()

    async def close(self) -> None:
        await self._grpc_manager.close()
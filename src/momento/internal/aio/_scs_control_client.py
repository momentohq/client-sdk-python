from datetime import timedelta
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
from momento.auth import CredentialProvider
from momento.errors import convert_error
from momento.internal._utilities import _validate_cache_name, _validate_ttl
from momento.internal.aio._scs_grpc_manager import _ControlGrpcManager
from momento.responses import (
    CreateCache,
    CreateCacheResponse,
    CreateSigningKeyResponse,
    DeleteCache,
    DeleteCacheResponse,
    ListCaches,
    ListCachesResponse,
    ListSigningKeysResponse,
    RevokeSigningKeyResponse,
)

_DEADLINE_SECONDS = 60.0  # 1 minute


class _ScsControlClient:
    """Momento Internal."""

    def __init__(self, credential_provider: CredentialProvider):
        endpoint = credential_provider.control_endpoint
        self._logger = logs.logger
        self._logger.debug("Simple cache control client instantiated with endpoint: %s", endpoint)
        self._grpc_manager = _ControlGrpcManager(credential_provider)
        self._endpoint = endpoint

    @property
    def endpoint(self) -> str:
        return self._endpoint

    async def create_cache(self, cache_name: str) -> CreateCacheResponse:
        try:
            self._logger.info(f"Creating cache with name: {cache_name}")
            _validate_cache_name(cache_name)
            request = _CreateCacheRequest()
            request.cache_name = cache_name
            await self._build_stub().CreateCache(request, timeout=_DEADLINE_SECONDS)
        except Exception as e:
            self._logger.debug("Failed to create cache: %s with exception: %s", cache_name, e)
            if isinstance(e, grpc.RpcError) and e.code() == grpc.StatusCode.ALREADY_EXISTS:
                return CreateCache.CacheAlreadyExists()
            return CreateCache.Error(convert_error(e))
        return CreateCache.Success()

    async def delete_cache(self, cache_name: str) -> DeleteCacheResponse:
        try:
            self._logger.info(f"Deleting cache with name: {cache_name}")
            _validate_cache_name(cache_name)
            request = _DeleteCacheRequest()
            request.cache_name = cache_name
            await self._build_stub().DeleteCache(request, timeout=_DEADLINE_SECONDS)
        except Exception as e:
            self._logger.debug("Failed to delete cache: %s with exception: %s", cache_name, e)
            return DeleteCache.Error(convert_error(e))
        return DeleteCache.Success()

    async def list_caches(self, next_token: Optional[str] = None) -> ListCachesResponse:
        try:
            list_caches_request = _ListCachesRequest()
            list_caches_request.next_token = next_token if next_token is not None else ""
            response = await self._build_stub().ListCaches(list_caches_request, timeout=_DEADLINE_SECONDS)
            return ListCaches.Success.from_grpc_response(response)
        except Exception as e:
            return ListCaches.Error(convert_error(e))

    async def create_signing_key(self, ttl: timedelta, endpoint: str) -> CreateSigningKeyResponse:
        try:
            _validate_ttl(ttl)
            ttl_minutes = round(ttl.total_seconds() / 60)
            self._logger.info(f"Creating signing key with ttl (in minutes): {ttl_minutes}")
            create_signing_key_request = _CreateSigningKeyRequest()
            create_signing_key_request.ttl_minutes = ttl_minutes
            return CreateSigningKeyResponse.from_grpc_response(
                await self._build_stub().CreateSigningKey(create_signing_key_request, timeout=_DEADLINE_SECONDS),
                endpoint,
            )
        except Exception as e:
            self._logger.warning(f"Failed to create signing key with exception: {e}")
            raise convert_error(e)

    async def revoke_signing_key(self, key_id: str) -> RevokeSigningKeyResponse:
        try:
            self._logger.info(f"Revoking signing key with key_id {key_id}")
            request = _RevokeSigningKeyRequest()
            request.key_id = key_id
            await self._build_stub().RevokeSigningKey(request, timeout=_DEADLINE_SECONDS)
            return RevokeSigningKeyResponse()
        except Exception as e:
            self._logger.warning(f"Failed to revoke signing key with key_id {key_id} exception: {e}")
            raise convert_error(e)

    async def list_signing_keys(self, endpoint: str, next_token: Optional[str] = None) -> ListSigningKeysResponse:
        try:
            list_signing_keys_request = _ListSigningKeysRequest()
            list_signing_keys_request.next_token = next_token if next_token is not None else ""
            return ListSigningKeysResponse.from_grpc_response(
                await self._build_stub().ListSigningKeys(list_signing_keys_request, timeout=_DEADLINE_SECONDS),
                endpoint,
            )
        except Exception as e:
            raise convert_error(e)

    def _build_stub(self) -> ScsControlStub:
        return self._grpc_manager.async_stub()

    async def close(self) -> None:
        await self._grpc_manager.close()

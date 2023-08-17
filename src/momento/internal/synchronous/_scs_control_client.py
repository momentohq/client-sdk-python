from datetime import timedelta

import grpc
from momento_wire_types import controlclient_pb2 as ctrl_pb
from momento_wire_types import controlclient_pb2_grpc as ctrl_grpc

from momento import logs
from momento.auth import CredentialProvider
from momento.config import Configuration
from momento.errors import convert_error
from momento.internal._utilities import _validate_cache_name, _validate_ttl
from momento.internal.synchronous._scs_grpc_manager import _ControlGrpcManager
from momento.responses import (
    CacheFlush,
    CacheFlushResponse,
    CreateCache,
    CreateCacheResponse,
    CreateSigningKey,
    CreateSigningKeyResponse,
    DeleteCache,
    DeleteCacheResponse,
    ListCaches,
    ListCachesResponse,
    ListSigningKeys,
    ListSigningKeysResponse,
    RevokeSigningKey,
    RevokeSigningKeyResponse,
)

_DEADLINE_SECONDS = 60.0  # 1 minute


class _ScsControlClient:
    """Momento Internal."""

    def __init__(self, configuration: Configuration, credential_provider: CredentialProvider):
        endpoint = credential_provider.control_endpoint
        self._logger = logs.logger
        self._logger.debug("Simple cache control client instantiated with endpoint: %s", endpoint)
        self._grpc_manager = _ControlGrpcManager(configuration, credential_provider)
        self._endpoint = endpoint

    @property
    def endpoint(self) -> str:
        return self._endpoint

    def create_cache(self, cache_name: str) -> CreateCacheResponse:
        try:
            self._logger.info(f"Creating cache with name: {cache_name}")
            _validate_cache_name(cache_name)
            request = ctrl_pb._CreateCacheRequest(cache_name=cache_name)
            self._build_stub().CreateCache(request, timeout=_DEADLINE_SECONDS)
        except Exception as e:
            self._logger.debug("Failed to create cache: %s with exception: %s", cache_name, e)
            if isinstance(e, grpc.RpcError) and e.code() == grpc.StatusCode.ALREADY_EXISTS:
                return CreateCache.CacheAlreadyExists()
            return CreateCache.Error(convert_error(e))
        return CreateCache.Success()

    def delete_cache(self, cache_name: str) -> DeleteCacheResponse:
        try:
            self._logger.info(f"Deleting cache with name: {cache_name}")
            _validate_cache_name(cache_name)
            request = ctrl_pb._DeleteCacheRequest(cache_name=cache_name)
            self._build_stub().DeleteCache(request, timeout=_DEADLINE_SECONDS)
        except Exception as e:
            self._logger.debug("Failed to delete cache: %s with exception: %s", cache_name, e)
            return DeleteCache.Error(convert_error(e))
        return DeleteCache.Success()

    def list_caches(self) -> ListCachesResponse:
        try:
            list_caches_request = ctrl_pb._ListCachesRequest(next_token="")
            response = self._build_stub().ListCaches(list_caches_request, timeout=_DEADLINE_SECONDS)
            return ListCaches.Success.from_grpc_response(response)
        except Exception as e:
            return ListCaches.Error(convert_error(e))

    def flush(self, cache_name: str) -> CacheFlushResponse:
        try:
            _validate_cache_name(cache_name)
            request = ctrl_pb._FlushCacheRequest(cache_name=cache_name)
            self._build_stub().FlushCache(request, timeout=_DEADLINE_SECONDS)
            return CacheFlush.Success()
        except Exception as e:
            return CacheFlush.Error(convert_error(e))

    def create_signing_key(self, ttl: timedelta, endpoint: str) -> CreateSigningKeyResponse:
        try:
            self._logger.info(f"Creating signing key with ttl={ttl!r} and endpoint={endpoint!r}")
            _validate_ttl(ttl)
            ttl_minutes = round(ttl.total_seconds() / 60)
            self._logger.info(f"Creating signing key with ttl (in minutes): {ttl_minutes}")
            create_signing_key_request = ctrl_pb._CreateSigningKeyRequest(ttl_minutes=ttl_minutes)
            response = self._build_stub().CreateSigningKey(create_signing_key_request, timeout=_DEADLINE_SECONDS)
            return CreateSigningKey.Success.from_grpc_response(response, endpoint)
        except Exception as e:
            self._logger.warning(f"Failed to create signing key with exception: {e}")
            return CreateSigningKey.Error(convert_error(e))

    def revoke_signing_key(self, key_id: str) -> RevokeSigningKeyResponse:
        try:
            self._logger.info(f"Revoking signing key with key_id {key_id}")
            request = ctrl_pb._RevokeSigningKeyRequest(key_id=key_id)
            self._build_stub().RevokeSigningKey(request, timeout=_DEADLINE_SECONDS)
            return RevokeSigningKey.Success()
        except Exception as e:
            self._logger.warning(f"Failed to revoke signing key with key_id {key_id} exception: {e}")
            return RevokeSigningKey.Error(convert_error(e))

    def list_signing_keys(self, endpoint: str) -> ListSigningKeysResponse:
        try:
            self._logger.info("List signing keys")
            list_signing_keys_request = ctrl_pb._ListSigningKeysRequest(next_token="")
            response = self._build_stub().ListSigningKeys(list_signing_keys_request, timeout=_DEADLINE_SECONDS)
            return ListSigningKeys.Success.from_grpc_response(
                response,
                endpoint,
            )
        except Exception as e:
            self._logger.warning(f"Failed to list signing keys with exception: {e}")
            return ListSigningKeys.Error(convert_error(e))

    def _build_stub(self) -> ctrl_grpc.ScsControlStub:
        return self._grpc_manager.stub()

    def close(self) -> None:
        self._grpc_manager.close()

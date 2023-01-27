from typing import Optional

import grpc
from momento_wire_types.controlclient_pb2 import (
    _CreateCacheRequest,
    _DeleteCacheRequest,
    _ListCachesRequest,
)
from momento_wire_types.controlclient_pb2_grpc import ScsControlStub

from momento import logs
from momento.auth import CredentialProvider
from momento.errors import convert_error
from momento.internal._utilities import _validate_cache_name
from momento.internal.synchronous._scs_grpc_manager import _ControlGrpcManager
from momento.responses import (
    CreateCache,
    CreateCacheResponse,
    DeleteCache,
    DeleteCacheResponse,
    ListCaches,
    ListCachesResponse,
)

_DEADLINE_SECONDS = 60.0  # 1 minute


class _ScsControlClient:
    """Momento Internal."""

    def __init__(self, credential_provider: CredentialProvider):
        endpoint = credential_provider.get_control_endpoint()
        self._logger = logs.logger
        self._logger.debug("Simple cache control client instantiated with endpoint: %s", endpoint)
        self._grpc_manager = _ControlGrpcManager(credential_provider)
        self._endpoint = endpoint

    @property
    def endpoint(self) -> str:
        return self._endpoint

    def create_cache(self, cache_name: str) -> CreateCacheResponse:
        try:
            self._logger.info(f"Creating cache with name: {cache_name}")
            _validate_cache_name(cache_name)
            request = _CreateCacheRequest()
            request.cache_name = cache_name
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
            request = _DeleteCacheRequest()
            request.cache_name = cache_name
            self._build_stub().DeleteCache(request, timeout=_DEADLINE_SECONDS)
        except Exception as e:
            self._logger.debug("Failed to delete cache: %s with exception: %s", cache_name, e)
            return DeleteCache.Error(convert_error(e))
        return DeleteCache.Success()

    def list_caches(self, next_token: Optional[str] = None) -> ListCachesResponse:
        try:
            list_caches_request = _ListCachesRequest()
            list_caches_request.next_token = next_token if next_token is not None else ""
            response = self._build_stub().ListCaches(list_caches_request, timeout=_DEADLINE_SECONDS)
            return ListCaches.Success.from_grpc_response(response)
        except Exception as e:
            return ListCaches.Error(convert_error(e))

    def _build_stub(self) -> ScsControlStub:
        return self._grpc_manager.stub()

    def close(self) -> None:
        self._grpc_manager.close()

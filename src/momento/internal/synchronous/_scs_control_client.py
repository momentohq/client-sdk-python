from typing import Optional

from momento_wire_types.controlclient_pb2 import (
    _CreateCacheRequest,
    _DeleteCacheRequest,
    _ListCachesRequest,
)
from momento_wire_types.controlclient_pb2_grpc import ScsControlStub

from momento import _cache_service_errors_converter, logs
from momento._utilities._data_validation import _validate_cache_name
from momento.cache_operation_types import (
    CreateCacheResponse,
    DeleteCacheResponse,
    ListCachesResponse,
)

from . import _scs_grpc_manager

_DEADLINE_SECONDS = 60.0  # 1 minute


class _ScsControlClient:
    """Momento Internal."""

    def __init__(self, auth_token: str, endpoint: str):
        self._grpc_manager = _scs_grpc_manager._ControlGrpcManager(auth_token, endpoint)

    def create_cache(self, cache_name: str) -> CreateCacheResponse:
        _validate_cache_name(cache_name)
        try:
            logs.debug(f"Creating cache with name: {cache_name}")
            request = _CreateCacheRequest()
            request.cache_name = cache_name
            self._getStub().CreateCache(request, timeout=_DEADLINE_SECONDS)
            return CreateCacheResponse()
        except Exception as e:
            logs.debug(f"Failed to create cache: {cache_name} with exception:{e}")
            # raise e
            raise _cache_service_errors_converter.convert(e) from None

    def delete_cache(self, cache_name: str) -> DeleteCacheResponse:
        _validate_cache_name(cache_name)
        try:
            logs.debug(f"Deleting cache with name: {cache_name}")
            request = _DeleteCacheRequest()
            request.cache_name = cache_name
            self._getStub().DeleteCache(request, timeout=_DEADLINE_SECONDS)
            return DeleteCacheResponse()
        except Exception as e:
            logs.debug(f"Failed to delete cache: {cache_name} with exception:{e}")
            raise _cache_service_errors_converter.convert(e) from None

    def list_caches(self, next_token: Optional[str] = None) -> ListCachesResponse:
        try:
            list_caches_request = _ListCachesRequest()
            list_caches_request.next_token = next_token if next_token is not None else ""
            return ListCachesResponse.from_grpc_response(
                self._getStub().ListCaches(list_caches_request, timeout=_DEADLINE_SECONDS)
            )
        except Exception as e:
            raise _cache_service_errors_converter.convert(e)

    def _getStub(self) -> ScsControlStub:
        return self._grpc_manager.stub()

    def close(self) -> None:
        self._grpc_manager.close()

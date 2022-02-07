from typing import Optional

from momento_wire_types.controlclient_pb2 import _CreateCacheRequest
from momento_wire_types.controlclient_pb2 import _DeleteCacheRequest
from momento_wire_types.controlclient_pb2 import _ListCachesRequest
from momento_wire_types.controlclient_pb2_grpc import ScsControlStub

from .cache_operation_responses import CreateCacheResponse
from .cache_operation_responses import DeleteCacheResponse
from .cache_operation_responses import ListCachesResponse

from . import _cache_service_errors_converter
from . import _momento_logger
from . import _scs_grpc_manager

from ._utilities._data_validation import _validate_cache_name


_DEADLINE_SECONDS = 60.0  # 1 minute


class _ScsControlClient:
    """Momento Internal."""

    def __init__(self, auth_token: str, endpoint: str):
        self._grpc_manager = _scs_grpc_manager._ControlGrpcManager(auth_token, endpoint)

    def create_cache(self, cache_name: str) -> CreateCacheResponse:
        _validate_cache_name(cache_name)
        try:
            _momento_logger.debug(f"Creating cache with name: {cache_name}")
            request = _CreateCacheRequest()
            request.cache_name = cache_name
            return CreateCacheResponse(
                self._getStub().CreateCache(request, timeout=_DEADLINE_SECONDS)
            )
        except Exception as e:
            _momento_logger.debug(
                f"Failed to create cache: {cache_name} with exception:{e}"
            )
            raise _cache_service_errors_converter.convert(e) from None

    def delete_cache(self, cache_name: str) -> DeleteCacheResponse:
        _validate_cache_name(cache_name)
        try:
            _momento_logger.debug(f"Deleting cache with name: {cache_name}")
            request = _DeleteCacheRequest()
            request.cache_name = cache_name
            return DeleteCacheResponse(
                self._getStub().DeleteCache(request, timeout=_DEADLINE_SECONDS)
            )
        except Exception as e:
            _momento_logger.debug(
                f"Failed to delete cache: {cache_name} with exception:{e}"
            )
            raise _cache_service_errors_converter.convert(e) from None

    def list_caches(self, next_token: Optional[str] = None) -> ListCachesResponse:
        try:
            list_caches_request = _ListCachesRequest()
            list_caches_request.next_token = (
                next_token if next_token is not None else ""
            )
            return ListCachesResponse(
                self._getStub().ListCaches(
                    list_caches_request, timeout=_DEADLINE_SECONDS
                )
            )
        except Exception as e:
            raise _cache_service_errors_converter.convert(e)

    def _getStub(self) -> ScsControlStub:
        return self._grpc_manager.stub()

    def close(self) -> None:
        self._grpc_manager.close()

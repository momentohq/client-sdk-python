from momento_wire_types.controlclient_pb2 import CreateCacheRequest
from momento_wire_types.controlclient_pb2 import DeleteCacheRequest
from momento_wire_types.controlclient_pb2 import ListCachesRequest

from .cache_operation_responses import CreateCacheResponse
from .cache_operation_responses import DeleteCacheResponse
from .cache_operation_responses import ListCachesResponse

from . import _cache_service_errors_converter
from . import errors
from . import _momento_logger
from . import _scs_grpc_manager

from ._utilities._data_validation import _validate_cache_name


class _ScsControlClient:
    """Momento Internal."""
    def __init__(self, auth_token, endpoint):
        self._grpc_manager = _scs_grpc_manager._ControlGrpcManager(
            auth_token, endpoint)

    def create_cache(self, cache_name):
        _validate_cache_name(cache_name)
        try:
            _momento_logger.debug(f'Creating cache with name: {cache_name}')
            request = CreateCacheRequest()
            request.cache_name = cache_name
            return CreateCacheResponse(self._getStub().CreateCache(request))
        except Exception as e:
            _momento_logger.debug(
                f'Failed to create cache: {cache_name} with exception:{e}')
            raise _cache_service_errors_converter.convert(e) from None

    def delete_cache(self, cache_name):
        _validate_cache_name(cache_name)
        try:
            _momento_logger.debug(f'Deleting cache with name: {cache_name}')
            request = DeleteCacheRequest()
            request.cache_name = cache_name
            return DeleteCacheResponse(self._getStub().DeleteCache(request))
        except Exception as e:
            _momento_logger.debug(
                f'Failed to delete cache: {cache_name} with exception:{e}')
            raise _cache_service_errors_converter.convert(e) from None

    def list_caches(self, next_token=None):
        try:
            list_caches_request = ListCachesRequest()
            list_caches_request.next_token = next_token if next_token is not None else ''
            return ListCachesResponse(
                self._getStub().ListCaches(list_caches_request))
        except Exception as e:
            raise _cache_service_errors_converter.convert(e)

    def _getStub(self):
        return self._grpc_manager.stub()

    def close(self):
        self._grpc_manager.close()

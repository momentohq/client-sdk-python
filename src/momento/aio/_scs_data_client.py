from typing import Optional, Union

from momento_wire_types.cacheclient_pb2 import _DeleteRequest, _GetRequest, _SetRequest

from .. import _cache_service_errors_converter
from .. import cache_operation_types as cache_sdk_ops
from .. import logs
from .._utilities._data_validation import (
    _as_bytes,
    _make_metadata,
    _validate_cache_name,
    _validate_ttl,
)
from . import _scs_grpc_manager

_DEFAULT_DEADLINE_SECONDS = 5.0  # 5 seconds


class _ScsDataClient:
    """Internal"""

    def __init__(
        self,
        auth_token: str,
        endpoint: str,
        default_ttl_seconds: int,
        operation_timeout_ms: Optional[int],
    ):
        self._logger = logs.logger
        self._logger.debug("Simple cache data client instantiated with endpoint: %s", endpoint)
        self._default_deadline_seconds = (
            _DEFAULT_DEADLINE_SECONDS if not operation_timeout_ms else operation_timeout_ms / 1000.0
        )
        self._grpc_manager = _scs_grpc_manager._DataGrpcManager(auth_token, endpoint)
        _validate_ttl(default_ttl_seconds)
        self._default_ttlSeconds = default_ttl_seconds
        self._endpoint = endpoint

    def get_endpoint(self) -> str:
        return self._endpoint

    async def set(
        self,
        cache_name: str,
        key: Union[str, bytes],
        value: Union[str, bytes],
        ttl_seconds: Optional[int],
    ) -> cache_sdk_ops.CacheSetResponse:
        _validate_cache_name(cache_name)
        try:
            self._logger.log(logs.TRACE, "Issuing a set request with key %s", str(key))
            item_ttl_seconds = self._default_ttlSeconds if ttl_seconds is None else ttl_seconds
            _validate_ttl(item_ttl_seconds)
            set_request = _SetRequest()
            set_request.cache_key = _as_bytes(key, "Unsupported type for key: ")
            set_request.cache_body = _as_bytes(value, "Unsupported type for value: ")
            set_request.ttl_milliseconds = item_ttl_seconds * 1000
            await self._grpc_manager.async_stub().Set(
                set_request,
                metadata=_make_metadata(cache_name),
                timeout=self._default_deadline_seconds,
            )
            self._logger.log(logs.TRACE, "Set succeeded for key: %s", str(key))
            return cache_sdk_ops.CacheSetResponse(set_request.cache_key, set_request.cache_body)
        except Exception as e:
            self._logger.log(logs.TRACE, "Set failed for %s with response: %s", str(key), e)
            raise _cache_service_errors_converter.convert(e)

    async def get(self, cache_name: str, key: Union[str, bytes]) -> cache_sdk_ops.CacheGetResponse:

        _validate_cache_name(cache_name)
        try:
            self._logger.log(logs.TRACE, "Issuing a get request with key %s", str(key))
            get_request = _GetRequest()
            get_request.cache_key = _as_bytes(key, "Unsupported type for key: ")
            response = await self._grpc_manager.async_stub().Get(
                get_request,
                metadata=_make_metadata(cache_name),
                timeout=self._default_deadline_seconds,
            )
            self._logger.log(logs.TRACE, "Received a get response for %s", str(key))
            return cache_sdk_ops.CacheGetResponse.from_grpc_response(response)
        except Exception as e:
            self._logger.log(logs.TRACE, "Get failed for %s with response: %s", str(key), e)
            raise _cache_service_errors_converter.convert(e)

    async def delete(self, cache_name: str, key: Union[str, bytes]) -> cache_sdk_ops.CacheDeleteResponse:
        _validate_cache_name(cache_name)
        try:
            self._logger.log(logs.TRACE, "Issuing a delete request with key %s", str(key))
            delete_request = _DeleteRequest()
            delete_request.cache_key = _as_bytes(key, "Unsupported type for key: ")
            await self._grpc_manager.async_stub().Delete(
                delete_request,
                metadata=_make_metadata(cache_name),
                timeout=self._default_deadline_seconds,
            )
            self._logger.log(logs.TRACE, "Received a delete response for %s", str(key))
            return cache_sdk_ops.CacheDeleteResponse()
        except Exception as e:
            self._logger.debug("Delete failed for %s with response: %s", str(key), e)
            raise _cache_service_errors_converter.convert(e)

    async def close(self) -> None:
        await self._grpc_manager.close()

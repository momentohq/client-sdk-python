import asyncio
from typing import Union, Optional, Mapping
from momento_wire_types.cacheclient_pb2 import _GetRequest, _SetRequest, _DeleteRequest

from .. import cache_operation_types as cache_sdk_ops
from .. import _cache_service_errors_converter
from .. import _momento_logger
from . import _scs_grpc_manager
from .._utilities._data_validation import (
    _as_bytes,
    _validate_ttl,
    _make_metadata,
    _validate_cache_name,
)

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
        self._default_deadline_seconds = (
            _DEFAULT_DEADLINE_SECONDS
            if not operation_timeout_ms
            else operation_timeout_ms / 1000.0
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
            _momento_logger.debug(f"Issuing a set request with key {str(key)}")
            item_ttl_seconds = (
                self._default_ttlSeconds if ttl_seconds is None else ttl_seconds
            )
            _validate_ttl(item_ttl_seconds)
            set_request = _SetRequest()
            set_request.cache_key = _as_bytes(key, "Unsupported type for key: ")
            set_request.cache_body = _as_bytes(value, "Unsupported type for value: ")
            set_request.ttl_milliseconds = item_ttl_seconds * 1000
            response = await self._grpc_manager.async_stub().Set(
                set_request,
                metadata=_make_metadata(cache_name),
                timeout=self._default_deadline_seconds,
            )
            _momento_logger.debug(f"Set succeeded for key: {str(key)}")
            return cache_sdk_ops.CacheSetResponse(
                set_request.cache_key, set_request.cache_body
            )
        except Exception as e:
            _momento_logger.debug(f"Set failed for {str(key)} with response: {e}")
            raise _cache_service_errors_converter.convert(e)

    async def set_multi(
        self,
        cache_name: str,
        items: Union[Mapping[str, str], Mapping[bytes, bytes]],
        ttl_seconds: Optional[int] = None,
    ) -> cache_sdk_ops.CacheSetMultiResponse:
        _validate_cache_name(cache_name)

        try:
            request_promises = [
                self.set(cache_name, key, value, ttl_seconds)
                for key, value in items.items()
            ]

            # A note on `return_exceptions=True`: because we're gathering the results,
            # if an individual promise raises an exception, we want the others to finish gracefully.
            responses = await asyncio.gather(
                *request_promises,
                return_exceptions=True,
            )

            for response in responses:
                if isinstance(response, Exception):
                    raise response

            items_as_bytes = {
                response.key_as_bytes(): response.value_as_bytes()
                for response in responses
            }
            return cache_sdk_ops.CacheSetMultiResponse(items=items_as_bytes)
        except Exception as e:
            _momento_logger.debug(f"multi-set failed with error: {e}")
            # re-raise any error caught here is fatal error with overall handling of request objects
            raise _cache_service_errors_converter.convert(e)

    async def get(
        self, cache_name: str, key: Union[str, bytes]
    ) -> cache_sdk_ops.CacheGetResponse:

        _validate_cache_name(cache_name)
        try:
            _momento_logger.debug(f"Issuing a get request with key {str(key)}")
            get_request = _GetRequest()
            get_request.cache_key = _as_bytes(key, "Unsupported type for key: ")
            response = await self._grpc_manager.async_stub().Get(
                get_request,
                metadata=_make_metadata(cache_name),
                timeout=self._default_deadline_seconds,
            )
            _momento_logger.debug(f"Received a get response for {str(key)}")
            return cache_sdk_ops.CacheGetResponse.from_grpc_response(response)
        except Exception as e:
            _momento_logger.debug(f"Get failed for {str(key)} with response: {e}")
            raise _cache_service_errors_converter.convert(e)

    async def get_multi(
        self,
        cache_name: str,
        *keys: Union[str, bytes],
    ) -> cache_sdk_ops.CacheGetMultiResponse:
        _validate_cache_name(cache_name)
        try:
            request_promises = [self.get(cache_name, key) for key in keys]

            # A note on `return_exceptions=True`: because we're gathering the results,
            # if an individual promise raises an exception, we want the others to finish gracefully.
            responses = await asyncio.gather(
                *request_promises,
                return_exceptions=True,
            )
            for response in responses:
                if isinstance(response, Exception):
                    raise response
        except Exception as e:
            _momento_logger.debug(f"get_multi failed with response: {e}")
            raise _cache_service_errors_converter.convert(e)

        return cache_sdk_ops.CacheGetMultiResponse(responses=responses)

    async def delete(
        self, cache_name: str, key: Union[str, bytes]
    ) -> cache_sdk_ops.CacheDeleteResponse:
        _validate_cache_name(cache_name)
        try:
            _momento_logger.debug(f"Issuing a delete request with key {str(key)}")
            delete_request = _DeleteRequest()
            delete_request.cache_key = _as_bytes(key, "Unsupported type for key: ")
            response = await self._grpc_manager.async_stub().Delete(
                delete_request,
                metadata=_make_metadata(cache_name),
                timeout=self._default_deadline_seconds,
            )
            _momento_logger.debug(f"Received a delete response for {str(key)}")
            return cache_sdk_ops.CacheDeleteResponse()
        except Exception as e:
            _momento_logger.debug(f"Delete failed for {str(key)} with response: {e}")
            raise _cache_service_errors_converter.convert(e)

    async def close(self) -> None:
        await self._grpc_manager.close()

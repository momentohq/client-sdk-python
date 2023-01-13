from datetime import timedelta
from typing import Optional, Union

from grpc.aio import Metadata
from momento_wire_types.cacheclient_pb2 import (
    _DeleteRequest,
    _DeleteResponse,
    _GetRequest,
    _GetResponse,
    _SetRequest,
    _SetResponse,
)

from momento.auth.credential_provider import CredentialProvider
from momento.config.configuration import Configuration
from momento.internal.common._data_client_ops import (
    construct_delete_response,
    construct_get_response,
    construct_set_response,
    get_default_client_deadline,
    prepare_delete_request,
    prepare_get_request,
    prepare_set_request,
    wrap_async_with_error_handling,
)

from .. import cache_operation_types as cache_sdk_ops
from .. import logs
from .._utilities._data_validation import _validate_ttl
from . import _scs_grpc_manager


def _make_metadata(cache_name: str) -> Metadata:
    return Metadata(("cache", cache_name))


class _ScsDataClient:
    """Internal"""

    def __init__(self, configuration: Configuration, credential_provider: CredentialProvider, default_ttl: timedelta):
        endpoint = credential_provider.get_cache_endpoint()
        self._logger = logs.logger
        self._logger.debug("Simple cache data client instantiated with endpoint: %s", endpoint)
        default_deadline: timedelta = get_default_client_deadline(configuration)
        self._default_deadline_seconds = int(default_deadline.total_seconds())
        self._grpc_manager = _scs_grpc_manager._DataGrpcManager(credential_provider)
        _validate_ttl(default_ttl)
        self._default_ttl = default_ttl
        self._endpoint = endpoint

    def get_endpoint(self) -> str:
        return self._endpoint

    async def set(
        self,
        cache_name: str,
        key: Union[str, bytes],
        value: Union[str, bytes],
        ttl: Optional[timedelta],
    ) -> cache_sdk_ops.CacheSetResponse:
        async def execute_set_request_fn(req: _SetRequest) -> _SetResponse:
            return await self._grpc_manager.async_stub().Set(
                req,
                metadata=_make_metadata(cache_name),
                timeout=self._default_deadline_seconds,
            )

        return await wrap_async_with_error_handling(
            cache_name=cache_name,
            request_type="Set",
            prepare_request_fn=lambda: prepare_set_request(  # type: ignore[no-any-return]
                key, value, ttl, self._default_ttl
            ),
            execute_request_fn=execute_set_request_fn,
            response_fn=construct_set_response,
        )

    async def get(self, cache_name: str, key: Union[str, bytes]) -> cache_sdk_ops.CacheGetResponse:
        async def execute_get_request_fn(req: _GetRequest) -> _GetResponse:
            return await self._grpc_manager.async_stub().Get(
                req,
                metadata=_make_metadata(cache_name),
                timeout=self._default_deadline_seconds,
            )

        return await wrap_async_with_error_handling(
            cache_name=cache_name,
            request_type="Get",
            prepare_request_fn=lambda: prepare_get_request(key),  # type: ignore[no-any-return]
            execute_request_fn=execute_get_request_fn,
            response_fn=construct_get_response,
        )

    async def delete(self, cache_name: str, key: Union[str, bytes]) -> cache_sdk_ops.CacheDeleteResponse:
        async def execute_delete_request_fn(req: _DeleteRequest) -> _DeleteResponse:
            return await self._grpc_manager.async_stub().Delete(
                req,
                metadata=_make_metadata(cache_name),
                timeout=self._default_deadline_seconds,
            )

        return await wrap_async_with_error_handling(
            cache_name=cache_name,
            request_type="Delete",
            prepare_request_fn=lambda: prepare_delete_request(key),  # type: ignore[no-any-return]
            execute_request_fn=execute_delete_request_fn,
            response_fn=construct_delete_response,
        )

    async def close(self) -> None:
        await self._grpc_manager.close()

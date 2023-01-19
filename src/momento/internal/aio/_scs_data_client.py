from datetime import timedelta
from functools import partial
from typing import Optional, Union

from momento_wire_types.cacheclient_pb2 import (
    _DeleteRequest,
    _DeleteResponse,
    _GetRequest,
    _GetResponse,
    _SetRequest,
    _SetResponse,
)
from momento_wire_types.cacheclient_pb2_grpc import ScsStub

from momento import logs
from momento._utilities._data_validation import _validate_ttl
from momento.auth.credential_provider import CredentialProvider
from momento.config.configuration import Configuration
from momento.internal.aio._scs_grpc_manager import _DataGrpcManager
from momento.internal.aio._utilities import make_metadata
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
from momento.responses import (
    CacheDeleteResponse,
    CacheDeleteResponseBase,
    CacheGetResponse,
    CacheGetResponseBase,
    CacheSetResponse,
    CacheSetResponseBase,
)


class _ScsDataClient:
    """Internal"""

    def __init__(self, configuration: Configuration, credential_provider: CredentialProvider, default_ttl: timedelta):
        endpoint = credential_provider.get_cache_endpoint()
        self._logger = logs.logger
        self._logger.debug("Simple cache data client instantiated with endpoint: %s", endpoint)
        self._endpoint = endpoint

        default_deadline: timedelta = get_default_client_deadline(configuration)
        self._default_deadline_seconds = int(default_deadline.total_seconds())

        self._grpc_manager = _DataGrpcManager(credential_provider)

        _validate_ttl(default_ttl)
        self._default_ttl = default_ttl

    @property
    def endpoint(self) -> str:
        return self._endpoint

    async def set(
        self,
        cache_name: str,
        key: Union[str, bytes],
        value: Union[str, bytes],
        ttl: Optional[timedelta],
    ) -> CacheSetResponseBase:
        metadata = make_metadata(cache_name)

        async def execute_set_request_fn(req: _SetRequest) -> _SetResponse:
            return await self._getStub().Set(
                req,
                metadata=metadata,
                timeout=self._default_deadline_seconds,
            )

        return await wrap_async_with_error_handling(
            cache_name=cache_name,
            request_type="Set",
            prepare_request_fn=partial(
                prepare_set_request,
                key=key,
                value=value,
                ttl=ttl,
                default_ttl=self._default_ttl,
            ),
            execute_request_fn=execute_set_request_fn,
            response_fn=construct_set_response,
            error_fn=CacheSetResponse.Error.from_sdkexception,
            metadata=metadata,
        )

    async def get(self, cache_name: str, key: Union[str, bytes]) -> CacheGetResponseBase:
        metadata = make_metadata(cache_name)

        async def execute_get_request_fn(req: _GetRequest) -> _GetResponse:
            return await self._getStub().Get(
                req,
                metadata=metadata,
                timeout=self._default_deadline_seconds,
            )

        return await wrap_async_with_error_handling(
            cache_name=cache_name,
            request_type="Get",
            prepare_request_fn=partial(prepare_get_request, key=key),
            execute_request_fn=execute_get_request_fn,
            response_fn=construct_get_response,
            error_fn=CacheGetResponse.Error.from_sdkexception,
            metadata=metadata,
        )

    async def delete(self, cache_name: str, key: Union[str, bytes]) -> CacheDeleteResponseBase:
        metadata = make_metadata(cache_name)

        async def execute_delete_request_fn(req: _DeleteRequest) -> _DeleteResponse:
            return await self._getStub().Delete(
                req,
                metadata=metadata,
                timeout=self._default_deadline_seconds,
            )

        return await wrap_async_with_error_handling(
            cache_name=cache_name,
            request_type="Delete",
            prepare_request_fn=partial(prepare_delete_request, key=key),
            execute_request_fn=execute_delete_request_fn,
            response_fn=construct_delete_response,
            error_fn=CacheDeleteResponse.Error.from_sdkexception,
            metadata=metadata,
        )

    def _getStub(self) -> ScsStub:
        return self._grpc_manager.async_stub()

    async def close(self) -> None:
        await self._grpc_manager.close()

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
from momento.internal.synchronous._scs_grpc_manager import _DataGrpcManager
from momento.internal.synchronous._utilities import make_metadata
from momento.internal.common._data_client_ops import (
    construct_delete_response_new,
    construct_get_response_new,
    construct_set_response_new,
    prepare_delete_request,
    prepare_get_request,
    prepare_set_request,
    wrap_with_error_handling,
)
from momento.responses import (
    CacheDeleteResponse,
    CacheDeleteResponseBase,
    CacheGetResponse,
    CacheGetResponseBase,
    CacheSetResponse,
    CacheSetResponseBase,
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
        self._logger = logs.logger
        self._logger.debug("Simple cache data client instantiated with endpoint: %s", endpoint)
        self._default_deadline_seconds = (
            _DEFAULT_DEADLINE_SECONDS if not operation_timeout_ms else operation_timeout_ms / 1000.0
        )
        self._grpc_manager = _DataGrpcManager(auth_token, endpoint)
        _validate_ttl(default_ttl_seconds)
        self._default_ttl_seconds = default_ttl_seconds
        self._endpoint = endpoint

    @property
    def endpoint(self) -> str:
        return self._endpoint

    def set(
        self,
        cache_name: str,
        key: Union[str, bytes],
        value: Union[str, bytes],
        ttl_seconds: Optional[int],
    ) -> CacheSetResponseBase:
        metadata = make_metadata(cache_name)

        def execute_set_request_fn(req: _SetRequest) -> _SetResponse:
            return self._getStub().Set(
                req,
                metadata=metadata,
                timeout=self._default_deadline_seconds,
            )

        return wrap_with_error_handling(
            cache_name=cache_name,
            request_type="Set",
            prepare_request_fn=partial(
                prepare_set_request,
                key=key,
                value=value,
                ttl_seconds=ttl_seconds,
                default_ttl_seconds=self._default_ttl_seconds,
            ),
            execute_request_fn=execute_set_request_fn,
            response_fn=construct_set_response_new,
            error_fn=CacheSetResponse.Error.from_sdkexception,
            metadata=metadata,
        )

    def get(self, cache_name: str, key: Union[str, bytes]) -> CacheGetResponseBase:
        metadata = make_metadata(cache_name)

        def execute_get_request_fn(req: _GetRequest) -> _GetResponse:
            return self._getStub().Get(
                req,
                metadata=metadata,
                timeout=self._default_deadline_seconds,
            )

        return wrap_with_error_handling(
            cache_name=cache_name,
            request_type="Get",
            prepare_request_fn=partial(prepare_get_request, key=key),
            execute_request_fn=execute_get_request_fn,
            response_fn=construct_get_response_new,
            error_fn=CacheGetResponse.Error.from_sdkexception,
            metadata=metadata,
        )

    def delete(self, cache_name: str, key: Union[str, bytes]) -> CacheDeleteResponseBase:
        metadata = make_metadata(cache_name)

        def execute_delete_request_fn(req: _DeleteRequest) -> _DeleteResponse:
            return self._getStub().Delete(
                req,
                metadata=metadata,
                timeout=self._default_deadline_seconds,
            )

        return wrap_with_error_handling(
            cache_name=cache_name,
            request_type="Delete",
            prepare_request_fn=partial(prepare_delete_request, key=key),
            execute_request_fn=execute_delete_request_fn,
            response_fn=construct_delete_response_new,
            error_fn=CacheDeleteResponse.Error.from_sdkexception,
            metadata=metadata,
        )

    def _getStub(self) -> ScsStub:
        return self._grpc_manager.stub()

    def close(self) -> None:
        self._grpc_manager.close()

from datetime import timedelta
from typing import List, Optional, Tuple, Union

from momento_wire_types.cacheclient_pb2 import (
    _DeleteRequest,
    _DeleteResponse,
    _GetRequest,
    _GetResponse,
    _SetRequest,
    _SetResponse,
)
from momento_wire_types.cacheclient_pb2_grpc import ScsStub

from momento import cache_operation_types
from momento._utilities._data_validation import _validate_ttl
from momento.auth.credential_provider import CredentialProvider
from momento.config.configuration import Configuration

from ..common._data_client_ops import (
    construct_delete_response,
    construct_get_response,
    construct_set_response,
    prepare_delete_request,
    prepare_get_request,
    prepare_set_request,
    wrap_with_error_handling,
)
from . import _scs_grpc_manager

_DEFAULT_DEADLINE_SECONDS = timedelta(seconds=5)


def _make_metadata(cache_name: str) -> List[Tuple[str, str]]:
    return [("cache", cache_name)]


class _ScsDataClient:
    """Internal"""

    def __init__(
        self,
        configuration: Configuration,
        auth_provider: CredentialProvider,
        default_ttl: timedelta,
    ):
        default_deadline: timedelta = (
            configuration.get_transport_strategy().get_grpc_configuration().get_deadline()
            or _DEFAULT_DEADLINE_SECONDS
        )
        self._default_deadline_seconds = int(default_deadline.total_seconds())
        self._grpc_manager = _scs_grpc_manager._DataGrpcManager(
            auth_provider.get_auth_token(),
            auth_provider.get_cache_endpoint()
        )
        _validate_ttl(default_ttl)
        self._default_ttlSeconds = int(default_ttl.total_seconds())

    def set(
        self,
        cache_name: str,
        key: Union[str, bytes],
        value: Union[str, bytes],
        ttl: Optional[timedelta],
    ) -> cache_operation_types.CacheSetResponse:
        def execute_set_request_fn(req: _SetRequest) -> _SetResponse:
            return self._get_stub().Set(
                req,
                metadata=_make_metadata(cache_name),
                timeout=self._default_deadline_seconds,
            )

        ttl_seconds: Optional[int] = int(ttl.total_seconds()) if ttl else None
        return wrap_with_error_handling(
            cache_name=cache_name,
            request_type="Set",
            prepare_request_fn=lambda: prepare_set_request(  # type: ignore[no-any-return]
                key=key, value=value, ttl_seconds=ttl_seconds, default_ttl_seconds=self._default_ttlSeconds
            ),
            execute_request_fn=execute_set_request_fn,
            response_fn=construct_set_response,
        )

    def get(self, cache_name: str, key: Union[str, bytes]) -> cache_operation_types.CacheGetResponse:
        def execute_get_request_fn(req: _GetRequest) -> _GetResponse:
            return self._get_stub().Get(
                req,
                metadata=_make_metadata(cache_name),
                timeout=self._default_deadline_seconds,
            )

        return wrap_with_error_handling(
            cache_name=cache_name,
            request_type="Get",
            prepare_request_fn=lambda: prepare_get_request(key),  # type: ignore[no-any-return]
            execute_request_fn=execute_get_request_fn,
            response_fn=construct_get_response,
        )

    def delete(self, cache_name: str, key: Union[str, bytes]) -> cache_operation_types.CacheDeleteResponse:
        def execute_delete_request_fn(req: _DeleteRequest) -> _DeleteResponse:
            return self._get_stub().Delete(
                req,
                metadata=_make_metadata(cache_name),
                timeout=self._default_deadline_seconds,
            )

        return wrap_with_error_handling(
            cache_name=cache_name,
            request_type="Delete",
            prepare_request_fn=lambda: prepare_delete_request(key),  # type: ignore[no-any-return]
            execute_request_fn=execute_delete_request_fn,
            response_fn=construct_delete_response,
        )

    def _get_stub(self) -> ScsStub:
        return self._grpc_manager.stub()

    def close(self) -> None:
        self._grpc_manager.close()

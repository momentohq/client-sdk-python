from datetime import timedelta
from functools import partial
from typing import Dict, Optional

from momento_wire_types.cacheclient_pb2 import (
    _DeleteRequest,
    _DeleteResponse,
    _GetRequest,
    _GetResponse,
    _ListConcatenateBackRequest,
    _ListFetchRequest,
    _SetRequest,
    _SetResponse,
)
from momento_wire_types.cacheclient_pb2_grpc import ScsStub

from momento import logs
from momento.auth import CredentialProvider
from momento.config import Configuration
from momento.errors import UnknownException, convert_error
from momento.internal._utilities import (
    _as_bytes,
    _list_as_bytes,
    _validate_cache_name,
    _validate_list_name,
    _validate_ttl,
)
from momento.internal.common._data_client_ops import (
    get_default_client_deadline,
    wrap_with_error_handling,
)
from momento.internal.common._data_client_scalar_ops import (
    construct_delete_response,
    construct_get_response,
    construct_set_response,
    prepare_delete_request,
    prepare_get_request,
    prepare_set_request,
)
from momento.internal.synchronous._scs_grpc_manager import _DataGrpcManager
from momento.internal.synchronous._utilities import make_metadata
from momento.requests import CollectionTtl
from momento.responses import (
    CacheDelete,
    CacheDeleteResponse,
    CacheGet,
    CacheGetResponse,
    CacheListConcatenateBack,
    CacheListConcatenateBackResponse,
    CacheListFetch,
    CacheListFetchResponse,
    CacheSet,
    CacheSetResponse,
)
from momento.typing import TCacheName, TListName, TListValues, TScalarKey, TScalarValue


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

    def set(
        self,
        cache_name: str,
        key: TScalarKey,
        value: TScalarValue,
        ttl: Optional[timedelta],
    ) -> CacheSetResponse:
        metadata = make_metadata(cache_name)

        def execute_set_request_fn(req: _SetRequest) -> _SetResponse:
            return self._build_stub().Set(
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
                ttl=ttl,
                default_ttl=self._default_ttl,
            ),
            execute_request_fn=execute_set_request_fn,
            response_fn=construct_set_response,
            error_fn=CacheSet.Error.from_sdkexception,
            metadata=metadata,
        )

    def get(self, cache_name: str, key: TScalarKey) -> CacheGetResponse:
        metadata = make_metadata(cache_name)

        def execute_get_request_fn(req: _GetRequest) -> _GetResponse:
            return self._build_stub().Get(
                req,
                metadata=metadata,
                timeout=self._default_deadline_seconds,
            )

        return wrap_with_error_handling(
            cache_name=cache_name,
            request_type="Get",
            prepare_request_fn=partial(prepare_get_request, key=key),
            execute_request_fn=execute_get_request_fn,
            response_fn=construct_get_response,
            error_fn=CacheGet.Error.from_sdkexception,
            metadata=metadata,
        )

    def delete(self, cache_name: str, key: TScalarKey) -> CacheDeleteResponse:
        metadata = make_metadata(cache_name)

        def execute_delete_request_fn(req: _DeleteRequest) -> _DeleteResponse:
            return self._build_stub().Delete(
                req,
                metadata=metadata,
                timeout=self._default_deadline_seconds,
            )

        return wrap_with_error_handling(
            cache_name=cache_name,
            request_type="Delete",
            prepare_request_fn=partial(prepare_delete_request, key=key),
            execute_request_fn=execute_delete_request_fn,
            response_fn=construct_delete_response,
            error_fn=CacheDelete.Error.from_sdkexception,
            metadata=metadata,
        )

    # DICTIONARY COLLECTION METHODS

    # LIST COLLECTION METHODS
    def list_concatenate_back(
        self,
        cache_name: TCacheName,
        list_name: TListName,
        values: TListValues,
        ttl: CollectionTtl = CollectionTtl.from_cache_ttl(),
        truncate_front_to_size: Optional[int] = None,
    ) -> CacheListConcatenateBackResponse:
        try:
            self._log_issuing_request("ListConcatenateBack", {})
            _validate_cache_name(cache_name)
            _validate_list_name(list_name)

            item_ttl = self._default_ttl if ttl.ttl is None else ttl.ttl
            request = _ListConcatenateBackRequest()
            request.list_name = _as_bytes(list_name, "Unsupported type for list_name: ")
            request.values.extend(_list_as_bytes(values, "Unsupported type for values: "))
            request.ttl_milliseconds = int(item_ttl.total_seconds() * 1000)
            request.refresh_ttl = ttl.refresh_ttl
            if truncate_front_to_size is not None:
                request.truncate_front_to_size = truncate_front_to_size

            self._build_stub().ListConcatenateBack(
                request,
                metadata=make_metadata(cache_name),
                timeout=self._default_deadline_seconds,
            )
            self._log_received_response("ListConcatenateBack", {"list_name": str(request.list_name)})
            return CacheListConcatenateBack.Success()
        except Exception as e:
            self._log_request_error("list_concatenate_back", e)
            return CacheListConcatenateBack.Error(convert_error(e))

    def list_fetch(self, cache_name: TCacheName, list_name: TListName) -> CacheListFetchResponse:
        try:
            self._log_issuing_request("ListFetch", {"list_name": str(list_name)})
            _validate_cache_name(cache_name)
            _validate_list_name(list_name)
            request = _ListFetchRequest()
            request.list_name = _as_bytes(list_name, "Unsupported type for list_name: ")
            response = self._build_stub().ListFetch(
                request,
                metadata=make_metadata(cache_name),
                timeout=self._default_deadline_seconds,
            )
            self._log_received_response("ListFetch", {"list_name": str(request.list_name)})

            type = response.WhichOneof("list")
            if type == "missing":
                return CacheListFetch.Miss()
            elif type == "found":
                return CacheListFetch.Hit(response.found.values)
            else:
                raise UnknownException("Unknown list field")
        except Exception as e:
            self._log_request_error("list_fetch", e)
            return CacheListFetch.Error(convert_error(e))

    # SET COLLECTION METHODS

    def _log_received_response(self, request_type: str, request_args: Dict[str, str]) -> None:
        self._logger.log(logs.TRACE, f"Received a {request_type} response for {request_args}")

    def _log_issuing_request(self, request_type: str, request_args: Dict[str, str]) -> None:
        self._logger.log(logs.TRACE, f"Issuing a {request_type} request with {request_args}")

    def _log_request_error(self, request_type: str, e: Exception) -> None:
        self._logger.warning(f"{request_type} failed with exception: {e}")

    def _build_stub(self) -> ScsStub:
        return self._grpc_manager.stub()

    def close(self) -> None:
        self._grpc_manager.close()

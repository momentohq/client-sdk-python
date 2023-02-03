from datetime import timedelta
from functools import partial
from typing import Dict, Optional

from momento_wire_types.cacheclient_pb2 import (
    _DeleteRequest,
    _DeleteResponse,
    _GetRequest,
    _GetResponse,
    _ListConcatenateBackRequest,
    _ListConcatenateFrontRequest,
    _ListFetchRequest,
    _ListLengthRequest,
    _ListPopBackRequest,
    _ListPopFrontRequest,
    _ListPushBackRequest,
    _ListPushFrontRequest,
    _ListRemoveRequest,
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
from momento.internal.aio._scs_grpc_manager import _DataGrpcManager
from momento.internal.aio._utilities import make_metadata
from momento.internal.common._data_client_ops import (
    get_default_client_deadline,
    wrap_async_with_error_handling,
)
from momento.internal.common._data_client_scalar_ops import (
    construct_delete_response,
    construct_get_response,
    construct_set_response,
    prepare_delete_request,
    prepare_get_request,
    prepare_set_request,
)
from momento.requests import CollectionTtl
from momento.responses import (
    CacheDelete,
    CacheDeleteResponse,
    CacheGet,
    CacheGetResponse,
    CacheListConcatenateBack,
    CacheListConcatenateBackResponse,
    CacheListConcatenateFront,
    CacheListConcatenateFrontResponse,
    CacheListFetch,
    CacheListFetchResponse,
    CacheListLength,
    CacheListLengthResponse,
    CacheListPopBack,
    CacheListPopBackResponse,
    CacheListPopFront,
    CacheListPopFrontResponse,
    CacheListPushBack,
    CacheListPushBackResponse,
    CacheListPushFront,
    CacheListPushFrontResponse,
    CacheListRemoveValue,
    CacheListRemoveValueResponse,
    CacheSet,
    CacheSetResponse,
)
from momento.typing import (
    TCacheName,
    TListName,
    TListValue,
    TListValuesInput,
    TScalarKey,
    TScalarValue,
)


class _ScsDataClient:
    """Internal"""

    __UNSUPPORTED_LIST_NAME_TYPE_MSG = "Unsupported type for list_name: "
    __UNSUPPORTED_LIST_VALUE_TYPE_MSG = "Unsupported type for value: "
    __UNSUPPORTED_LIST_VALUES_TYPE_MSG = "Unsupported type for values: "

    def __init__(self, configuration: Configuration, credential_provider: CredentialProvider, default_ttl: timedelta):
        endpoint = credential_provider.cache_endpoint
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
        key: TScalarKey,
        value: TScalarValue,
        ttl: Optional[timedelta],
    ) -> CacheSetResponse:
        metadata = make_metadata(cache_name)

        async def execute_set_request_fn(req: _SetRequest) -> _SetResponse:
            return await self._build_stub().Set(
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
            error_fn=CacheSet.Error.from_sdkexception,
            metadata=metadata,
        )

    async def get(self, cache_name: str, key: TScalarKey) -> CacheGetResponse:
        metadata = make_metadata(cache_name)

        async def execute_get_request_fn(req: _GetRequest) -> _GetResponse:
            return await self._build_stub().Get(
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
            error_fn=CacheGet.Error.from_sdkexception,
            metadata=metadata,
        )

    async def delete(self, cache_name: str, key: TScalarKey) -> CacheDeleteResponse:
        metadata = make_metadata(cache_name)

        async def execute_delete_request_fn(req: _DeleteRequest) -> _DeleteResponse:
            return await self._build_stub().Delete(
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
            error_fn=CacheDelete.Error.from_sdkexception,
            metadata=metadata,
        )

    # DICTIONARY COLLECTION METHODS

    # LIST COLLECTION METHODS
    async def list_concatenate_back(
        self,
        cache_name: TCacheName,
        list_name: TListName,
        values: TListValuesInput,
        ttl: CollectionTtl = CollectionTtl.from_cache_ttl(),
        truncate_front_to_size: Optional[int] = None,
    ) -> CacheListConcatenateBackResponse:
        try:
            self._log_issuing_request("ListConcatenateBack", {})
            _validate_cache_name(cache_name)
            _validate_list_name(list_name)

            item_ttl = self._default_ttl if ttl.ttl is None else ttl.ttl
            request = _ListConcatenateBackRequest()
            request.list_name = _as_bytes(list_name, self.__UNSUPPORTED_LIST_NAME_TYPE_MSG)
            request.values.extend(_list_as_bytes(values, self.__UNSUPPORTED_LIST_VALUES_TYPE_MSG))
            request.ttl_milliseconds = int(item_ttl.total_seconds() * 1000)
            request.refresh_ttl = ttl.refresh_ttl
            if truncate_front_to_size is not None:
                request.truncate_front_to_size = truncate_front_to_size

            response = await self._build_stub().ListConcatenateBack(
                request,
                metadata=make_metadata(cache_name),
                timeout=self._default_deadline_seconds,
            )
            self._log_received_response("ListConcatenateBack", {"list_name": str(request.list_name)})
            return CacheListConcatenateBack.Success(response.list_length)
        except Exception as e:
            self._log_request_error("list_concatenate_back", e)
            return CacheListConcatenateBack.Error(convert_error(e))

    async def list_concatenate_front(
        self,
        cache_name: TCacheName,
        list_name: TListName,
        values: TListValuesInput,
        ttl: CollectionTtl = CollectionTtl.from_cache_ttl(),
        truncate_back_to_size: Optional[int] = None,
    ) -> CacheListConcatenateFrontResponse:
        try:
            self._log_issuing_request("ListConcatenateFront", {})
            _validate_cache_name(cache_name)
            _validate_list_name(list_name)

            item_ttl = self._default_ttl if ttl.ttl is None else ttl.ttl
            request = _ListConcatenateFrontRequest()
            request.list_name = _as_bytes(list_name, self.__UNSUPPORTED_LIST_NAME_TYPE_MSG)
            request.values.extend(_list_as_bytes(values, self.__UNSUPPORTED_LIST_VALUES_TYPE_MSG))
            request.ttl_milliseconds = int(item_ttl.total_seconds() * 1000)
            request.refresh_ttl = ttl.refresh_ttl
            if truncate_back_to_size is not None:
                request.truncate_back_to_size = truncate_back_to_size

            response = await self._build_stub().ListConcatenateFront(
                request,
                metadata=make_metadata(cache_name),
                timeout=self._default_deadline_seconds,
            )
            self._log_received_response("ListConcatenateFront", {"list_name": str(request.list_name)})
            return CacheListConcatenateFront.Success(response.list_length)
        except Exception as e:
            self._log_request_error("list_concatenate_front", e)
            return CacheListConcatenateFront.Error(convert_error(e))

    async def list_fetch(self, cache_name: TCacheName, list_name: TListName) -> CacheListFetchResponse:
        try:
            self._log_issuing_request("ListFetch", {"list_name": str(list_name)})
            _validate_cache_name(cache_name)
            _validate_list_name(list_name)
            request = _ListFetchRequest()
            request.list_name = _as_bytes(list_name, self.__UNSUPPORTED_LIST_NAME_TYPE_MSG)
            response = await self._build_stub().ListFetch(
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

    async def list_length(self, cache_name: TCacheName, list_name: TListName) -> CacheListLengthResponse:
        try:
            self._log_issuing_request("ListLength", {"list_name": str(list_name)})
            _validate_cache_name(cache_name)
            _validate_list_name(list_name)
            request = _ListLengthRequest()
            request.list_name = _as_bytes(list_name, self.__UNSUPPORTED_LIST_NAME_TYPE_MSG)
            response = await self._build_stub().ListLength(
                request,
                metadata=make_metadata(cache_name),
                timeout=self._default_deadline_seconds,
            )
            self._log_received_response("ListLength", {"list_name": str(request.list_name)})

            type = response.WhichOneof("list")
            if type == "missing":
                return CacheListLength.Miss()
            elif type == "found":
                return CacheListLength.Hit(response.found.length)
            else:
                raise UnknownException("Unknown list field")
        except Exception as e:
            self._log_request_error("list_length", e)
            return CacheListLength.Error(convert_error(e))

    async def list_pop_back(self, cache_name: TCacheName, list_name: TListName) -> CacheListPopBackResponse:
        try:
            self._log_issuing_request("ListPopBack", {"list_name": str(list_name)})
            _validate_cache_name(cache_name)
            _validate_list_name(list_name)
            request = _ListPopBackRequest()
            request.list_name = _as_bytes(list_name, self.__UNSUPPORTED_LIST_NAME_TYPE_MSG)
            response = await self._build_stub().ListPopBack(
                request,
                metadata=make_metadata(cache_name),
                timeout=self._default_deadline_seconds,
            )
            self._log_received_response("ListPopBack", {"list_name": str(request.list_name)})

            type = response.WhichOneof("list")
            if type == "missing":
                return CacheListPopBack.Miss()
            elif type == "found":
                return CacheListPopBack.Hit(response.found.back)
            else:
                raise UnknownException("Unknown list field")
        except Exception as e:
            self._log_request_error("list_pop_back", e)
            return CacheListPopBack.Error(convert_error(e))

    async def list_pop_front(self, cache_name: TCacheName, list_name: TListName) -> CacheListPopFrontResponse:
        try:
            self._log_issuing_request("ListPopFront", {"list_name": str(list_name)})
            _validate_cache_name(cache_name)
            _validate_list_name(list_name)
            request = _ListPopFrontRequest()
            request.list_name = _as_bytes(list_name, self.__UNSUPPORTED_LIST_NAME_TYPE_MSG)
            response = await self._build_stub().ListPopFront(
                request,
                metadata=make_metadata(cache_name),
                timeout=self._default_deadline_seconds,
            )
            self._log_received_response("ListPopFront", {"list_name": str(request.list_name)})

            type = response.WhichOneof("list")
            if type == "missing":
                return CacheListPopFront.Miss()
            elif type == "found":
                return CacheListPopFront.Hit(response.found.front)
            else:
                raise UnknownException("Unknown list field")
        except Exception as e:
            self._log_request_error("list_pop_front", e)
            return CacheListPopFront.Error(convert_error(e))

    async def list_push_back(
        self,
        cache_name: TCacheName,
        list_name: TListName,
        value: TListValue,
        ttl: CollectionTtl = CollectionTtl.from_cache_ttl(),
        truncate_front_to_size: Optional[int] = None,
    ) -> CacheListPushBackResponse:
        try:
            self._log_issuing_request("ListPushBack", {})
            _validate_cache_name(cache_name)
            _validate_list_name(list_name)

            item_ttl = self._default_ttl if ttl.ttl is None else ttl.ttl
            request = _ListPushBackRequest()
            request.list_name = _as_bytes(list_name, self.__UNSUPPORTED_LIST_NAME_TYPE_MSG)
            request.value = _as_bytes(value, self.__UNSUPPORTED_LIST_VALUE_TYPE_MSG)
            request.ttl_milliseconds = int(item_ttl.total_seconds() * 1000)
            request.refresh_ttl = ttl.refresh_ttl
            if truncate_front_to_size is not None:
                request.truncate_front_to_size = truncate_front_to_size

            response = await self._build_stub().ListPushBack(
                request,
                metadata=make_metadata(cache_name),
                timeout=self._default_deadline_seconds,
            )
            self._log_received_response("ListPushBack", {"list_name": str(request.list_name)})
            return CacheListPushBack.Success(response.list_length)
        except Exception as e:
            self._log_request_error("list_push_back", e)
            return CacheListPushBack.Error(convert_error(e))

    async def list_push_front(
        self,
        cache_name: TCacheName,
        list_name: TListName,
        value: TListValue,
        ttl: CollectionTtl = CollectionTtl.from_cache_ttl(),
        truncate_back_to_size: Optional[int] = None,
    ) -> CacheListPushFrontResponse:
        try:
            self._log_issuing_request("ListPushFront", {})
            _validate_cache_name(cache_name)
            _validate_list_name(list_name)

            item_ttl = self._default_ttl if ttl.ttl is None else ttl.ttl
            request = _ListPushFrontRequest()
            request.list_name = _as_bytes(list_name, self.__UNSUPPORTED_LIST_NAME_TYPE_MSG)
            request.value = _as_bytes(value, self.__UNSUPPORTED_LIST_VALUE_TYPE_MSG)
            request.ttl_milliseconds = int(item_ttl.total_seconds() * 1000)
            request.refresh_ttl = ttl.refresh_ttl
            if truncate_back_to_size is not None:
                request.truncate_back_to_size = truncate_back_to_size

            response = await self._build_stub().ListPushFront(
                request,
                metadata=make_metadata(cache_name),
                timeout=self._default_deadline_seconds,
            )
            self._log_received_response("ListPushFront", {"list_name": str(request.list_name)})
            return CacheListPushFront.Success(response.list_length)
        except Exception as e:
            self._log_request_error("list_push_front", e)
            return CacheListPushFront.Error(convert_error(e))

    async def list_remove_value(
        self,
        cache_name: TCacheName,
        list_name: TListName,
        value: TListValue,
    ) -> CacheListRemoveValueResponse:
        try:
            self._log_issuing_request("ListRemoveValue", {})
            _validate_cache_name(cache_name)
            _validate_list_name(list_name)

            request = _ListRemoveRequest()
            request.list_name = _as_bytes(list_name, self.__UNSUPPORTED_LIST_NAME_TYPE_MSG)
            request.all_elements_with_value = _as_bytes(value, self.__UNSUPPORTED_LIST_VALUE_TYPE_MSG)

            await self._build_stub().ListRemove(
                request,
                metadata=make_metadata(cache_name),
                timeout=self._default_deadline_seconds,
            )
            self._log_received_response("ListRemoveValue", {"list_name": str(request.list_name)})
            return CacheListRemoveValue.Success()
        except Exception as e:
            self._log_request_error("list_remove_value", e)
            return CacheListRemoveValue.Error(convert_error(e))

    # SET COLLECTION METHODS

    def _log_received_response(self, request_type: str, request_args: Dict[str, str]) -> None:
        self._logger.log(logs.TRACE, f"Received a {request_type} response for {request_args}")

    def _log_issuing_request(self, request_type: str, request_args: Dict[str, str]) -> None:
        self._logger.log(logs.TRACE, f"Issuing a {request_type} request with {request_args}")

    def _log_request_error(self, request_type: str, e: Exception) -> None:
        self._logger.warning(f"{request_type} failed with exception: {e}")

    def _build_stub(self) -> ScsStub:
        return self._grpc_manager.async_stub()

    async def close(self) -> None:
        await self._grpc_manager.close()

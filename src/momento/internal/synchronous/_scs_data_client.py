from __future__ import annotations

from datetime import timedelta
from typing import Any, Optional

from momento_wire_types import cacheclient_pb2 as cache_pb
from momento_wire_types import cacheclient_pb2_grpc as cache_grpc

from momento import logs
from momento.auth import CredentialProvider
from momento.config import Configuration
from momento.errors import UnknownException, convert_error
from momento.internal._utilities import (
    _as_bytes,
    _gen_dictionary_fields_as_bytes,
    _gen_dictionary_items_as_bytes,
    _gen_list_as_bytes,
    _gen_set_input_as_bytes,
    _validate_cache_name,
    _validate_dictionary_name,
    _validate_list_name,
    _validate_set_name,
    _validate_ttl,
)
from momento.internal._utilities._data_validation import (
    _gen_sorted_set_elements_as_bytes,
    _gen_sorted_set_values_as_bytes,
    _validate_sorted_set_name,
    _validate_sorted_set_score,
)
from momento.internal.synchronous._scs_grpc_manager import _DataGrpcManager
from momento.internal.synchronous._utilities import make_metadata
from momento.requests import CollectionTtl, SortOrder
from momento.responses import (
    CacheDelete,
    CacheDeleteResponse,
    CacheDictionaryFetch,
    CacheDictionaryFetchResponse,
    CacheDictionaryGetField,
    CacheDictionaryGetFieldResponse,
    CacheDictionaryGetFields,
    CacheDictionaryGetFieldsResponse,
    CacheDictionaryIncrement,
    CacheDictionaryIncrementResponse,
    CacheDictionaryRemoveFields,
    CacheDictionaryRemoveFieldsResponse,
    CacheDictionarySetFields,
    CacheDictionarySetFieldsResponse,
    CacheGet,
    CacheGetResponse,
    CacheIncrement,
    CacheIncrementResponse,
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
    CacheSetAddElements,
    CacheSetAddElementsResponse,
    CacheSetFetch,
    CacheSetFetchResponse,
    CacheSetIfNotExists,
    CacheSetIfNotExistsResponse,
    CacheSetRemoveElements,
    CacheSetRemoveElementsResponse,
    CacheSetResponse,
)
from momento.responses.data.sorted_set.fetch import (
    CacheSortedSetFetch,
    CacheSortedSetFetchResponse,
)
from momento.responses.data.sorted_set.get_rank import (
    CacheSortedSetGetRank,
    CacheSortedSetGetRankResponse,
)
from momento.responses.data.sorted_set.get_score import (
    CacheSortedSetGetScore,
    CacheSortedSetGetScoreResponse,
)
from momento.responses.data.sorted_set.get_scores import (
    CacheSortedSetGetScores,
    CacheSortedSetGetScoresResponse,
)
from momento.responses.data.sorted_set.increment import (
    CacheSortedSetIncrement,
    CacheSortedSetIncrementResponse,
)
from momento.responses.data.sorted_set.put_elements import (
    CacheSortedSetPutElements,
    CacheSortedSetPutElementsResponse,
)
from momento.responses.data.sorted_set.remove_elements import (
    CacheSortedSetRemoveElements,
    CacheSortedSetRemoveElementsResponse,
)
from momento.typing import (
    TCacheName,
    TDictionaryField,
    TDictionaryFields,
    TDictionaryItems,
    TDictionaryName,
    TListName,
    TListValue,
    TListValuesInput,
    TScalarKey,
    TScalarValue,
    TSetElementsInput,
    TSetName,
    TSortedSetElements,
    TSortedSetName,
    TSortedSetScore,
    TSortedSetValue,
    TSortedSetValues,
)


class _ScsDataClient:
    """Internal data client."""

    __UNSUPPORTED_LIST_NAME_TYPE_MSG = "Unsupported type for list_name: "
    __UNSUPPORTED_LIST_VALUE_TYPE_MSG = "Unsupported type for value: "
    __UNSUPPORTED_LIST_VALUES_TYPE_MSG = "Unsupported type for values: "
    __UNSUPPORTED_SET_NAME_TYPE_MSG = "Unsupported type for set_name: "
    __UNSUPPORTED_SET_ELEMENTS_TYPE_MSG = "Unsupported type for elements: "
    __UNSUPPORTED_SORTED_SET_NAME_TYPE_MSG = "Unsupported type for sorted_set_name: "
    __UNSUPPORTED_SORTED_SET_ELEMENTS_TYPE_MSG = "Unsupported type for sorted set elements: "
    __UNSUPPORTED_SORTED_SET_VALUE_TYPE_MSG = "Unsupported type for sorted set value: "
    __UNSUPPORTED_SORTED_SET_VALUES_TYPE_MSG = "Unsupported type for sorted set values: "

    __UNSUPPORTED_DICTIONARY_NAME_TYPE_MSG = "Unsupported type for dictionary_name: "
    __UNSUPPORTED_DICTIONARY_FIELD_TYPE_MSG = "Unsupported type for field: "
    __UNSUPPORTED_DICTIONARY_FIELDS_TYPE_MSG = "Unsupported type for fields: "
    __UNSUPPORTED_DICTIONARY_ITEMS_TYPE_MSG = "Unsupported type for items: "

    def __init__(self, configuration: Configuration, credential_provider: CredentialProvider, default_ttl: timedelta):
        endpoint = credential_provider.cache_endpoint
        self._logger = logs.logger
        self._logger.debug("Simple cache data client instantiated with endpoint: %s", endpoint)
        self._endpoint = endpoint

        default_deadline: timedelta = configuration.get_transport_strategy().get_grpc_configuration().get_deadline()
        self._default_deadline_seconds = int(default_deadline.total_seconds())

        self._grpc_manager = _DataGrpcManager(configuration, credential_provider)

        _validate_ttl(default_ttl)
        self._default_ttl = default_ttl

    @property
    def endpoint(self) -> str:
        return self._endpoint

    def increment(
        self, cache_name: TCacheName, key: TScalarKey, amount: int = 1, ttl: Optional[timedelta] = None
    ) -> CacheIncrementResponse:
        try:
            self._log_issuing_request("Increment", {"key": str(key), "amount": str(amount)})
            _validate_cache_name(cache_name)
            _validate_ttl(ttl)

            request = cache_pb._IncrementRequest(
                cache_key=_as_bytes(key, "Unsupported type for key: "),
                amount=amount,
                ttl_milliseconds=self._ttl_or_default_milliseconds(ttl),
            )

            response = self._build_stub().Increment(
                request,
                metadata=make_metadata(cache_name),
                timeout=self._default_deadline_seconds,
            )
            self._log_received_response("Increment", {"key": str(key), "amount": str(amount)})
            return CacheIncrement.Success(response.value)
        except Exception as e:
            self._log_request_error("increment", e)
            return CacheIncrement.Error(convert_error(e))

    def set(
        self,
        cache_name: str,
        key: TScalarKey,
        value: TScalarValue,
        ttl: Optional[timedelta],
    ) -> CacheSetResponse:
        try:
            self._log_issuing_request("Set", {"key": str(key)})
            _validate_cache_name(cache_name)
            _validate_ttl(ttl)
            request = cache_pb._SetRequest(
                cache_key=_as_bytes(key, "Unsupported type for key: "),
                cache_body=_as_bytes(value, "Unsupported type for value: "),
                ttl_milliseconds=self._ttl_or_default_milliseconds(ttl),
            )

            self._build_stub().Set(request, metadata=make_metadata(cache_name), timeout=self._default_deadline_seconds)

            self._log_received_response("Set", {"key": str(key)})
            return CacheSet.Success()
        except Exception as e:
            self._log_request_error("set", e)
            return CacheSet.Error(convert_error(e))

    def set_if_not_exists(
        self, cache_name: TCacheName, key: TScalarKey, value: TScalarValue, ttl: Optional[timedelta]
    ) -> CacheSetIfNotExistsResponse:
        try:
            self._log_issuing_request("SetIfNotExists", {"key": str(key)})

            _validate_cache_name(cache_name)
            _validate_ttl(ttl)
            request = cache_pb._SetIfNotExistsRequest(
                cache_key=_as_bytes(key, "Unsupported type for key: "),
                cache_body=_as_bytes(value, "Unsupported type for value: "),
                ttl_milliseconds=self._ttl_or_default_milliseconds(ttl),
            )

            response = self._build_stub().SetIfNotExists(
                request, metadata=make_metadata(cache_name), timeout=self._default_deadline_seconds
            )

            self._log_received_response("SetIfNotExists", {"key": str(key)})

            result = response.WhichOneof("result")
            if result == "stored":
                return CacheSetIfNotExists.Stored()
            elif result == "not_stored":
                return CacheSetIfNotExists.NotStored()
            else:
                raise UnknownException("SetIfNotExists responded with an unknown result")
        except Exception as e:
            self._log_request_error("set_if_not_exists", e)
            return CacheSetIfNotExists.Error(convert_error(e))

    def get(self, cache_name: str, key: TScalarKey) -> CacheGetResponse:
        try:
            self._log_issuing_request("Get", {"key": str(key)})

            _validate_cache_name(cache_name)
            request = cache_pb._GetRequest(cache_key=_as_bytes(key, "Unsupported type for key: "))

            response = self._build_stub().Get(
                request, metadata=make_metadata(cache_name), timeout=self._default_deadline_seconds
            )

            self._log_received_response("Get", {"key": str(key)})

            if response.result == cache_pb.Hit:
                return CacheGet.Hit(response.cache_body)
            elif response.result == cache_pb.Miss:
                return CacheGet.Miss()
            else:
                raise UnknownException("Get responded with an unknown result")
        except Exception as e:
            self._log_request_error("set_if_not_exists", e)
            return CacheGet.Error(convert_error(e))

    def delete(self, cache_name: str, key: TScalarKey) -> CacheDeleteResponse:
        try:
            self._log_issuing_request("Delete", {"key": str(key)})
            _validate_cache_name(cache_name)
            request = cache_pb._DeleteRequest(cache_key=_as_bytes(key, "Unsupported type for key: "))

            self._build_stub().Delete(
                request, metadata=make_metadata(cache_name), timeout=self._default_deadline_seconds
            )

            self._log_received_response("Delete", {"key": str(key)})
            return CacheDelete.Success()
        except Exception as e:
            self._log_request_error("set", e)
            return CacheDelete.Error(convert_error(e))

    # DICTIONARY COLLECTION METHODS
    def dictionary_get_fields(
        self,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
        fields: TDictionaryFields,
    ) -> CacheDictionaryGetFieldsResponse:
        try:
            self._log_issuing_request("DictionaryGet", {"dictionary_name": dictionary_name})
            _validate_cache_name(cache_name)
            _validate_dictionary_name(dictionary_name)

            bytes_fields = list(_gen_dictionary_fields_as_bytes(fields, self.__UNSUPPORTED_DICTIONARY_FIELDS_TYPE_MSG))
            request = cache_pb._DictionaryGetRequest(
                dictionary_name=_as_bytes(dictionary_name, self.__UNSUPPORTED_DICTIONARY_NAME_TYPE_MSG),
                fields=bytes_fields,
            )

            response = self._build_stub().DictionaryGet(
                request,
                metadata=make_metadata(cache_name),
                timeout=self._default_deadline_seconds,
            )
            self._log_received_response("DictionaryGet", {"dictionary_name": dictionary_name})

            type = response.WhichOneof("dictionary")
            if type == "found":
                get_responses: list[CacheDictionaryGetFieldResponse] = []
                for field, get_response in zip(bytes_fields, response.found.items):
                    if get_response.result == cache_pb.Miss:
                        get_responses.append(CacheDictionaryGetField.Miss())
                    else:
                        get_responses.append(CacheDictionaryGetField.Hit(get_response.cache_body, field))
                return CacheDictionaryGetFields.Hit(get_responses)
            elif type == "missing":
                return CacheDictionaryGetFields.Miss()
            else:
                raise UnknownException("Unknown dictionary field")
        except Exception as e:
            self._log_request_error("dictionary_get_fields", e)
            return CacheDictionaryGetFields.Error(convert_error(e))

    def dictionary_fetch(
        self, cache_name: TCacheName, dictionary_name: TDictionaryName
    ) -> CacheDictionaryFetchResponse:
        try:
            self._log_issuing_request("DictionaryFetch", {"dictionary_name": dictionary_name})
            _validate_cache_name(cache_name)
            _validate_dictionary_name(dictionary_name)
            request = cache_pb._DictionaryFetchRequest(
                dictionary_name=_as_bytes(dictionary_name, self.__UNSUPPORTED_DICTIONARY_NAME_TYPE_MSG)
            )
            response = self._build_stub().DictionaryFetch(
                request,
                metadata=make_metadata(cache_name),
                timeout=self._default_deadline_seconds,
            )
            self._log_received_response("DictionaryFetch", {"dictionary_name": dictionary_name})

            type = response.WhichOneof("dictionary")
            if type == "missing":
                return CacheDictionaryFetch.Miss()
            elif type == "found":
                return CacheDictionaryFetch.Hit({item.field: item.value for item in response.found.items})
            else:
                raise UnknownException("Unknown dictionary field")
        except Exception as e:
            self._log_request_error("dictionary_fetch", e)
            return CacheDictionaryFetch.Error(convert_error(e))

    def dictionary_increment(
        self,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
        field: TDictionaryField,
        amount: int = 1,
        ttl: CollectionTtl = CollectionTtl.from_cache_ttl(),
    ) -> CacheDictionaryIncrementResponse:
        try:
            self._log_issuing_request("DictionaryIncrement", {"dictionary_name": dictionary_name})
            _validate_cache_name(cache_name)
            _validate_dictionary_name(dictionary_name)

            request = cache_pb._DictionaryIncrementRequest(
                dictionary_name=_as_bytes(dictionary_name, self.__UNSUPPORTED_DICTIONARY_NAME_TYPE_MSG),
                field=_as_bytes(field, self.__UNSUPPORTED_DICTIONARY_FIELD_TYPE_MSG),
                amount=amount,
                **self._prepare_collection_ttl_for_request(ttl),
            )

            response = self._build_stub().DictionaryIncrement(
                request,
                metadata=make_metadata(cache_name),
                timeout=self._default_deadline_seconds,
            )
            self._log_received_response("DictionaryIncrement", {"dictionary_name": dictionary_name})
            return CacheDictionaryIncrement.Success(response.value)
        except Exception as e:
            self._log_request_error("dictionary_increment", e)
            return CacheDictionaryIncrement.Error(convert_error(e))

    def dictionary_remove_fields(
        self,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
        fields: TDictionaryFields,
    ) -> CacheDictionaryRemoveFieldsResponse:
        try:
            self._log_issuing_request("DictionaryDelete", {"dictionary_name": dictionary_name})
            _validate_cache_name(cache_name)
            _validate_dictionary_name(dictionary_name)

            request = cache_pb._DictionaryDeleteRequest(
                dictionary_name=_as_bytes(dictionary_name, self.__UNSUPPORTED_DICTIONARY_NAME_TYPE_MSG),
                some=cache_pb._DictionaryDeleteRequest.Some(
                    fields=_gen_dictionary_fields_as_bytes(fields, self.__UNSUPPORTED_DICTIONARY_FIELDS_TYPE_MSG)
                ),
            )

            self._build_stub().DictionaryDelete(
                request,
                metadata=make_metadata(cache_name),
                timeout=self._default_deadline_seconds,
            )
            self._log_received_response("DictionaryDelete", {"dictionary_name": dictionary_name})
            return CacheDictionaryRemoveFields.Success()
        except Exception as e:
            self._log_request_error("dictionary_remove_fields", e)
            return CacheDictionaryRemoveFields.Error(convert_error(e))

    def dictionary_set_fields(
        self,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
        items: TDictionaryItems,
        ttl: CollectionTtl = CollectionTtl.from_cache_ttl(),
    ) -> CacheDictionarySetFieldsResponse:
        try:
            self._log_issuing_request("DictionarySet", {"dictionary_name": dictionary_name})
            _validate_cache_name(cache_name)
            _validate_dictionary_name(dictionary_name)

            request = cache_pb._DictionarySetRequest(
                dictionary_name=_as_bytes(dictionary_name, self.__UNSUPPORTED_DICTIONARY_NAME_TYPE_MSG),
                items=[
                    cache_pb._DictionaryFieldValuePair(field=field, value=value)
                    for field, value in _gen_dictionary_items_as_bytes(
                        items, self.__UNSUPPORTED_DICTIONARY_ITEMS_TYPE_MSG
                    )
                ],
                **self._prepare_collection_ttl_for_request(ttl),
            )

            self._build_stub().DictionarySet(
                request,
                metadata=make_metadata(cache_name),
                timeout=self._default_deadline_seconds,
            )
            self._log_received_response("DictionarySet", {"dictionary_name": dictionary_name})
            return CacheDictionarySetFields.Success()
        except Exception as e:
            self._log_request_error("dictionary_set_fields", e)
            return CacheDictionarySetFields.Error(convert_error(e))

    # LIST COLLECTION METHODS
    def list_concatenate_back(
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

            request = cache_pb._ListConcatenateBackRequest(
                list_name=_as_bytes(list_name, self.__UNSUPPORTED_LIST_NAME_TYPE_MSG),
                values=_gen_list_as_bytes(values, self.__UNSUPPORTED_LIST_VALUES_TYPE_MSG),
                truncate_front_to_size=truncate_front_to_size,
                **self._prepare_collection_ttl_for_request(ttl),
            )

            response = self._build_stub().ListConcatenateBack(
                request,
                metadata=make_metadata(cache_name),
                timeout=self._default_deadline_seconds,
            )
            self._log_received_response("ListConcatenateBack", {"list_name": str(request.list_name)})
            return CacheListConcatenateBack.Success(response.list_length)
        except Exception as e:
            self._log_request_error("list_concatenate_back", e)
            return CacheListConcatenateBack.Error(convert_error(e))

    def list_concatenate_front(
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

            request = cache_pb._ListConcatenateFrontRequest(
                list_name=_as_bytes(list_name, self.__UNSUPPORTED_LIST_NAME_TYPE_MSG),
                values=_gen_list_as_bytes(values, self.__UNSUPPORTED_LIST_VALUES_TYPE_MSG),
                truncate_back_to_size=truncate_back_to_size,
                **self._prepare_collection_ttl_for_request(ttl),
            )

            response = self._build_stub().ListConcatenateFront(
                request,
                metadata=make_metadata(cache_name),
                timeout=self._default_deadline_seconds,
            )
            self._log_received_response("ListConcatenateFront", {"list_name": str(request.list_name)})
            return CacheListConcatenateFront.Success(response.list_length)
        except Exception as e:
            self._log_request_error("list_concatenate_front", e)
            return CacheListConcatenateFront.Error(convert_error(e))

    def list_fetch(self, cache_name: TCacheName, list_name: TListName) -> CacheListFetchResponse:
        try:
            self._log_issuing_request("ListFetch", {"list_name": str(list_name)})
            _validate_cache_name(cache_name)
            _validate_list_name(list_name)
            request = cache_pb._ListFetchRequest(list_name=_as_bytes(list_name, self.__UNSUPPORTED_LIST_NAME_TYPE_MSG))
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
                return CacheListFetch.Hit(list(response.found.values))
            else:
                raise UnknownException("Unknown list field")
        except Exception as e:
            self._log_request_error("list_fetch", e)
            return CacheListFetch.Error(convert_error(e))

    def list_length(self, cache_name: TCacheName, list_name: TListName) -> CacheListLengthResponse:
        try:
            self._log_issuing_request("ListLength", {"list_name": str(list_name)})
            _validate_cache_name(cache_name)
            _validate_list_name(list_name)
            request = cache_pb._ListLengthRequest(list_name=_as_bytes(list_name, self.__UNSUPPORTED_LIST_NAME_TYPE_MSG))
            response = self._build_stub().ListLength(
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

    def list_pop_back(self, cache_name: TCacheName, list_name: TListName) -> CacheListPopBackResponse:
        try:
            self._log_issuing_request("ListPopBack", {"list_name": str(list_name)})
            _validate_cache_name(cache_name)
            _validate_list_name(list_name)
            request = cache_pb._ListPopBackRequest(
                list_name=_as_bytes(list_name, self.__UNSUPPORTED_LIST_NAME_TYPE_MSG)
            )
            response = self._build_stub().ListPopBack(
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

    def list_pop_front(self, cache_name: TCacheName, list_name: TListName) -> CacheListPopFrontResponse:
        try:
            self._log_issuing_request("ListPopFront", {"list_name": str(list_name)})
            _validate_cache_name(cache_name)
            _validate_list_name(list_name)
            request = cache_pb._ListPopFrontRequest(
                list_name=_as_bytes(list_name, self.__UNSUPPORTED_LIST_NAME_TYPE_MSG)
            )
            response = self._build_stub().ListPopFront(
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

    def list_push_back(
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

            request = cache_pb._ListPushBackRequest(
                list_name=_as_bytes(list_name, self.__UNSUPPORTED_LIST_NAME_TYPE_MSG),
                value=_as_bytes(value, self.__UNSUPPORTED_LIST_VALUE_TYPE_MSG),
                truncate_front_to_size=truncate_front_to_size,
                **self._prepare_collection_ttl_for_request(ttl),
            )

            response = self._build_stub().ListPushBack(
                request,
                metadata=make_metadata(cache_name),
                timeout=self._default_deadline_seconds,
            )
            self._log_received_response("ListPushBack", {"list_name": str(request.list_name)})
            return CacheListPushBack.Success(response.list_length)
        except Exception as e:
            self._log_request_error("list_push_back", e)
            return CacheListPushBack.Error(convert_error(e))

    def list_push_front(
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

            request = cache_pb._ListPushFrontRequest(
                list_name=_as_bytes(list_name, self.__UNSUPPORTED_LIST_NAME_TYPE_MSG),
                value=_as_bytes(value, self.__UNSUPPORTED_LIST_VALUE_TYPE_MSG),
                truncate_back_to_size=truncate_back_to_size,
                **self._prepare_collection_ttl_for_request(ttl),
            )

            response = self._build_stub().ListPushFront(
                request,
                metadata=make_metadata(cache_name),
                timeout=self._default_deadline_seconds,
            )
            self._log_received_response("ListPushFront", {"list_name": str(request.list_name)})
            return CacheListPushFront.Success(response.list_length)
        except Exception as e:
            self._log_request_error("list_push_front", e)
            return CacheListPushFront.Error(convert_error(e))

    def list_remove_value(
        self,
        cache_name: TCacheName,
        list_name: TListName,
        value: TListValue,
    ) -> CacheListRemoveValueResponse:
        try:
            self._log_issuing_request("ListRemoveValue", {})
            _validate_cache_name(cache_name)
            _validate_list_name(list_name)

            request = cache_pb._ListRemoveRequest(
                list_name=_as_bytes(list_name, self.__UNSUPPORTED_LIST_NAME_TYPE_MSG),
                all_elements_with_value=_as_bytes(value, self.__UNSUPPORTED_LIST_VALUE_TYPE_MSG),
            )

            self._build_stub().ListRemove(
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
    def set_add_elements(
        self,
        cache_name: TCacheName,
        set_name: TSetName,
        elements: TSetElementsInput,
        ttl: CollectionTtl = CollectionTtl.from_cache_ttl(),
    ) -> CacheSetAddElementsResponse:
        try:
            self._log_issuing_request("SetAddElements", {})
            _validate_cache_name(cache_name)
            _validate_set_name(set_name)

            request = cache_pb._SetUnionRequest(
                set_name=_as_bytes(set_name, self.__UNSUPPORTED_SET_NAME_TYPE_MSG),
                elements=_gen_set_input_as_bytes(elements, self.__UNSUPPORTED_SET_ELEMENTS_TYPE_MSG),
                **self._prepare_collection_ttl_for_request(ttl),
            )

            self._build_stub().SetUnion(
                request,
                metadata=make_metadata(cache_name),
                timeout=self._default_deadline_seconds,
            )
            self._log_received_response("SetAddElements", {"set_name": str(request.set_name)})
            return CacheSetAddElements.Success()
        except Exception as e:
            self._log_request_error("set_remove_elements", e)
            return CacheSetAddElements.Error(convert_error(e))

    def set_fetch(
        self,
        cache_name: TCacheName,
        set_name: TSetName,
    ) -> CacheSetFetchResponse:
        try:
            self._log_issuing_request("SetFetch", {"set_name": str(set_name)})
            _validate_cache_name(cache_name)
            _validate_set_name(set_name)

            request = cache_pb._SetFetchRequest(set_name=_as_bytes(set_name, "Unsupported type for set_name: "))
            response = self._build_stub().SetFetch(
                request,
                metadata=make_metadata(cache_name),
                timeout=self._default_deadline_seconds,
            )
            self._log_received_response("SetFetch", {"set_name": str(request.set_name)})

            type = response.WhichOneof("set")
            if type == "missing":
                return CacheSetFetch.Miss()
            elif type == "found":
                return CacheSetFetch.Hit(set(response.found.elements))
            else:
                raise UnknownException(f"Unknown set field in response: {type}")
        except Exception as e:
            self._log_request_error("set_fetch", e)
            return CacheSetFetch.Error(convert_error(e))

    def set_remove_elements(
        self, cache_name: TCacheName, set_name: TSetName, elements: TSetElementsInput
    ) -> CacheSetRemoveElementsResponse:
        try:
            self._log_issuing_request("SetRemoveElements", {})
            _validate_cache_name(cache_name)
            _validate_set_name(set_name)

            request = cache_pb._SetDifferenceRequest(
                set_name=_as_bytes(set_name, self.__UNSUPPORTED_SET_NAME_TYPE_MSG),
                subtrahend=cache_pb._SetDifferenceRequest._Subtrahend(
                    set=cache_pb._SetDifferenceRequest._Subtrahend._Set(
                        elements=_gen_set_input_as_bytes(elements, self.__UNSUPPORTED_SET_ELEMENTS_TYPE_MSG)
                    )
                ),
            )

            self._build_stub().SetDifference(
                request,
                metadata=make_metadata(cache_name),
                timeout=self._default_deadline_seconds,
            )
            self._log_received_response("SetRemoveElements", {"set_name": str(request.set_name)})
            return CacheSetRemoveElements.Success()
        except Exception as e:
            self._log_request_error("set_remove_elements", e)
            return CacheSetRemoveElements.Error(convert_error(e))

    def sorted_set_put_elements(
        self,
        cache_name: TCacheName,
        sorted_set_name: TSetName,
        elements: TSortedSetElements,
        ttl: CollectionTtl = CollectionTtl.from_cache_ttl(),
    ) -> CacheSortedSetPutElementsResponse:
        try:
            self._log_issuing_request("SortedSetPutElements", {})
            _validate_cache_name(cache_name)
            _validate_sorted_set_name(sorted_set_name)

            request = cache_pb._SortedSetPutRequest(
                set_name=_as_bytes(sorted_set_name, self.__UNSUPPORTED_SORTED_SET_NAME_TYPE_MSG),
                **self._prepare_collection_ttl_for_request(ttl),
            )
            for value, score in _gen_sorted_set_elements_as_bytes(
                elements, self.__UNSUPPORTED_SORTED_SET_ELEMENTS_TYPE_MSG
            ):
                _validate_sorted_set_score(score)
                request.elements.append(cache_pb._SortedSetElement(value=value, score=score))

            self._build_stub().SortedSetPut(
                request,
                metadata=make_metadata(cache_name),
                timeout=self._default_deadline_seconds,
            )
            self._log_received_response("SortedSetPutElements", {"sorted_set_name": str(request.set_name)})
            return CacheSortedSetPutElements.Success()
        except Exception as e:
            self._log_request_error("sorted_set_put_elements", e)
            return CacheSortedSetPutElements.Error(convert_error(e))

    def sorted_set_fetch_by_score(
        self,
        cache_name: TCacheName,
        sorted_set_name: TSortedSetName,
        min_score: Optional[float],
        max_score: Optional[float],
        sort_order: SortOrder,
        offset: Optional[int],
        count: Optional[int],
    ) -> CacheSortedSetFetchResponse:
        try:
            self._log_issuing_request("SortedSetFetch", {"sorted_set_name": str(sorted_set_name)})
            _validate_cache_name(cache_name)
            _validate_sorted_set_name(sorted_set_name)

            request = cache_pb._SortedSetFetchRequest(
                set_name=_as_bytes(sorted_set_name, "Unsupported type for set_name: "), with_scores=True
            )

            if min_score is not None:
                request.by_score.min_score = cache_pb._SortedSetFetchRequest._ByScore._Score(score=min_score)
            else:
                request.by_score.unbounded_min.CopyFrom(cache_pb._Unbounded())

            if max_score is not None:
                request.by_score.max_score = cache_pb._SortedSetFetchRequest._ByScore._Score(score=max_score)
            else:
                request.by_score.unbounded_max.CopyFrom(cache_pb._Unbounded())

            if offset is not None:
                request.by_score.offset = offset
            else:
                request.by_score.offset = 0

            if count is not None:
                request.by_score.count = count
            else:
                request.by_score.count = -1

            if sort_order == SortOrder.ASCENDING:
                request.order = cache_pb._SortedSetFetchRequest.ASCENDING
            else:
                request.order = cache_pb._SortedSetFetchRequest.DESCENDING

            response = self._build_stub().SortedSetFetch(
                request,
                metadata=make_metadata(cache_name),
                timeout=self._default_deadline_seconds,
            )
            self._log_received_response("SortedSetFetch", {"sorted_set_name": str(request.set_name)})

            type = response.WhichOneof("sorted_set")
            if type == "missing":
                return CacheSortedSetFetch.Miss()
            elif type == "found":
                return CacheSortedSetFetch.Hit(
                    list((e.value, e.score) for e in response.found.values_with_scores.elements)
                )
            else:
                raise UnknownException(f"Unknown set field in response: {type}")
        except Exception as e:
            self._log_request_error("sorted_set_fetch", e)
            return CacheSortedSetFetch.Error(convert_error(e))

    def sorted_set_fetch_by_rank(
        self,
        cache_name: TCacheName,
        sorted_set_name: TSortedSetName,
        start_rank: Optional[int],
        end_rank: Optional[int],
        sort_order: SortOrder,
    ) -> CacheSortedSetFetchResponse:
        try:
            self._log_issuing_request("SortedSetFetch", {"sorted_set_name": str(sorted_set_name)})
            _validate_cache_name(cache_name)
            _validate_sorted_set_name(sorted_set_name)

            request = cache_pb._SortedSetFetchRequest(
                set_name=_as_bytes(sorted_set_name, "Unsupported type for set_name: "), with_scores=True
            )

            if start_rank is not None:
                request.by_index.inclusive_start_index = start_rank
            else:
                request.by_index.unbounded_start.CopyFrom(cache_pb._Unbounded())

            if end_rank is not None:
                request.by_index.exclusive_end_index = end_rank
            else:
                request.by_index.unbounded_end.CopyFrom(cache_pb._Unbounded())

            if sort_order == SortOrder.ASCENDING:
                request.order = cache_pb._SortedSetFetchRequest.ASCENDING
            else:
                request.order = cache_pb._SortedSetFetchRequest.DESCENDING

            response = self._build_stub().SortedSetFetch(
                request,
                metadata=make_metadata(cache_name),
                timeout=self._default_deadline_seconds,
            )
            self._log_received_response("SortedSetFetch", {"sorted_set_name": str(request.set_name)})

            type = response.WhichOneof("sorted_set")
            if type == "missing":
                return CacheSortedSetFetch.Miss()
            elif type == "found":
                return CacheSortedSetFetch.Hit(
                    list((e.value, e.score) for e in response.found.values_with_scores.elements)
                )
            else:
                raise UnknownException(f"Unknown set field in response: {type}")
        except Exception as e:
            self._log_request_error("sorted_set_fetch", e)
            return CacheSortedSetFetch.Error(convert_error(e))

    def sorted_set_get_scores(
        self, cache_name: TCacheName, sorted_set_name: TSortedSetName, values: TSortedSetValues
    ) -> CacheSortedSetGetScoresResponse:
        try:
            self._log_issuing_request("SortedSetGetScores", {"sorted_set_name": str(sorted_set_name)})
            _validate_cache_name(cache_name)
            _validate_sorted_set_name(sorted_set_name)

            bytes_values = list(_gen_sorted_set_values_as_bytes(values, self.__UNSUPPORTED_SORTED_SET_VALUES_TYPE_MSG))
            request = cache_pb._SortedSetGetScoreRequest(
                set_name=_as_bytes(sorted_set_name, "Unsupported type for sorted_set_name: "), values=bytes_values
            )

            response = self._build_stub().SortedSetGetScore(
                request,
                metadata=make_metadata(cache_name),
                timeout=self._default_deadline_seconds,
            )
            self._log_received_response("SortedSetGetScores", {"sorted_set_name": str(request.set_name)})

            type = response.WhichOneof("sorted_set")
            if type == "found":
                get_responses: list[CacheSortedSetGetScoreResponse] = []
                for value, get_response in zip(bytes_values, response.found.elements):
                    if get_response.result == cache_pb.Miss:
                        get_responses.append(CacheSortedSetGetScore.Miss(value))
                    else:
                        get_responses.append(CacheSortedSetGetScore.Hit(value, get_response.score))
                return CacheSortedSetGetScores.Hit(get_responses)
            elif type == "missing":
                return CacheSortedSetGetScores.Miss()
            else:
                raise UnknownException(f"Unknown field in response: {type}")
        except Exception as e:
            self._log_request_error("sorted_set_get_scores", e)
            return CacheSortedSetGetScores.Error(convert_error(e))

    def sorted_set_get_rank(
        self,
        cache_name: TCacheName,
        sorted_set_name: TSortedSetName,
        value: TSortedSetValue,
        sort_order: SortOrder,
    ) -> CacheSortedSetGetRankResponse:
        try:
            self._log_issuing_request("SortedSetGetRank", {"sorted_set_name": str(sorted_set_name)})
            _validate_cache_name(cache_name)
            _validate_sorted_set_name(sorted_set_name)

            request = cache_pb._SortedSetGetRankRequest(
                set_name=_as_bytes(sorted_set_name, "Unsupported type for sorted_set_name: "),
                value=_as_bytes(value, self.__UNSUPPORTED_SORTED_SET_VALUE_TYPE_MSG),
            )

            if sort_order == SortOrder.ASCENDING:
                request.order = cache_pb._SortedSetGetRankRequest.ASCENDING
            else:
                request.order = cache_pb._SortedSetGetRankRequest.DESCENDING

            response = self._build_stub().SortedSetGetRank(
                request,
                metadata=make_metadata(cache_name),
                timeout=self._default_deadline_seconds,
            )
            self._log_received_response("SortedSetGetRank", {"sorted_set_name": str(request.set_name)})

            if response.element_rank.result == cache_pb.Hit:
                return CacheSortedSetGetRank.Hit(response.element_rank.rank)
            if response.element_rank.result == cache_pb.Miss:
                return CacheSortedSetGetRank.Miss()
            else:
                raise UnknownException(f"Unknown field in response: {type}")
        except Exception as e:
            self._log_request_error("sorted_set_get_rank", e)
            return CacheSortedSetGetRank.Error(convert_error(e))

    def sorted_set_remove_elements(
        self,
        cache_name: TCacheName,
        sorted_set_name: TSortedSetName,
        values: TSortedSetValues,
    ) -> CacheSortedSetRemoveElementsResponse:
        try:
            self._log_issuing_request("SortedSetRemoveElements", {"sorted_set_name": str(sorted_set_name)})
            _validate_cache_name(cache_name)
            _validate_sorted_set_name(sorted_set_name)

            request = cache_pb._SortedSetRemoveRequest(
                set_name=_as_bytes(sorted_set_name, "Unsupported type for sorted_set_name: "),
                some=cache_pb._SortedSetRemoveRequest._Some(
                    values=_gen_sorted_set_values_as_bytes(values, self.__UNSUPPORTED_SORTED_SET_VALUES_TYPE_MSG)
                ),
            )

            self._build_stub().SortedSetRemove(
                request,
                metadata=make_metadata(cache_name),
                timeout=self._default_deadline_seconds,
            )
            self._log_received_response("SortedSetRemoveElements", {"sorted_set_name": str(request.set_name)})

            return CacheSortedSetRemoveElements.Success()
        except Exception as e:
            self._log_request_error("sorted_set_remove_elements", e)
            return CacheSortedSetRemoveElements.Error(convert_error(e))

    def sorted_set_increment(
        self,
        cache_name: TCacheName,
        sorted_set_name: TSortedSetName,
        value: TSortedSetValue,
        score: TSortedSetScore,
        ttl: CollectionTtl = CollectionTtl.from_cache_ttl(),
    ) -> CacheSortedSetIncrementResponse:
        try:
            self._log_issuing_request("SortedSetIncrement", {"sorted_set_name": str(sorted_set_name)})
            _validate_cache_name(cache_name)
            _validate_sorted_set_name(sorted_set_name)
            _validate_sorted_set_score(score)

            request = cache_pb._SortedSetIncrementRequest(
                set_name=_as_bytes(sorted_set_name, "Unsupported type for sorted_set_name: "),
                value=_as_bytes(value),
                amount=score,
                **self._prepare_collection_ttl_for_request(ttl),
            )

            response = self._build_stub().SortedSetIncrement(
                request,
                metadata=make_metadata(cache_name),
                timeout=self._default_deadline_seconds,
            )
            self._log_received_response("SortedSetIncrement", {"sorted_set_name": str(request.set_name)})

            return CacheSortedSetIncrement.Success(response.score)
        except Exception as e:
            self._log_request_error("sorted_set_increment", e)
            return CacheSortedSetIncrement.Error(convert_error(e))

    def _log_received_response(self, request_type: str, request_args: dict[str, str]) -> None:
        self._logger.log(logs.TRACE, f"Received a {request_type} response for {request_args}")

    def _log_issuing_request(self, request_type: str, request_args: dict[str, str]) -> None:
        self._logger.log(logs.TRACE, f"Issuing a {request_type} request with {request_args}")

    def _log_request_error(self, request_type: str, e: Exception) -> None:
        self._logger.warning(f"{request_type} failed with exception: {e}")

    def _prepare_collection_ttl_for_request(self, collection_ttl: CollectionTtl) -> dict[str, Any]:  # type: ignore
        """Converts a CollectionTtl object into a dictionary that can be used as kwargs for a request.

        The TTL is converted to milliseconds, with a default of the client default TTL.
        The refresh TTL is left as is.

        Args:
            collection_ttl (CollectionTtl): The CollectionTtl object to convert.

        Returns:
            dict[str, Any]: The dictionary that can be used as kwargs for a request.
        """
        return {
            "ttl_milliseconds": self._ttl_or_default_milliseconds(collection_ttl.ttl),
            "refresh_ttl": collection_ttl.refresh_ttl,
        }

    def _ttl_or_default_milliseconds(self, ttl: Optional[timedelta]) -> int:
        which_ttl = self._default_ttl
        if ttl is not None:
            which_ttl = ttl

        return int(which_ttl.total_seconds() * 1000)

    def _build_stub(self) -> cache_grpc.ScsStub:
        return self._grpc_manager.stub()

    def close(self) -> None:
        self._grpc_manager.close()

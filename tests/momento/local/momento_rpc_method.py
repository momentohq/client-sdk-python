from enum import Enum
from typing import Optional


class MomentoRpcMethod(Enum):
    GET = ("_GetRequest", "get")
    SET = ("_SetRequest", "set")
    DELETE = ("_DeleteRequest", "delete")
    INCREMENT = ("_IncrementRequest", "increment")
    SET_IF = ("_SetIfRequest", "set-if")
    SET_IF_NOT_EXISTS = ("_SetIfNotExistsRequest", "set-if")
    GET_BATCH = ("_GetBatchRequest", "get-batch")
    SET_BATCH = ("_SetBatchRequest", "set-batch")
    KEYS_EXIST = ("_KeysExistRequest", "keys-exist")
    UPDATE_TTL = ("_UpdateTtlRequest", "update-ttl")
    ITEM_GET_TTL = ("_ItemGetTtlRequest", "item-get-ttl")
    ITEM_GET_TYPE = ("_ItemGetTypeRequest", "item-get-type")
    DICTIONARY_GET = ("_DictionaryGetRequest", "dictionary-get")
    DICTIONARY_FETCH = ("_DictionaryFetchRequest", "dictionary-fetch")
    DICTIONARY_SET = ("_DictionarySetRequest", "dictionary-set")
    DICTIONARY_INCREMENT = ("_DictionaryIncrementRequest", "dictionary-increment")
    DICTIONARY_DELETE = ("_DictionaryDeleteRequest", "dictionary-delete")
    DICTIONARY_LENGTH = ("_DictionaryLengthRequest", "dictionary-length")
    SET_FETCH = ("_SetFetchRequest", "set-fetch")
    SET_SAMPLE = ("_SetSampleRequest", "set-sample")
    SET_UNION = ("_SetUnionRequest", "set-union")
    SET_DIFFERENCE = ("_SetDifferenceRequest", "set-difference")
    SET_CONTAINS = ("_SetContainsRequest", "set-contains")
    SET_LENGTH = ("_SetLengthRequest", "set-length")
    SET_POP = ("_SetPopRequest", "set-pop")
    LIST_PUSH_FRONT = ("_ListPushFrontRequest", "list-push-front")
    LIST_PUSH_BACK = ("_ListPushBackRequest", "list-push-back")
    LIST_POP_FRONT = ("_ListPopFrontRequest", "list-push-front")
    LIST_POP_BACK = ("_ListPopBackRequest", "list-pop-back")
    LIST_ERASE = ("_ListEraseRequest", "list-remove")  # Alias for list-remove
    LIST_REMOVE = ("_ListRemoveRequest", "list-remove")
    LIST_FETCH = ("_ListFetchRequest", "list-fetch")
    LIST_LENGTH = ("_ListLengthRequest", "list-length")
    LIST_CONCATENATE_FRONT = ("_ListConcatenateFrontRequest", "list-concatenate-front")
    LIST_CONCATENATE_BACK = ("_ListConcatenateBackRequest", "list-concatenate-back")
    LIST_RETAIN = ("_ListRetainRequest", "list-retain")
    SORTED_SET_PUT = ("_SortedSetPutRequest", "sorted-set-put")
    SORTED_SET_FETCH = ("_SortedSetFetchRequest", "sorted-set-fetch")
    SORTED_SET_GET_SCORE = ("_SortedSetGetScoreRequest", "sorted-set-get-score")
    SORTED_SET_REMOVE = ("_SortedSetRemoveRequest", "sorted-set-remove")
    SORTED_SET_INCREMENT = ("_SortedSetIncrementRequest", "sorted-set-increment")
    SORTED_SET_GET_RANK = ("_SortedSetGetRankRequest", "sorted-set-get-rank")
    SORTED_SET_LENGTH = ("_SortedSetLengthRequest", "sorted-set-length")
    SORTED_SET_LENGTH_BY_SCORE = ("_SortedSetLengthByScoreRequest", "sorted-set-length-by-score")
    TOPIC_PUBLISH = ("_PublishRequest", "topic-publish")
    TOPIC_SUBSCRIBE = ("_SubscriptionRequest", "topic-subscribe")

    def __init__(self, request_name: str, metadata: str) -> None:
        self._request_name = request_name
        self._metadata = metadata

    @property
    def request_name(self) -> str:
        return self._request_name

    @property
    def metadata(self) -> str:
        return self._metadata

    @classmethod
    def from_request_name(cls, request_name: str) -> Optional["MomentoRpcMethod"]:
        for method in cls:
            if method.request_name == request_name:
                return method
        return None

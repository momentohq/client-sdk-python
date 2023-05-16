"""Momento client response types.

In general a command returns a type with a name ending in "Response", and
you can access the concrete types in the namespace of the name, eg:

- `get` returns `CacheGetResponse` which is one of `CacheGet.Hit`, `CacheGet.Miss`, `CacheGet.Error`
- `set` returns `CacheSetResponse` which is one of `CacheSet.Success`, `CacheSet.Error`
- `delete` returns `CacheDeleteResponse` which is one of `CacheDelete.Success`, `CacheDelete.Error`
- `dictionary_get_field` returns `CacheDictionaryGetFieldResponse` which is one of `CacheDictionaryGetField.Hit`,
    `CacheDictionaryGetField.Miss`, `CacheDictionaryGetField.Error`
- etc
"""

from momento.responses.control.cache.flush import CacheFlush, CacheFlushResponse

from .control.cache.create import CreateCache, CreateCacheResponse
from .control.cache.delete import DeleteCache, DeleteCacheResponse
from .control.cache.list import ListCaches, ListCachesResponse
from .control.signing_key.create import CreateSigningKey, CreateSigningKeyResponse
from .control.signing_key.list import (
    ListSigningKeys,
    ListSigningKeysResponse,
    SigningKey,
)
from .control.signing_key.revoke import RevokeSigningKey, RevokeSigningKeyResponse
from .data.dictionary.fetch import CacheDictionaryFetch, CacheDictionaryFetchResponse
from .data.dictionary.get_field import (
    CacheDictionaryGetField,
    CacheDictionaryGetFieldResponse,
)
from .data.dictionary.get_fields import (
    CacheDictionaryGetFields,
    CacheDictionaryGetFieldsResponse,
)
from .data.dictionary.increment import (
    CacheDictionaryIncrement,
    CacheDictionaryIncrementResponse,
)
from .data.dictionary.remove_field import (
    CacheDictionaryRemoveField,
    CacheDictionaryRemoveFieldResponse,
)
from .data.dictionary.remove_fields import (
    CacheDictionaryRemoveFields,
    CacheDictionaryRemoveFieldsResponse,
)
from .data.dictionary.set_field import (
    CacheDictionarySetField,
    CacheDictionarySetFieldResponse,
)
from .data.dictionary.set_fields import (
    CacheDictionarySetFields,
    CacheDictionarySetFieldsResponse,
)
from .data.list.concatenate_back import (
    CacheListConcatenateBack,
    CacheListConcatenateBackResponse,
)
from .data.list.concatenate_front import (
    CacheListConcatenateFront,
    CacheListConcatenateFrontResponse,
)
from .data.list.fetch import CacheListFetch, CacheListFetchResponse
from .data.list.length import CacheListLength, CacheListLengthResponse
from .data.list.pop_back import CacheListPopBack, CacheListPopBackResponse
from .data.list.pop_front import CacheListPopFront, CacheListPopFrontResponse
from .data.list.push_back import CacheListPushBack, CacheListPushBackResponse
from .data.list.push_front import CacheListPushFront, CacheListPushFrontResponse
from .data.list.remove_value import CacheListRemoveValue, CacheListRemoveValueResponse
from .data.scalar.delete import CacheDelete, CacheDeleteResponse
from .data.scalar.get import CacheGet, CacheGetResponse
from .data.scalar.increment import CacheIncrement, CacheIncrementResponse
from .data.scalar.set import CacheSet, CacheSetResponse
from .data.scalar.set_if_not_exists import (
    CacheSetIfNotExists,
    CacheSetIfNotExistsResponse,
)
from .data.set.add_element import CacheSetAddElement, CacheSetAddElementResponse
from .data.set.add_elements import CacheSetAddElements, CacheSetAddElementsResponse
from .data.set.fetch import CacheSetFetch, CacheSetFetchResponse
from .data.set.remove_element import (
    CacheSetRemoveElement,
    CacheSetRemoveElementResponse,
)
from .data.set.remove_elements import (
    CacheSetRemoveElements,
    CacheSetRemoveElementsResponse,
)
from .data.sorted_set.fetch import CacheSortedSetFetch, CacheSortedSetFetchResponse
from .data.sorted_set.get_rank import (
    CacheSortedSetGetRank,
    CacheSortedSetGetRankResponse,
)
from .data.sorted_set.get_score import (
    CacheSortedSetGetScore,
    CacheSortedSetGetScoreResponse,
)
from .data.sorted_set.get_scores import (
    CacheSortedSetGetScores,
    CacheSortedSetGetScoresResponse,
)
from .data.sorted_set.put_element import (
    CacheSortedSetPutElement,
    CacheSortedSetPutElementResponse,
)
from .data.sorted_set.put_elements import (
    CacheSortedSetPutElements,
    CacheSortedSetPutElementsResponse,
)
from .response import CacheResponse, ControlResponse

__all__ = [
    "CreateCache",
    "CreateCacheResponse",
    "DeleteCache",
    "DeleteCacheResponse",
    "ListCaches",
    "ListCachesResponse",
    "CacheFlush",
    "CacheFlushResponse",
    "CreateSigningKey",
    "CreateSigningKeyResponse",
    "ListSigningKeys",
    "ListSigningKeysResponse",
    "SigningKey",
    "RevokeSigningKey",
    "RevokeSigningKeyResponse",
    "CacheDictionaryFetch",
    "CacheDictionaryFetchResponse",
    "CacheDictionaryGetField",
    "CacheDictionaryGetFieldResponse",
    "CacheDictionaryGetFields",
    "CacheDictionaryGetFieldsResponse",
    "CacheDictionaryIncrement",
    "CacheDictionaryIncrementResponse",
    "CacheDictionaryRemoveField",
    "CacheDictionaryRemoveFieldResponse",
    "CacheDictionaryRemoveFields",
    "CacheDictionaryRemoveFieldsResponse",
    "CacheDictionarySetField",
    "CacheDictionarySetFieldResponse",
    "CacheDictionarySetFields",
    "CacheDictionarySetFieldsResponse",
    "CacheListConcatenateBack",
    "CacheListConcatenateBackResponse",
    "CacheListConcatenateFront",
    "CacheListConcatenateFrontResponse",
    "CacheListFetch",
    "CacheListFetchResponse",
    "CacheListLength",
    "CacheListLengthResponse",
    "CacheListPopBack",
    "CacheListPopBackResponse",
    "CacheListPopFront",
    "CacheListPopFrontResponse",
    "CacheListPushBack",
    "CacheListPushBackResponse",
    "CacheListPushFront",
    "CacheListPushFrontResponse",
    "CacheListRemoveValue",
    "CacheListRemoveValueResponse",
    "CacheDelete",
    "CacheDeleteResponse",
    "CacheGet",
    "CacheGetResponse",
    "CacheIncrement",
    "CacheIncrementResponse",
    "CacheSet",
    "CacheSetResponse",
    "CacheSetIfNotExists",
    "CacheSetIfNotExistsResponse",
    "CacheSetAddElement",
    "CacheSetAddElementResponse",
    "CacheSetAddElements",
    "CacheSetAddElementsResponse",
    "CacheSetFetch",
    "CacheSetFetchResponse",
    "CacheSetRemoveElement",
    "CacheSetRemoveElementResponse",
    "CacheSetRemoveElements",
    "CacheSetRemoveElementsResponse",
    "CacheSortedSetPutElement",
    "CacheSortedSetGetRank",
    "CacheSortedSetGetRankResponse",
    "CacheSortedSetGetScore",
    "CacheSortedSetGetScoreResponse",
    "CacheSortedSetGetScores",
    "CacheSortedSetGetScoresResponse",
    "CacheSortedSetPutElementResponse",
    "CacheSortedSetPutElements",
    "CacheSortedSetPutElementsResponse",
    "CacheSortedSetFetch",
    "CacheSortedSetFetchResponse",
    "CacheResponse",
    "ControlResponse",
]

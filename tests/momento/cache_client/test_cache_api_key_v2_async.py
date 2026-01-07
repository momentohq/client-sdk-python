from momento import CacheClientAsync
from momento.errors import MomentoErrorCode
from momento.requests.sort_order import SortOrder
from momento.responses import (
    CacheFlush,
    CacheGet,
    CacheSet,
    CreateCache,
    DeleteCache,
    ListCaches,
)
from momento.responses.data.dictionary.fetch import CacheDictionaryFetch
from momento.responses.data.dictionary.get_fields import CacheDictionaryGetFields
from momento.responses.data.dictionary.increment import CacheDictionaryIncrement
from momento.responses.data.dictionary.remove_fields import CacheDictionaryRemoveFields
from momento.responses.data.dictionary.set_fields import CacheDictionarySetFields
from momento.responses.data.list.concatenate_back import CacheListConcatenateBack
from momento.responses.data.list.concatenate_front import CacheListConcatenateFront
from momento.responses.data.list.fetch import CacheListFetch
from momento.responses.data.list.length import CacheListLength
from momento.responses.data.list.pop_back import CacheListPopBack
from momento.responses.data.list.pop_front import CacheListPopFront
from momento.responses.data.list.push_back import CacheListPushBack
from momento.responses.data.list.push_front import CacheListPushFront
from momento.responses.data.list.remove_value import CacheListRemoveValue
from momento.responses.data.scalar.delete import CacheDelete
from momento.responses.data.scalar.increment import CacheIncrement
from momento.responses.data.set.add_elements import CacheSetAddElements
from momento.responses.data.set.fetch import CacheSetFetch
from momento.responses.data.set.remove_element import CacheSetRemoveElement
from momento.responses.data.sorted_set.fetch import CacheSortedSetFetch
from momento.responses.data.sorted_set.get_score import CacheSortedSetGetScore
from momento.responses.data.sorted_set.increment_score import CacheSortedSetIncrementScore
from momento.responses.data.sorted_set.put_elements import CacheSortedSetPutElements
from momento.responses.data.sorted_set.remove_element import CacheSortedSetRemoveElement

from tests.conftest import TUniqueCacheNameAsync
from tests.utils import uuid_str

# Control plane


async def test_create_and_delete_cache_succeeds(client_async_v2: CacheClientAsync, cache_name: str) -> None:
    cache_name = uuid_str()

    response = await client_async_v2.create_cache(cache_name)
    assert isinstance(response, CreateCache.Success)

    delete_response = await client_async_v2.delete_cache(cache_name)
    assert isinstance(delete_response, DeleteCache.Success)

    delete_response = await client_async_v2.delete_cache(cache_name)
    assert isinstance(delete_response, DeleteCache.Error)
    assert delete_response.error_code == MomentoErrorCode.NOT_FOUND_ERROR


async def test_flush_cache_succeeds(
    client_async_v2: CacheClientAsync, unique_cache_name_async: TUniqueCacheNameAsync
) -> None:
    cache_name = unique_cache_name_async(client_async_v2)

    create_cache_rsp = await client_async_v2.create_cache(cache_name)
    assert isinstance(create_cache_rsp, CreateCache.Success)

    # set test key
    rsp = await client_async_v2.set(cache_name, "test-key", "test-value")
    assert isinstance(rsp, CacheSet.Success)

    # flush it
    flush_response = await client_async_v2.flush_cache(cache_name)
    assert isinstance(flush_response, CacheFlush.Success)

    # make sure key is gone
    get_rsp = await client_async_v2.get(cache_name, "test-key")
    assert isinstance(get_rsp, CacheGet.Miss)


async def test_list_caches_succeeds(client_async_v2: CacheClientAsync, cache_name: str) -> None:
    cache_name = uuid_str()

    initial_response = await client_async_v2.list_caches()
    assert isinstance(initial_response, ListCaches.Success)

    cache_names = [cache.name for cache in initial_response.caches]
    assert cache_name not in cache_names

    try:
        response = await client_async_v2.create_cache(cache_name)
        assert isinstance(response, CreateCache.Success)

        list_cache_resp = await client_async_v2.list_caches()
        assert isinstance(list_cache_resp, ListCaches.Success)

        cache_names = [cache.name for cache in list_cache_resp.caches]
        assert cache_name in cache_names
    finally:
        delete_response = await client_async_v2.delete_cache(cache_name)
        assert isinstance(delete_response, DeleteCache.Success)


# Scalar methods


async def test_create_get_set_delete_succeeds(
    client_async_v2: CacheClientAsync,
    cache_name: str,
) -> None:
    key = uuid_str()
    value = uuid_str()

    set_resp = await client_async_v2.set(cache_name, key, value)
    assert isinstance(set_resp, CacheSet.Success)

    get_resp = await client_async_v2.get(cache_name, key)
    assert isinstance(get_resp, CacheGet.Hit)
    assert get_resp.value_string == value

    delete_resp = await client_async_v2.delete(cache_name, key)
    assert isinstance(delete_resp, CacheDelete.Success)

    get_after_delete_resp = await client_async_v2.get(cache_name, key)
    assert isinstance(get_after_delete_resp, CacheGet.Miss)


async def test_increment_succeeds(
    client_async_v2: CacheClientAsync,
    cache_name: str,
) -> None:
    key = uuid_str()

    inc_resp1 = await client_async_v2.increment(cache_name, key)
    assert isinstance(inc_resp1, CacheIncrement.Success)
    assert inc_resp1.value == 1

    inc_resp2 = await client_async_v2.increment(cache_name, key)
    assert isinstance(inc_resp2, CacheIncrement.Success)
    assert inc_resp2.value == 2


# Dictionary


async def test_dictionary_set_get_remove_fields(
    client_async_v2: CacheClientAsync,
    cache_name: str,
) -> None:
    key = uuid_str()
    field1 = "field1"
    field2 = "field2"
    value1 = "value1"
    value2 = "value2"

    dict_set_resp = await client_async_v2.dictionary_set_fields(
        cache_name,
        key,
        {field1: value1, field2: value2},
    )
    assert isinstance(dict_set_resp, CacheDictionarySetFields.Success)

    dict_get_resp = await client_async_v2.dictionary_get_fields(
        cache_name,
        key,
        [field1, field2],
    )
    assert isinstance(dict_get_resp, CacheDictionaryGetFields.Hit)
    assert dict_get_resp.value_dictionary_string_string == {field1: value1, field2: value2}

    dict_remove_resp = await client_async_v2.dictionary_remove_fields(
        cache_name,
        key,
        [field1],
    )
    assert isinstance(dict_remove_resp, CacheDictionaryRemoveFields.Success)

    dict_get_after_remove_resp = await client_async_v2.dictionary_get_fields(
        cache_name,
        key,
        [field1, field2],
    )
    assert isinstance(dict_get_after_remove_resp, CacheDictionaryGetFields.Hit)
    assert dict_get_after_remove_resp.value_dictionary_string_string == {field2: value2}


async def test_dictionary_increment_and_fetch(
    client_async_v2: CacheClientAsync,
    cache_name: str,
) -> None:
    key = uuid_str()
    field = "counter"

    dict_inc_resp1 = await client_async_v2.dictionary_increment(cache_name, key, field)
    assert isinstance(dict_inc_resp1, CacheDictionaryIncrement.Success)
    assert dict_inc_resp1.value == 1

    dict_inc_resp2 = await client_async_v2.dictionary_increment(cache_name, key, field)
    assert isinstance(dict_inc_resp2, CacheDictionaryIncrement.Success)
    assert dict_inc_resp2.value == 2

    dict_fetch_resp = await client_async_v2.dictionary_fetch(cache_name, key)
    assert isinstance(dict_fetch_resp, CacheDictionaryFetch.Hit)
    assert dict_fetch_resp.value_dictionary_string_string == {field: "2"}


# List


async def test_list_concatenate_and_length(
    client_async_v2: CacheClientAsync,
    cache_name: str,
) -> None:
    list_name = uuid_str()

    # Concatenate first batch
    concat_resp1 = await client_async_v2.list_concatenate_back(cache_name, list_name, ["a", "b", "c"])
    assert isinstance(concat_resp1, CacheListConcatenateBack.Success)

    # Check length
    length_resp1 = await client_async_v2.list_length(cache_name, list_name)
    assert isinstance(length_resp1, CacheListLength.Hit)
    assert length_resp1.length == 3

    # Concatenate second batch
    concat_resp2 = await client_async_v2.list_concatenate_front(cache_name, list_name, ["d", "e"])
    assert isinstance(concat_resp2, CacheListConcatenateFront.Success)

    # Check length again
    length_resp2 = await client_async_v2.list_length(cache_name, list_name)
    assert isinstance(length_resp2, CacheListLength.Hit)
    assert length_resp2.length == 5


async def test_list_push_and_pop(
    client_async_v2: CacheClientAsync,
    cache_name: str,
) -> None:
    list_name = uuid_str()

    # Push to back
    push_back_resp = await client_async_v2.list_push_back(cache_name, list_name, "x")
    assert isinstance(push_back_resp, CacheListPushBack.Success)

    length_resp = await client_async_v2.list_length(cache_name, list_name)
    assert isinstance(length_resp, CacheListLength.Hit)
    assert length_resp.length == 1

    # Push to front
    push_front_resp = await client_async_v2.list_push_front(cache_name, list_name, "y")
    assert isinstance(push_front_resp, CacheListPushFront.Success)

    length_resp = await client_async_v2.list_length(cache_name, list_name)
    assert isinstance(length_resp, CacheListLength.Hit)
    assert length_resp.length == 2

    # Pop from back
    pop_back_resp = await client_async_v2.list_pop_back(cache_name, list_name)
    assert isinstance(pop_back_resp, CacheListPopBack.Hit)
    assert pop_back_resp.value_string == "x"

    # Pop from front
    pop_front_resp = await client_async_v2.list_pop_front(cache_name, list_name)
    assert isinstance(pop_front_resp, CacheListPopFront.Hit)
    assert pop_front_resp.value_string == "y"


async def test_list_fetch_and_remove_value(
    client_async_v2: CacheClientAsync,
    cache_name: str,
) -> None:
    list_name = uuid_str()

    # Populate list
    populate_resp = await client_async_v2.list_concatenate_back(cache_name, list_name, ["a", "b", "c", "b", "d"])
    assert isinstance(populate_resp, CacheListConcatenateBack.Success)

    # Fetch list
    fetch_resp = await client_async_v2.list_fetch(cache_name, list_name)
    assert isinstance(fetch_resp, CacheListFetch.Hit)
    assert fetch_resp.value_list_string == ["a", "b", "c", "b", "d"]

    # Remove value 'b'
    remove_resp = await client_async_v2.list_remove_value(cache_name, list_name, "b")
    assert isinstance(remove_resp, CacheListRemoveValue.Success)

    # Fetch list again
    fetch_resp_2 = await client_async_v2.list_fetch(cache_name, list_name)
    assert isinstance(fetch_resp_2, CacheListFetch.Hit)
    assert fetch_resp_2.value_list_string == ["a", "c", "d"]


# Set


async def test_set_add_fetch_remove(
    client_async_v2: CacheClientAsync,
    cache_name: str,
) -> None:
    set_name = uuid_str()

    # Add elements
    add_resp = await client_async_v2.set_add_elements(cache_name, set_name, {"one", "two", "three"})
    assert isinstance(add_resp, CacheSetAddElements.Success)

    # Fetch set
    fetch_resp = await client_async_v2.set_fetch(cache_name, set_name)
    assert isinstance(fetch_resp, CacheSetFetch.Hit)
    assert fetch_resp.value_set_string == {"one", "two", "three"}

    # Remove an element
    remove_resp = await client_async_v2.set_remove_element(cache_name, set_name, "two")
    assert isinstance(remove_resp, CacheSetRemoveElement.Success)

    # Fetch set again
    fetch_resp_2 = await client_async_v2.set_fetch(cache_name, set_name)
    assert isinstance(fetch_resp_2, CacheSetFetch.Hit)
    assert fetch_resp_2.value_set_string == {"one", "three"}


# Sorted Set


async def test_sorted_set_put_fetch_remove(client_async_v2: CacheClientAsync, cache_name: str) -> None:
    sorted_set_name = uuid_str()

    # Put elements
    scores = {"a": 10.0, "b": 20.0, "c": 30.0}
    resp = await client_async_v2.sorted_set_put_elements(cache_name, sorted_set_name, scores)
    assert isinstance(resp, CacheSortedSetPutElements.Success)

    # Fetch elements
    fetch_resp = await client_async_v2.sorted_set_fetch_by_score(cache_name, sorted_set_name)
    assert isinstance(fetch_resp, CacheSortedSetFetch.Hit)
    assert fetch_resp.value_list_string == list(scores.items())

    # Remove an element
    remove_resp = await client_async_v2.sorted_set_remove_element(cache_name, sorted_set_name, "b")
    assert isinstance(remove_resp, CacheSortedSetRemoveElement.Success)

    # Fetch elements again
    fetch_resp_2 = await client_async_v2.sorted_set_fetch_by_score(cache_name, sorted_set_name)
    assert isinstance(fetch_resp_2, CacheSortedSetFetch.Hit)
    assert fetch_resp_2.value_list_string == [("a", 10.0), ("c", 30.0)]


async def test_sorted_set_increment_and_get(
    client_async_v2: CacheClientAsync,
    cache_name: str,
) -> None:
    sorted_set_name = uuid_str()
    member = "counter"

    # Increment member
    inc_resp1 = await client_async_v2.sorted_set_increment_score(cache_name, sorted_set_name, member, 5.0)
    assert isinstance(inc_resp1, CacheSortedSetIncrementScore.Success)
    assert inc_resp1.score == 5.0

    inc_resp2 = await client_async_v2.sorted_set_increment_score(cache_name, sorted_set_name, member, 3.0)
    assert isinstance(inc_resp2, CacheSortedSetIncrementScore.Success)
    assert inc_resp2.score == 8.0

    # Get member score
    get_resp = await client_async_v2.sorted_set_get_score(cache_name, sorted_set_name, member)
    assert isinstance(get_resp, CacheSortedSetGetScore.Hit)
    assert get_resp.score == 8.0


async def test_sorted_set_fetch_by_score_and_rank(
    client_async_v2: CacheClientAsync,
    cache_name: str,
) -> None:
    sorted_set_name = uuid_str()

    # Put elements
    scores = {"a": 10.0, "b": 20.0, "c": 30.0, "d": 40.0, "e": 50.0}
    resp = await client_async_v2.sorted_set_put_elements(cache_name, sorted_set_name, scores)
    assert isinstance(resp, CacheSortedSetPutElements.Success)

    # Fetch by rank descending
    fetch_by_rank_resp = await client_async_v2.sorted_set_fetch_by_rank(
        cache_name, sorted_set_name, start_rank=0, end_rank=2, sort_order=SortOrder.DESCENDING
    )
    assert isinstance(fetch_by_rank_resp, CacheSortedSetFetch.Hit)
    assert fetch_by_rank_resp.value_list_string == [("e", 50.0), ("d", 40.0)]

    # Fetch by score with all options
    fetch_by_score_resp = await client_async_v2.sorted_set_fetch_by_score(
        cache_name, sorted_set_name, min_score=15.0, max_score=50.0, sort_order=SortOrder.ASCENDING, offset=1, count=2
    )
    assert isinstance(fetch_by_score_resp, CacheSortedSetFetch.Hit)
    assert fetch_by_score_resp.value_list_string == [("c", 30.0), ("d", 40.0)]

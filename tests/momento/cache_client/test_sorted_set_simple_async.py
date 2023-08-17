"""
The tests here were developed to quickly verify some code fixes in a straightforward manner.
The rspec type test setup was making it essentially impossible for me to understand how to
actually test fetching sorted sets by score in a way that actually triggered the code branch
I was trying to hit.

We can certainly take another stab at figuring out the "right" way to do this in the rspec
style setup, or we can continue to add simplified supplemental tests like this for cases where
the test harness is obtuse and burdensome.
"""
from typing import Dict
from momento import CacheClientAsync
from momento.responses import CacheSortedSetPutElements, CacheSortedSetFetch
from momento.requests import SortOrder


async def _populate_scores(client_async: CacheClientAsync, cache_name: str, sorted_set_name: str) -> Dict[str, float]:
    scores = {"one": 1.0, "ten": 10.0, "twenty": 20.0, "alot": 100.0}
    resp = await client_async.sorted_set_put_elements(cache_name, sorted_set_name, scores)
    assert isinstance(resp, CacheSortedSetPutElements.Success)
    return scores


async def test_sorted_set_fetch_by_score_fetch_all(
    client_async: CacheClientAsync, cache_name: str, sorted_set_name: str
) -> None:
    scores = await _populate_scores(client_async, cache_name, sorted_set_name)
    resp = await client_async.sorted_set_fetch_by_score(cache_name, sorted_set_name)
    if isinstance(resp, CacheSortedSetFetch.Hit):
        assert resp.value_list_string == [(k, v) for k, v in scores.items()]


async def test_sorted_set_fetch_by_score_fetch_all_descending(
    client_async: CacheClientAsync, cache_name: str, sorted_set_name: str
) -> None:
    scores = await _populate_scores(client_async, cache_name, sorted_set_name)
    resp = await client_async.sorted_set_fetch_by_score(cache_name, sorted_set_name, sort_order=SortOrder.DESCENDING)
    if isinstance(resp, CacheSortedSetFetch.Hit):
        expected = [(k, v) for k, v in scores.items()]
        expected.reverse()
        assert resp.value_list_string == expected


async def test_sorted_set_fetch_by_score_with_minmax(
    client_async: CacheClientAsync, cache_name: str, sorted_set_name: str
) -> None:
    scores = await _populate_scores(client_async, cache_name, sorted_set_name)
    resp = await client_async.sorted_set_fetch_by_score(cache_name, sorted_set_name, min_score=10, max_score=99)
    assert isinstance(resp, CacheSortedSetFetch.Hit)
    if isinstance(resp, CacheSortedSetFetch.Hit):
        assert resp.value_list_string == [(k, v) for k, v in scores.items() if 10 <= v <= 99]


async def test_sorted_set_fetch_by_score_with_minmax_descending(
    client_async: CacheClientAsync, cache_name: str, sorted_set_name: str
) -> None:
    scores = await _populate_scores(client_async, cache_name, sorted_set_name)
    resp = await client_async.sorted_set_fetch_by_score(
        cache_name, sorted_set_name, sort_order=SortOrder.DESCENDING, min_score=10, max_score=99
    )
    assert isinstance(resp, CacheSortedSetFetch.Hit)
    if isinstance(resp, CacheSortedSetFetch.Hit):
        expected = [(k, v) for k, v in scores.items() if 10 <= v <= 99]
        expected.reverse()
        assert resp.value_list_string == expected


async def test_sorted_set_fetch_by_score_fetch_all_with_offset(
    client_async: CacheClientAsync, cache_name: str, sorted_set_name: str
) -> None:
    scores = await _populate_scores(client_async, cache_name, sorted_set_name)
    resp = await client_async.sorted_set_fetch_by_score(cache_name, sorted_set_name, offset=2)
    assert isinstance(resp, CacheSortedSetFetch.Hit)
    if isinstance(resp, CacheSortedSetFetch.Hit):
        assert resp.value_list_string == [(k, v) for k, v in scores.items()][2:]


async def test_sorted_set_fetch_by_score_fetch_all_with_offset_descending(
    client_async: CacheClientAsync, cache_name: str, sorted_set_name: str
) -> None:
    scores = await _populate_scores(client_async, cache_name, sorted_set_name)
    resp = await client_async.sorted_set_fetch_by_score(
        cache_name, sorted_set_name, offset=2, sort_order=SortOrder.DESCENDING
    )
    assert isinstance(resp, CacheSortedSetFetch.Hit)
    if isinstance(resp, CacheSortedSetFetch.Hit):
        expected = [(k, v) for k, v in scores.items()][2:]
        expected.reverse()
        assert resp.value_list_string == expected


async def test_sorted_set_fetch_by_score_fetch_all_with_count(
    client_async: CacheClientAsync, cache_name: str, sorted_set_name: str
) -> None:
    scores = await _populate_scores(client_async, cache_name, sorted_set_name)
    resp = await client_async.sorted_set_fetch_by_score(cache_name, sorted_set_name, count=2)
    assert isinstance(resp, CacheSortedSetFetch.Hit)
    if isinstance(resp, CacheSortedSetFetch.Hit):
        assert resp.value_list_string == [(k, v) for k, v in scores.items()][:2]


async def test_sorted_set_fetch_by_score_fetch_all_with_count_descending(
    client_async: CacheClientAsync, cache_name: str, sorted_set_name: str
) -> None:
    scores = await _populate_scores(client_async, cache_name, sorted_set_name)
    resp = await client_async.sorted_set_fetch_by_score(
        cache_name, sorted_set_name, count=2, sort_order=SortOrder.DESCENDING
    )
    assert isinstance(resp, CacheSortedSetFetch.Hit)
    if isinstance(resp, CacheSortedSetFetch.Hit):
        expected = [(k, v) for k, v in scores.items()][:2]
        expected.reverse()
        assert resp.value_list_string == expected


async def test_sorted_set_fetch_by_score_fetch_all_with_offset_and_count(
    client_async: CacheClientAsync, cache_name: str, sorted_set_name: str
) -> None:
    scores = await _populate_scores(client_async, cache_name, sorted_set_name)
    resp = await client_async.sorted_set_fetch_by_score(cache_name, sorted_set_name, offset=1, count=2)
    assert isinstance(resp, CacheSortedSetFetch.Hit)
    if isinstance(resp, CacheSortedSetFetch.Hit):
        assert resp.value_list_string == [(k, v) for k, v in scores.items()][1:3]


async def test_sorted_set_fetch_by_score_fetch_all_with_offset_and_count_descending(
    client_async: CacheClientAsync, cache_name: str, sorted_set_name: str
) -> None:
    scores = await _populate_scores(client_async, cache_name, sorted_set_name)
    resp = await client_async.sorted_set_fetch_by_score(cache_name, sorted_set_name, offset=1, count=2, sort_order=SortOrder.DESCENDING)
    assert isinstance(resp, CacheSortedSetFetch.Hit)
    if isinstance(resp, CacheSortedSetFetch.Hit):
        expected = [(k, v) for k, v in scores.items()][1:3]
        expected.reverse()
        assert resp.value_list_string == expected

from momento.incubating.aio.simple_cache_client import SimpleCacheClientIncubating
from tests.utils import uuid_str


async def test_exists_unary_missing(incubating_client_async: SimpleCacheClientIncubating, cache_name: str):
    key = uuid_str()
    response = await incubating_client_async.exists(cache_name, key)
    assert not response
    assert response.num_exists() == 0
    assert response.results() == [False]
    assert response.missing_keys() == [key]
    assert response.present_keys() == []
    assert list(response.zip_keys_and_results()) == [(key, False)]


async def test_exists_unary_exists(incubating_client_async: SimpleCacheClientIncubating, cache_name: str):
    key, value = uuid_str(), uuid_str()
    await incubating_client_async.set(cache_name, key, value)

    response = await incubating_client_async.exists(cache_name, key)

    assert response
    assert response.num_exists() == 1
    assert response.results() == [True]
    assert response.missing_keys() == []
    assert response.present_keys() == [key]
    assert list(response.zip_keys_and_results()) == [(key, True)]


async def test_exists_multi(incubating_client_async: SimpleCacheClientIncubating, cache_name: str):
    keys = []
    for i in range(3):
        key = uuid_str()
        await incubating_client_async.set(cache_name, key, uuid_str())
        keys.append(key)

    response = await incubating_client_async.exists(cache_name, *keys)
    assert response
    assert response.all()
    assert response.num_exists() == 3
    assert response.results() == [True] * 3
    assert response.missing_keys() == []
    assert response.present_keys() == keys
    assert list(response.zip_keys_and_results()) == list(zip(keys, [True] * 3))

    missing1, missing2 = uuid_str(), uuid_str()
    more_keys = [missing1] + keys + [missing2]
    response = await incubating_client_async.exists(cache_name, *more_keys)
    assert not response
    assert not response.all()
    assert response.num_exists() == 3

    mask = [False] + [True] * 3 + [False]
    assert response.results() == mask

    assert response.missing_keys() == [missing1, missing2]
    assert response.present_keys() == keys
    assert list(response.zip_keys_and_results()) == list(zip(more_keys, mask))
import itertools
import warnings

import pytest

from momento.cache_operation_types import CacheGetStatus
from momento.incubating.cache_operation_types import CacheDictionaryGetUnaryResponse
import momento.incubating.aio.simple_cache_client as simple_cache_client
from momento.incubating.aio.simple_cache_client import SimpleCacheClient
from momento.incubating.aio.utils import convert_dict_items_to_bytes
from tests.utils import uuid_str, uuid_bytes, str_to_bytes


async def test_incubating_warning(
    incubating_client_async: SimpleCacheClient,
    auth_token: str,
    default_ttl_seconds: int,
):
    with pytest.warns(UserWarning):
        warnings.simplefilter("always")
        async with simple_cache_client.init(auth_token, default_ttl_seconds):
            pass


async def test_dictionary_get_miss(
    incubating_client_async: SimpleCacheClient, cache_name: str
):
    get_response = await incubating_client_async.dictionary_get(
        cache_name=cache_name, dictionary_name=uuid_str(), key=uuid_str()
    )
    assert get_response.status() == CacheGetStatus.MISS


async def test_dictionary_get_multi_miss(
    incubating_client_async: SimpleCacheClient, cache_name: str
):
    get_response = await incubating_client_async.dictionary_get_multi(
        cache_name, uuid_str(), uuid_str(), uuid_str(), uuid_str()
    )
    assert len(get_response.to_list()) == 3
    assert all(result == CacheGetStatus.MISS for result in get_response.status())
    assert len(get_response.values()) == 3
    assert all(value is None for value in get_response.values())
    assert len(get_response.values_as_bytes()) == 3
    assert all(value is None for value in get_response.values_as_bytes())


async def test_dictionary_set_response(
    incubating_client_async: SimpleCacheClient, cache_name: str
):
    # Test with key as string
    dictionary = {uuid_str(): uuid_str()}
    dictionary_name = uuid_str()
    set_response = await incubating_client_async.dictionary_set_multi(
        cache_name=cache_name,
        dictionary_name=dictionary_name,
        dictionary=dictionary,
        refresh_ttl=False,
    )
    assert set_response.dictionary_name() == dictionary_name
    assert set_response.dictionary() == dictionary

    # Test as bytes
    dictionary = {uuid_bytes(): uuid_bytes()}
    dictionary_name = uuid_str()
    set_response = await incubating_client_async.dictionary_set_multi(
        cache_name=cache_name,
        dictionary_name=dictionary_name,
        dictionary=dictionary,
        refresh_ttl=False,
    )
    assert set_response.dictionary_name() == dictionary_name
    assert set_response.dictionary_as_bytes() == dictionary


async def test_dictionary_set_unary(
    incubating_client_async: SimpleCacheClient, cache_name: str
):
    dictionary_name = uuid_str()
    key, value = uuid_str(), uuid_str()
    set_response = await incubating_client_async.dictionary_set(
        cache_name=cache_name,
        dictionary_name=dictionary_name,
        key=key,
        value=value,
        refresh_ttl=False,
    )
    assert set_response.dictionary_name() == dictionary_name
    assert set_response.key() == key
    assert set_response.value() == value

    get_response = await incubating_client_async.dictionary_get(
        cache_name=cache_name, dictionary_name=dictionary_name, key=key
    )
    assert get_response.value() == value


async def test_dictionary_set_and_dictionary_get_missing_key(
    incubating_client_async: SimpleCacheClient, cache_name: str
):
    dictionary_name = uuid_str()
    await incubating_client_async.dictionary_set(
        cache_name=cache_name,
        dictionary_name=dictionary_name,
        key=uuid_str(),
        value=uuid_str(),
        refresh_ttl=False,
    )
    get_response = await incubating_client_async.dictionary_get(
        cache_name=cache_name,
        dictionary_name=dictionary_name,
        key=uuid_str(),
    )
    assert get_response.status() == CacheGetStatus.MISS


async def test_dictionary_get_zero_length_keys(
    incubating_client_async: SimpleCacheClient, cache_name: str
):
    with pytest.raises(ValueError):
        await incubating_client_async.dictionary_get_multi(cache_name, uuid_str(), *[])


async def test_dictionary_get_hit(
    incubating_client_async: SimpleCacheClient, cache_name: str
):
    # Test all combinations of type(key) in {str, bytes} and type(value) in {str, bytes}
    for i, (key_is_str, value_is_str) in enumerate(
        itertools.product((True, False), (True, False))
    ):
        key, value = uuid_str(), uuid_str()
        if not key_is_str:
            key = key.encode()
        if not value_is_str:
            value = value.encode()
        dictionary = {key: value}
        dictionary_name = uuid_str()
        await incubating_client_async.dictionary_set_multi(
            cache_name=cache_name,
            dictionary_name=dictionary_name,
            dictionary=dictionary,
            refresh_ttl=False,
        )
        get_response = await incubating_client_async.dictionary_get(
            cache_name=cache_name,
            dictionary_name=dictionary_name,
            key=key,
        )
        assert get_response.status() == CacheGetStatus.HIT
        assert (
            get_response.value() if value_is_str else get_response.value_as_bytes()
        ) == value


async def test_dictionary_get_multi_hit(
    incubating_client_async: SimpleCacheClient, cache_name: str
):
    dictionary_name = uuid_str()
    keys = [uuid_str() for _ in range(3)]
    values = [uuid_str() for _ in range(3)]
    dictionary = {k: v for k, v in zip(keys, values)}
    await incubating_client_async.dictionary_set_multi(
        cache_name=cache_name,
        dictionary_name=dictionary_name,
        dictionary=dictionary,
        refresh_ttl=False,
    )
    get_response = await incubating_client_async.dictionary_get_multi(
        cache_name, dictionary_name, *keys
    )

    assert get_response.values() == values
    assert get_response.values_as_bytes() == [str_to_bytes(i) for i in values]

    results = [CacheGetStatus.HIT] * 3
    assert get_response.status() == results

    individual_responses = [
        CacheDictionaryGetUnaryResponse(value.encode("utf-8"), result)
        for value, result in zip(values, results)
    ]
    assert get_response.to_list() == individual_responses

    get_response = await incubating_client_async.dictionary_get_multi(
        cache_name, dictionary_name, keys[0], keys[1], uuid_str()
    )
    assert get_response.status() == [
        CacheGetStatus.HIT,
        CacheGetStatus.HIT,
        CacheGetStatus.MISS,
    ]
    assert get_response.values() == [values[0], values[1], None]
    assert get_response.values_as_bytes() == [
        str_to_bytes(values[0]),
        str_to_bytes(values[1]),
        None,
    ]


async def test_dictionary_get_all_miss(
    incubating_client_async: SimpleCacheClient, cache_name: str
):
    get_response = await incubating_client_async.dictionary_get_all(
        cache_name=cache_name, dictionary_name=uuid_str()
    )
    assert get_response.status() == CacheGetStatus.MISS


async def test_dictionary_get_all_hit(
    incubating_client_async: SimpleCacheClient, cache_name: str
):
    dictionary_name = uuid_str()
    dictionary = {uuid_str(): uuid_str(), uuid_str(): uuid_str()}
    await incubating_client_async.dictionary_set_multi(
        cache_name=cache_name,
        dictionary_name=dictionary_name,
        dictionary=dictionary,
        refresh_ttl=False,
    )
    get_all_response = await incubating_client_async.dictionary_get_all(
        cache_name=cache_name, dictionary_name=dictionary_name
    )
    assert get_all_response.status() == CacheGetStatus.HIT

    expected = convert_dict_items_to_bytes(dictionary)
    assert get_all_response.value_as_bytes() == expected

    expected = dictionary
    assert get_all_response.value() == expected

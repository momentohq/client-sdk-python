from momento import CacheClientAsync
from momento.errors import MomentoErrorCode
from momento.responses import (
    CacheFlush,
    CacheGet,
    CacheSet,
    CreateCache,
    DeleteCache,
    ListCaches,
)
from tests.conftest import TUniqueCacheNameAsync
from tests.utils import uuid_str


async def test_create_cache_get_set_values_and_delete_cache(
    client_async: CacheClientAsync,
    cache_name: str,
    unique_cache_name_async: TUniqueCacheNameAsync,
) -> None:
    key = uuid_str()
    value = uuid_str()
    new_cache_name = unique_cache_name_async(client_async)

    await client_async.create_cache(new_cache_name)

    set_resp = await client_async.set(new_cache_name, key, value)
    assert isinstance(set_resp, CacheSet.Success)

    get_resp = await client_async.get(new_cache_name, key)
    assert isinstance(get_resp, CacheGet.Hit)
    assert get_resp.value_string == value

    get_for_key_in_some_other_cache = await client_async.get(cache_name, key)
    assert isinstance(get_for_key_in_some_other_cache, CacheGet.Miss)


async def test_create_cache_already_exists_when_creating_existing_cache(
    client_async: CacheClientAsync, cache_name: str
) -> None:
    response = await client_async.create_cache(cache_name)
    assert isinstance(response, CreateCache.CacheAlreadyExists)


async def test_create_cache_throws_exception_for_empty_cache_name(
    client_async: CacheClientAsync,
) -> None:
    response = await client_async.create_cache("")
    assert isinstance(response, CreateCache.Error)
    assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR


async def test_create_cache_throws_validation_exception_for_null_cache_name(
    client_async: CacheClientAsync,
) -> None:
    response = await client_async.create_cache(None)  # type: ignore[arg-type]
    assert isinstance(response, CreateCache.Error)
    assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
    assert response.inner_exception.message == "Cache name must be a string"


async def test_create_cache_with_bad_cache_name_throws_exception(
    client_async: CacheClientAsync,
) -> None:
    response = await client_async.create_cache(1)  # type: ignore[arg-type]
    assert isinstance(response, CreateCache.Error)
    assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
    assert response.inner_exception.message == "Cache name must be a string"


# Delete cache
async def test_delete_cache_succeeds(client_async: CacheClientAsync, cache_name: str) -> None:
    cache_name = uuid_str()

    response = await client_async.create_cache(cache_name)
    assert isinstance(response, CreateCache.Success)

    delete_response = await client_async.delete_cache(cache_name)
    assert isinstance(delete_response, DeleteCache.Success)

    delete_response = await client_async.delete_cache(cache_name)
    assert isinstance(delete_response, DeleteCache.Error)
    assert delete_response.error_code == MomentoErrorCode.NOT_FOUND_ERROR


async def test_delete_cache_throws_not_found_when_deleting_unknown_cache(
    client_async: CacheClientAsync,
) -> None:
    cache_name = uuid_str()
    response = await client_async.delete_cache(cache_name)
    assert isinstance(response, DeleteCache.Error)
    assert response.error_code == MomentoErrorCode.NOT_FOUND_ERROR


async def test_delete_cache_throws_invalid_input_for_null_cache_name(
    client_async: CacheClientAsync,
) -> None:
    response = await client_async.delete_cache(None)  # type: ignore[arg-type]
    assert isinstance(response, DeleteCache.Error)
    assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR


async def test_delete_cache_throws_exception_for_empty_cache_name(
    client_async: CacheClientAsync,
) -> None:
    response = await client_async.delete_cache("")
    assert isinstance(response, DeleteCache.Error)
    assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR


async def test_delete_with_bad_cache_name_throws_exception(client_async: CacheClientAsync) -> None:
    response = await client_async.delete_cache(1)  # type: ignore[arg-type]
    assert isinstance(response, DeleteCache.Error)
    assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
    assert response.inner_exception.message == "Cache name must be a string"


# Flush Cache
async def test_flush_cache_succeeds(
    client_async: CacheClientAsync, unique_cache_name_async: TUniqueCacheNameAsync
) -> None:
    cache_name = unique_cache_name_async(client_async)

    create_cache_rsp = await client_async.create_cache(cache_name)
    assert isinstance(create_cache_rsp, CreateCache.Success)

    # set test key
    rsp = await client_async.set(cache_name, "test-key", "test-value")
    assert isinstance(rsp, CacheSet.Success)

    # flush it
    flush_response = await client_async.flush_cache(cache_name)
    assert isinstance(flush_response, CacheFlush.Success)

    # make sure key is gone
    get_rsp = await client_async.get(cache_name, "test-key")
    assert isinstance(get_rsp, CacheGet.Miss)


async def test_flush_cache_on_non_existent_cache(client_async: CacheClientAsync) -> None:
    cache_name = uuid_str()

    # flush it
    flush_response = await client_async.flush_cache(cache_name)
    assert isinstance(flush_response, CacheFlush.Error)
    assert flush_response.error_code == MomentoErrorCode.NOT_FOUND_ERROR


# List caches
async def test_list_caches_succeeds(client_async: CacheClientAsync, cache_name: str) -> None:
    cache_name = uuid_str()

    initial_response = await client_async.list_caches()
    assert isinstance(initial_response, ListCaches.Success)

    cache_names = [cache.name for cache in initial_response.caches]
    assert cache_name not in cache_names

    try:
        response = await client_async.create_cache(cache_name)
        assert isinstance(response, CreateCache.Success)

        list_cache_resp = await client_async.list_caches()
        assert isinstance(list_cache_resp, ListCaches.Success)

        cache_names = [cache.name for cache in list_cache_resp.caches]
        assert cache_name in cache_names
    finally:
        delete_response = await client_async.delete_cache(cache_name)
        assert isinstance(delete_response, DeleteCache.Success)


# async def test_create_list_revoke_signing_keys(client_async: CacheClientAsync) -> None:
#     create_response = await client_async.create_signing_key(timedelta(minutes=30))
#     assert isinstance(create_response, CreateSigningKey.Success)
#
#     list_response = await client_async.list_signing_keys()
#     assert isinstance(list_response, ListSigningKeys.Success)
#     assert create_response.key_id in [signing_key.key_id for signing_key in list_response.signing_keys]
#
#     revoke_response = await client_async.revoke_signing_key(create_response.key_id)
#     assert isinstance(revoke_response, RevokeSigningKey.Success)
#
#     list_response = await client_async.list_signing_keys()
#     assert isinstance(list_response, ListSigningKeys.Success)
#     assert create_response.key_id not in [signing_key.key_id for signing_key in list_response.signing_keys]

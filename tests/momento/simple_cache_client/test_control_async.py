from datetime import timedelta

import momento.errors as errors
from momento import SimpleCacheClientAsync
from momento.auth import EnvMomentoTokenProvider
from momento.config import Configuration
from momento.errors import MomentoErrorCode
from momento.responses import CacheGet, CacheSet, CreateCache, DeleteCache, ListCaches
from tests.utils import uuid_str


async def test_create_cache_get_set_values_and_delete_cache(
    client_async: SimpleCacheClientAsync, cache_name: str, unique_cache_name_async
) -> None:
    key = uuid_str()
    value = uuid_str()
    unique_cache_name = unique_cache_name_async(client_async)

    await client_async.create_cache(unique_cache_name)

    set_resp = await client_async.set(unique_cache_name, key, value)
    assert isinstance(set_resp, CacheSet.Success)

    get_resp = await client_async.get(unique_cache_name, key)
    assert isinstance(get_resp, CacheGet.Hit)
    assert get_resp.value_string == value

    get_for_key_in_some_other_cache = await client_async.get(cache_name, key)
    assert isinstance(get_for_key_in_some_other_cache, CacheGet.Miss)


async def test_create_cache__already_exists_when_creating_existing_cache(
    client_async: SimpleCacheClientAsync, cache_name: str
) -> None:
    response = await client_async.create_cache(cache_name)
    assert isinstance(response, CreateCache.CacheAlreadyExists)


async def test_create_cache_throws_exception_for_empty_cache_name(
    client_async: SimpleCacheClientAsync,
) -> None:
    response = await client_async.create_cache("")
    assert isinstance(response, CreateCache.Error)
    assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR


async def test_create_cache_throws_validation_exception_for_null_cache_name(
    client_async: SimpleCacheClientAsync,
) -> None:
    response = await client_async.create_cache(None)
    assert isinstance(response, CreateCache.Error)
    assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
    assert response.inner_exception.message == "Cache name must be a non-empty string"


async def test_create_cache_with_bad_cache_name_throws_exception(
    client_async: SimpleCacheClientAsync,
) -> None:
    response = await client_async.create_cache(1)
    assert isinstance(response, CreateCache.Error)
    assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
    assert response.inner_exception.message == "Cache name must be a non-empty string"


async def test_create_cache_throws_authentication_exception_for_bad_token(
    bad_token_credential_provider: EnvMomentoTokenProvider,
    configuration: Configuration,
    default_ttl_seconds: timedelta,
    unique_cache_name_async,
) -> None:
    async with SimpleCacheClientAsync(
        configuration, bad_token_credential_provider, default_ttl_seconds
    ) as client_async:
        unique_cache_name = unique_cache_name_async(client_async)
        response = await client_async.create_cache(unique_cache_name)
        assert isinstance(response, CreateCache.Error)
        assert response.error_code == errors.MomentoErrorCode.AUTHENTICATION_ERROR


# Delete cache
async def test_delete_cache_succeeds(client_async: SimpleCacheClientAsync, cache_name: str) -> None:
    cache_name = uuid_str()

    response = await client_async.create_cache(cache_name)
    assert isinstance(response, CreateCache.Success)

    response = await client_async.delete_cache(cache_name)
    assert isinstance(response, DeleteCache.Success)

    response = await client_async.delete_cache(cache_name)
    assert isinstance(response, DeleteCache.Error)
    assert response.error_code == MomentoErrorCode.NOT_FOUND_ERROR


async def test_delete_cache_throws_not_found_when_deleting_unknown_cache(
    client_async: SimpleCacheClientAsync,
) -> None:
    cache_name = uuid_str()
    response = await client_async.delete_cache(cache_name)
    assert isinstance(response, DeleteCache.Error)
    assert response.error_code == MomentoErrorCode.NOT_FOUND_ERROR


async def test_delete_cache_throws_invalid_input_for_null_cache_name(
    client_async: SimpleCacheClientAsync,
) -> None:
    response = await client_async.delete_cache(None)
    assert isinstance(response, DeleteCache.Error)
    assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR


async def test_delete_cache_throws_exception_for_empty_cache_name(
    client_async: SimpleCacheClientAsync,
) -> None:
    response = await client_async.delete_cache("")
    assert isinstance(response, DeleteCache.Error)
    assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR


async def test_delete_with_bad_cache_name_throws_exception(
    client_async: SimpleCacheClientAsync, cache_name: str
) -> None:
    response = await client_async.delete_cache(1)
    assert isinstance(response, DeleteCache.Error)
    assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
    assert response.inner_exception.message == "Cache name must be a non-empty string"


async def test_delete_cache_throws_authentication_exception_for_bad_token(
    bad_token_credential_provider: EnvMomentoTokenProvider, configuration: Configuration, default_ttl_seconds: timedelta
) -> None:
    async with SimpleCacheClientAsync(
        configuration, bad_token_credential_provider, default_ttl_seconds
    ) as client_async:
        response = await client_async.delete_cache(uuid_str())
        assert isinstance(response, DeleteCache.Error)
        assert response.error_code == MomentoErrorCode.AUTHENTICATION_ERROR


# List caches
async def test_list_caches_succeeds(client_async: SimpleCacheClientAsync, cache_name: str) -> None:
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
        assert list_cache_resp.next_token is None
    finally:
        delete_response = await client_async.delete_cache(cache_name)
        assert isinstance(delete_response, DeleteCache.Success)


async def test_list_caches_throws_authentication_exception_for_bad_token(
    bad_token_credential_provider: EnvMomentoTokenProvider, configuration: Configuration, default_ttl_seconds: timedelta
) -> None:
    async with SimpleCacheClientAsync(
        configuration, bad_token_credential_provider, default_ttl_seconds
    ) as client_async:
        response = await client_async.list_caches()
        assert isinstance(response, ListCaches.Error)
        assert response.error_code == MomentoErrorCode.AUTHENTICATION_ERROR


async def test_list_caches_with_next_token_works(client_async: SimpleCacheClientAsync, cache_name: str) -> None:
    """skip until pagination is actually implemented, see
    https://github.com/momentohq/control-plane-service/issues/83"""
    pass


async def test_create_list_revoke_signing_keys(client_async: SimpleCacheClientAsync) -> None:
    create_resp = await client_async.create_signing_key(timedelta(minutes=30))
    list_resp = await client_async.list_signing_keys()
    assert create_resp.key_id() in [signing_key.key_id() for signing_key in list_resp.signing_keys()]

    await client_async.revoke_signing_key(create_resp.key_id())
    list_resp = await client_async.list_signing_keys()
    assert create_resp.key_id() not in [signing_key.key_id() for signing_key in list_resp.signing_keys()]

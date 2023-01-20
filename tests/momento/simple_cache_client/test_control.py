from datetime import timedelta

import momento.errors as errors
from momento import SimpleCacheClient
from momento.auth.credential_provider import EnvMomentoTokenProvider
from momento.config.configuration import Configuration
from momento.errors import MomentoErrorCode
from momento.responses import (
    CacheGetResponse,
    CacheSetResponse,
    CreateCacheResponse,
    DeleteCacheResponse,
    ListCachesResponse,
)
from tests.utils import unique_test_cache_name, uuid_str


def test_create_cache_get_set_values_and_delete_cache(client: SimpleCacheClient, cache_name: str) -> None:
    random_cache_name = unique_test_cache_name()
    key = uuid_str()
    value = uuid_str()

    client.create_cache(random_cache_name)

    set_resp = client.set(random_cache_name, key, value)
    assert isinstance(set_resp, CacheSetResponse.Success)

    get_resp = client.get(random_cache_name, key)
    assert isinstance(get_resp, CacheGetResponse.Hit)
    assert get_resp.value_string == value

    get_for_key_in_some_other_cache = client.get(cache_name, key)
    assert isinstance(get_for_key_in_some_other_cache, CacheGetResponse.Miss)


def test_create_cache__already_exists_when_creating_existing_cache(client: SimpleCacheClient, cache_name: str) -> None:
    response = client.create_cache(cache_name)
    assert isinstance(response, CreateCacheResponse.CacheAlreadyExists)


def test_create_cache_throws_exception_for_empty_cache_name(
    client: SimpleCacheClient,
) -> None:
    response = client.create_cache("")
    assert isinstance(response, CreateCacheResponse.Error)
    assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR


def test_create_cache_throws_validation_exception_for_null_cache_name(
    client: SimpleCacheClient,
) -> None:
    response = client.create_cache(None)
    assert isinstance(response, CreateCacheResponse.Error)
    assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
    assert response.inner_exception.message == "Cache name must be a non-empty string"


def test_create_cache_with_bad_cache_name_throws_exception(
    client: SimpleCacheClient,
) -> None:
    response = client.create_cache(1)
    assert isinstance(response, CreateCacheResponse.Error)
    assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
    assert response.inner_exception.message == "Cache name must be a non-empty string"


def test_create_cache_throws_authentication_exception_for_bad_token(
    bad_token_credential_provider: EnvMomentoTokenProvider, configuration: Configuration, default_ttl_seconds: timedelta
) -> None:
    with SimpleCacheClient(configuration, bad_token_credential_provider, default_ttl_seconds) as client:
        response = client.create_cache(unique_test_cache_name())
        assert isinstance(response, CreateCacheResponse.Error)
        assert response.error_code == errors.MomentoErrorCode.AUTHENTICATION_ERROR


# Delete cache
def test_delete_cache_succeeds(client: SimpleCacheClient, cache_name: str) -> None:
    cache_name = uuid_str()

    response = client.create_cache(cache_name)
    assert isinstance(response, CreateCacheResponse.Success)

    response = client.delete_cache(cache_name)
    assert isinstance(response, DeleteCacheResponse.Success)

    response = client.delete_cache(cache_name)
    assert isinstance(response, DeleteCacheResponse.Error)
    assert response.error_code == MomentoErrorCode.NOT_FOUND_ERROR


def test_delete_cache_throws_not_found_when_deleting_unknown_cache(
    client: SimpleCacheClient,
) -> None:
    cache_name = uuid_str()
    response = client.delete_cache(cache_name)
    assert isinstance(response, DeleteCacheResponse.Error)
    assert response.error_code == MomentoErrorCode.NOT_FOUND_ERROR


def test_delete_cache_throws_invalid_input_for_null_cache_name(
    client: SimpleCacheClient,
) -> None:
    response = client.delete_cache(None)
    assert isinstance(response, DeleteCacheResponse.Error)
    assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR


def test_delete_cache_throws_exception_for_empty_cache_name(
    client: SimpleCacheClient,
) -> None:
    response = client.delete_cache("")
    assert isinstance(response, DeleteCacheResponse.Error)
    assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR


def test_delete_with_bad_cache_name_throws_exception(client: SimpleCacheClient, cache_name: str) -> None:
    response = client.delete_cache(1)
    assert isinstance(response, DeleteCacheResponse.Error)
    assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
    assert response.inner_exception.message == "Cache name must be a non-empty string"


def test_delete_cache_throws_authentication_exception_for_bad_token(
    bad_token_credential_provider: EnvMomentoTokenProvider, configuration: Configuration, default_ttl_seconds: timedelta
) -> None:
    with SimpleCacheClient(configuration, bad_token_credential_provider, default_ttl_seconds) as client:
        response = client.delete_cache(uuid_str())
        assert isinstance(response, DeleteCacheResponse.Error)
        assert response.error_code == MomentoErrorCode.AUTHENTICATION_ERROR


# List caches
def test_list_caches_succeeds(client: SimpleCacheClient, cache_name: str) -> None:
    cache_name = uuid_str()

    initial_response = client.list_caches()
    assert isinstance(initial_response, ListCachesResponse.Success)

    cache_names = [cache.name for cache in initial_response.caches]
    assert cache_name not in cache_names

    try:
        response = client.create_cache(cache_name)
        assert isinstance(response, CreateCacheResponse.Success)

        list_cache_resp = client.list_caches()
        assert isinstance(list_cache_resp, ListCachesResponse.Success)

        cache_names = [cache.name for cache in list_cache_resp.caches]
        assert cache_name in cache_names
        assert list_cache_resp.next_token is None
    finally:
        delete_response = client.delete_cache(cache_name)
        assert isinstance(delete_response, DeleteCacheResponse.Success)


def test_list_caches_throws_authentication_exception_for_bad_token(
    bad_token_credential_provider: EnvMomentoTokenProvider, configuration: Configuration, default_ttl_seconds: timedelta
) -> None:
    with SimpleCacheClient(configuration, bad_token_credential_provider, default_ttl_seconds) as client:
        response = client.list_caches()
        assert isinstance(response, ListCachesResponse.Error)
        assert response.error_code == MomentoErrorCode.AUTHENTICATION_ERROR


def test_list_caches_with_next_token_works(client: SimpleCacheClient, cache_name: str) -> None:
    """skip until pagination is actually implemented, see
    https://github.com/momentohq/control-plane-service/issues/83"""
    pass


# Signing keys
def test_create_list_revoke_signing_keys(client: SimpleCacheClient) -> None:
    create_resp = client.create_signing_key(timedelta(minutes=30))
    list_resp = client.list_signing_keys()
    assert create_resp.key_id() in [signing_key.key_id() for signing_key in list_resp.signing_keys()]

    client.revoke_signing_key(create_resp.key_id())
    list_resp = client.list_signing_keys()
    assert create_resp.key_id() not in [signing_key.key_id() for signing_key in list_resp.signing_keys()]

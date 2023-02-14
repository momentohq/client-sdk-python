from datetime import timedelta

import momento.errors as errors
from momento import SimpleCacheClient
from momento.auth import CredentialProvider
from momento.config import Configuration
from momento.errors import MomentoErrorCode
from momento.responses import CacheGet, CacheSet, CreateCache, DeleteCache, ListCaches
from tests.conftest import TUniqueCacheName
from tests.utils import uuid_str


def test_create_cache_get_set_values_and_delete_cache(
    client: SimpleCacheClient,
    cache_name: str,
    unique_cache_name: TUniqueCacheName,
) -> None:
    key = uuid_str()
    value = uuid_str()
    new_cache_name = unique_cache_name(client)

    client.create_cache(new_cache_name)

    set_resp = client.set(new_cache_name, key, value)
    assert isinstance(set_resp, CacheSet.Success)

    get_resp = client.get(new_cache_name, key)
    assert isinstance(get_resp, CacheGet.Hit)
    assert get_resp.value_string == value

    get_for_key_in_some_other_cache = client.get(cache_name, key)
    assert isinstance(get_for_key_in_some_other_cache, CacheGet.Miss)


def test_create_cache__already_exists_when_creating_existing_cache(client: SimpleCacheClient, cache_name: str) -> None:
    response = client.create_cache(cache_name)
    assert isinstance(response, CreateCache.CacheAlreadyExists)


def test_create_cache_throws_exception_for_empty_cache_name(
    client: SimpleCacheClient,
) -> None:
    response = client.create_cache("")
    assert isinstance(response, CreateCache.Error)
    assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR


def test_create_cache_throws_validation_exception_for_null_cache_name(
    client: SimpleCacheClient,
) -> None:
    response = client.create_cache(None)  # type: ignore[arg-type]
    assert isinstance(response, CreateCache.Error)
    assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
    assert response.inner_exception.message == "Cache name must be a string"


def test_create_cache_with_bad_cache_name_throws_exception(
    client: SimpleCacheClient,
) -> None:
    response = client.create_cache(1)  # type: ignore[arg-type]
    assert isinstance(response, CreateCache.Error)
    assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
    assert response.inner_exception.message == "Cache name must be a string"


def test_create_cache_throws_authentication_exception_for_bad_token(
    bad_token_credential_provider: CredentialProvider,
    configuration: Configuration,
    default_ttl_seconds: timedelta,
    unique_cache_name: TUniqueCacheName,
) -> None:
    with SimpleCacheClient(configuration, bad_token_credential_provider, default_ttl_seconds) as client:
        new_cache_name = unique_cache_name(client)
        response = client.create_cache(new_cache_name)
        assert isinstance(response, CreateCache.Error)
        assert response.error_code == errors.MomentoErrorCode.AUTHENTICATION_ERROR


# Delete cache
def test_delete_cache_succeeds(client: SimpleCacheClient, cache_name: str) -> None:
    cache_name = uuid_str()

    response = client.create_cache(cache_name)
    assert isinstance(response, CreateCache.Success)

    delete_response = client.delete_cache(cache_name)
    assert isinstance(delete_response, DeleteCache.Success)

    delete_response = client.delete_cache(cache_name)
    assert isinstance(delete_response, DeleteCache.Error)
    assert delete_response.error_code == MomentoErrorCode.NOT_FOUND_ERROR


def test_delete_cache_throws_not_found_when_deleting_unknown_cache(
    client: SimpleCacheClient,
) -> None:
    cache_name = uuid_str()
    response = client.delete_cache(cache_name)
    assert isinstance(response, DeleteCache.Error)
    assert response.error_code == MomentoErrorCode.NOT_FOUND_ERROR


def test_delete_cache_throws_invalid_input_for_null_cache_name(
    client: SimpleCacheClient,
) -> None:
    response = client.delete_cache(None)  # type: ignore[arg-type]
    assert isinstance(response, DeleteCache.Error)
    assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR


def test_delete_cache_throws_exception_for_empty_cache_name(
    client: SimpleCacheClient,
) -> None:
    response = client.delete_cache("")
    assert isinstance(response, DeleteCache.Error)
    assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR


def test_delete_with_bad_cache_name_throws_exception(client: SimpleCacheClient) -> None:
    response = client.delete_cache(1)  # type: ignore[arg-type]
    assert isinstance(response, DeleteCache.Error)
    assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
    assert response.inner_exception.message == "Cache name must be a string"


def test_delete_cache_throws_authentication_exception_for_bad_token(
    bad_token_credential_provider: CredentialProvider, configuration: Configuration, default_ttl_seconds: timedelta
) -> None:
    with SimpleCacheClient(configuration, bad_token_credential_provider, default_ttl_seconds) as client:
        response = client.delete_cache(uuid_str())
        assert isinstance(response, DeleteCache.Error)
        assert response.error_code == MomentoErrorCode.AUTHENTICATION_ERROR


# List caches
def test_list_caches_succeeds(client: SimpleCacheClient, cache_name: str) -> None:
    cache_name = uuid_str()

    initial_response = client.list_caches()
    assert isinstance(initial_response, ListCaches.Success)

    cache_names = [cache.name for cache in initial_response.caches]
    assert cache_name not in cache_names

    try:
        response = client.create_cache(cache_name)
        assert isinstance(response, CreateCache.Success)

        list_cache_resp = client.list_caches()
        assert isinstance(list_cache_resp, ListCaches.Success)

        cache_names = [cache.name for cache in list_cache_resp.caches]
        assert cache_name in cache_names
        assert list_cache_resp.next_token is None
    finally:
        delete_response = client.delete_cache(cache_name)
        assert isinstance(delete_response, DeleteCache.Success)


def test_list_caches_throws_authentication_exception_for_bad_token(
    bad_token_credential_provider: CredentialProvider, configuration: Configuration, default_ttl_seconds: timedelta
) -> None:
    with SimpleCacheClient(configuration, bad_token_credential_provider, default_ttl_seconds) as client:
        response = client.list_caches()
        assert isinstance(response, ListCaches.Error)
        assert response.error_code == MomentoErrorCode.AUTHENTICATION_ERROR


def test_list_caches_with_next_token_works() -> None:
    """skip until pagination is actually implemented, see
    https://github.com/momentohq/control-plane-service/issues/83"""
    pass


def test_create_list_revoke_signing_keys(client: SimpleCacheClient) -> None:
    create_resp = client.create_signing_key(timedelta(minutes=30))
    list_resp = client.list_signing_keys()
    assert create_resp.key_id in [signing_key.key_id for signing_key in list_resp.signing_keys]

    client.revoke_signing_key(create_resp.key_id)
    list_resp = client.list_signing_keys()
    assert create_resp.key_id not in [signing_key.key_id for signing_key in list_resp.signing_keys]

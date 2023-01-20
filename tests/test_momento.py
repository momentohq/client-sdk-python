import os
import time
from datetime import timedelta

import pytest

import momento.errors as errors
from momento import SimpleCacheClient
from momento.auth.credential_provider import CredentialProvider, EnvMomentoTokenProvider
from momento.config.configuration import Configuration
from momento.errors import InvalidArgumentException, MomentoErrorCode
from momento.responses import (
    CacheDelete,
    CacheGet,
    CacheSet,
    CreateCache,
    DeleteCache,
    ListCaches,
)
from tests.utils import str_to_bytes, unique_test_cache_name, uuid_bytes, uuid_str


def test_create_cache_get_set_values_and_delete_cache(client: SimpleCacheClient, cache_name: str) -> None:
    random_cache_name = unique_test_cache_name()
    key = uuid_str()
    value = uuid_str()

    client.create_cache(random_cache_name)

    set_resp = client.set(random_cache_name, key, value)
    assert isinstance(set_resp, CacheSet.Success)

    get_resp = client.get(random_cache_name, key)
    assert isinstance(get_resp, CacheGet.Hit)
    assert get_resp.value_string == value

    get_for_key_in_some_other_cache = client.get(cache_name, key)
    assert isinstance(get_for_key_in_some_other_cache, CacheGet.Miss)

    client.delete_cache(random_cache_name)


# Init
def test_init_throws_exception_when_client_uses_negative_default_ttl(
    configuration: Configuration, credential_provider: CredentialProvider
) -> None:
    with pytest.raises(InvalidArgumentException, match="TTL timedelta must be a non-negative integer"):
        SimpleCacheClient(configuration, credential_provider, timedelta(seconds=-1))


def test_init_throws_exception_for_non_jwt_token(configuration: Configuration, default_ttl_seconds: timedelta) -> None:
    with pytest.raises(InvalidArgumentException, match="Invalid Auth token."):
        os.environ["BAD_AUTH_TOKEN"] = "notanauthtoken"
        credential_provider = EnvMomentoTokenProvider("BAD_AUTH_TOKEN")
        SimpleCacheClient(configuration, credential_provider, default_ttl_seconds)


def test_init_throws_exception_when_client_uses_integer_request_timeout_ms(
    configuration: Configuration, credential_provider: CredentialProvider, default_ttl_seconds: int
) -> None:
    with pytest.raises(
        InvalidArgumentException, match="Request timeout must be a timedelta with a value greater " "than zero."
    ):
        configuration.with_client_timeout(-1)


def test_init_throws_exception_when_client_uses_negative_request_timeout_ms(
    configuration: Configuration, credential_provider: CredentialProvider, default_ttl_seconds: timedelta
) -> None:
    with pytest.raises(
        InvalidArgumentException, match="Request timeout must be a timedelta with a value greater than zero."
    ):
        configuration = configuration.with_client_timeout(timedelta(seconds=-1))
        SimpleCacheClient(configuration, credential_provider, default_ttl_seconds)


def test_init_throws_exception_when_client_uses_zero_request_timeout_ms(
    configuration: Configuration, credential_provider: CredentialProvider, default_ttl_seconds: timedelta
) -> None:
    with pytest.raises(
        InvalidArgumentException, match="Request timeout must be a timedelta with a value greater than zero."
    ):
        configuration = configuration.with_client_timeout(timedelta(seconds=0))
        SimpleCacheClient(configuration, credential_provider, default_ttl_seconds)


# Create cache
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
    response = client.create_cache(None)
    assert isinstance(response, CreateCache.Error)
    assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
    assert response.inner_exception.message == "Cache name must be a non-empty string"


def test_create_cache_with_bad_cache_name_throws_exception(
    client: SimpleCacheClient,
) -> None:
    response = client.create_cache(1)
    assert isinstance(response, CreateCache.Error)
    assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
    assert response.inner_exception.message == "Cache name must be a non-empty string"


def test_create_cache_throws_authentication_exception_for_bad_token(
    bad_token_credential_provider: EnvMomentoTokenProvider, configuration: Configuration, default_ttl_seconds: timedelta
) -> None:
    with SimpleCacheClient(configuration, bad_token_credential_provider, default_ttl_seconds) as client:
        response = client.create_cache(unique_test_cache_name())
        assert isinstance(response, CreateCache.Error)
        assert response.error_code == errors.MomentoErrorCode.AUTHENTICATION_ERROR


# Delete cache
def test_delete_cache_succeeds(client: SimpleCacheClient, cache_name: str) -> None:
    cache_name = uuid_str()

    response = client.create_cache(cache_name)
    assert isinstance(response, CreateCache.Success)

    response = client.delete_cache(cache_name)
    assert isinstance(response, DeleteCache.Success)

    response = client.delete_cache(cache_name)
    assert isinstance(response, DeleteCache.Error)
    assert response.error_code == MomentoErrorCode.NOT_FOUND_ERROR


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
    response = client.delete_cache(None)
    assert isinstance(response, DeleteCache.Error)
    assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR


def test_delete_cache_throws_exception_for_empty_cache_name(
    client: SimpleCacheClient,
) -> None:
    response = client.delete_cache("")
    assert isinstance(response, DeleteCache.Error)
    assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR


def test_delete_with_bad_cache_name_throws_exception(client: SimpleCacheClient, cache_name: str) -> None:
    response = client.delete_cache(1)
    assert isinstance(response, DeleteCache.Error)
    assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
    assert response.inner_exception.message == "Cache name must be a non-empty string"


def test_delete_cache_throws_authentication_exception_for_bad_token(
    bad_token_credential_provider: EnvMomentoTokenProvider, configuration: Configuration, default_ttl_seconds: timedelta
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
    bad_token_credential_provider: EnvMomentoTokenProvider, configuration: Configuration, default_ttl_seconds: timedelta
) -> None:
    with SimpleCacheClient(configuration, bad_token_credential_provider, default_ttl_seconds) as client:
        response = client.list_caches()
        assert isinstance(response, ListCaches.Error)
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


# Setting and Getting
def test_set_and_get_with_hit(client: SimpleCacheClient, cache_name: str) -> None:
    key = uuid_str()
    value = uuid_str()

    set_resp = client.set(cache_name, key, value)
    assert isinstance(set_resp, CacheSet.Success)

    get_resp = client.get(cache_name, key)
    isinstance(get_resp, CacheGet.Hit)
    assert get_resp.value_string == value
    assert get_resp.value_bytes == str_to_bytes(value)


def test_set_and_get_with_byte_key_values(client: SimpleCacheClient, cache_name: str) -> None:
    key = uuid_bytes()
    value = uuid_bytes()

    set_resp = client.set(cache_name, key, value)
    assert isinstance(set_resp, CacheSet.Success)

    get_resp = client.get(cache_name, key)
    assert isinstance(get_resp, CacheGet.Hit)
    assert get_resp.value_bytes == value


def test_get_returns_miss(client: SimpleCacheClient, cache_name: str) -> None:
    key = uuid_str()

    get_resp = client.get(cache_name, key)
    assert isinstance(get_resp, CacheGet.Miss)


def test_expires_items_after_ttl(client: SimpleCacheClient, cache_name: str) -> None:
    key = uuid_str()
    val = uuid_str()

    client.set(cache_name, key, val, timedelta(seconds=2))
    get_response = client.get(cache_name, key)
    assert isinstance(get_response, CacheGet.Hit)

    time.sleep(4)
    get_response = client.get(cache_name, key)
    assert isinstance(get_response, CacheGet.Miss)


def test_set_with_different_ttl(client: SimpleCacheClient, cache_name: str) -> None:
    key1 = uuid_str()
    key2 = uuid_str()

    client.set(cache_name, key1, "1", timedelta(seconds=2))
    client.set(cache_name, key2, "2")

    # Before
    get_response = client.get(cache_name, key1)
    assert isinstance(get_response, CacheGet.Hit)
    get_response = client.get(cache_name, key2)
    assert isinstance(get_response, CacheGet.Hit)

    time.sleep(4)

    # After
    get_response = client.get(cache_name, key1)
    assert isinstance(get_response, CacheGet.Miss)
    get_response = client.get(cache_name, key2)
    assert isinstance(get_response, CacheGet.Hit)


# Set
def test_set_with_non_existent_cache_name_throws_not_found(
    client: SimpleCacheClient,
) -> None:
    cache_name = uuid_str()
    set_response = client.set(cache_name, "foo", "bar")
    assert isinstance(set_response, CacheSet.Error)
    assert set_response.error_code == MomentoErrorCode.NOT_FOUND_ERROR


def test_set_with_null_cache_name_throws_exception(client: SimpleCacheClient, cache_name: str) -> None:
    set_response = client.set(None, "foo", "bar")
    assert isinstance(set_response, CacheSet.Error)
    assert set_response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
    assert set_response.inner_exception.message == "Cache name must be a non-empty string"


def test_set_with_empty_cache_name_throws_exception(
    client: SimpleCacheClient,
) -> None:
    set_response = client.set("", "foo", "bar")
    assert isinstance(set_response, CacheSet.Error)
    assert set_response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
    assert set_response.inner_exception.message == "Cache header is empty"


def test_set_with_null_key_throws_exception(client: SimpleCacheClient, cache_name: str) -> None:
    set_response = client.set(cache_name, None, "bar")
    assert isinstance(set_response, CacheSet.Error)
    assert set_response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR


def test_set_with_null_value_throws_exception(client: SimpleCacheClient, cache_name: str) -> None:
    set_response = client.set(cache_name, "foo", None)
    assert isinstance(set_response, CacheSet.Error)
    assert set_response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR


def test_set_negative_ttl_throws_exception(client: SimpleCacheClient, cache_name: str) -> None:
    set_response = client.set(cache_name, "foo", "bar", timedelta(seconds=-1))
    assert isinstance(set_response, CacheSet.Error)
    assert set_response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
    assert set_response.inner_exception.message == "TTL timedelta must be a non-negative integer"


def test_set_with_bad_cache_name_throws_exception(
    client: SimpleCacheClient,
) -> None:
    set_response = client.set(1, "foo", "bar")
    assert isinstance(set_response, CacheSet.Error)
    assert set_response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
    assert set_response.inner_exception.message == "Cache name must be a non-empty string"


def test_set_with_bad_key_throws_exception(client: SimpleCacheClient, cache_name: str) -> None:
    set_response = client.set(cache_name, 1, "bar")
    assert isinstance(set_response, CacheSet.Error)
    assert set_response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
    assert set_response.inner_exception.message == "Unsupported type for key: <class 'int'>"


def test_set_with_bad_value_throws_exception(client: SimpleCacheClient, cache_name: str) -> None:
    set_response = client.set(cache_name, "foo", 1)
    assert isinstance(set_response, CacheSet.Error)
    assert set_response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
    assert set_response.inner_exception.message == "Unsupported type for value: <class 'int'>"


def test_set_throws_authentication_exception_for_bad_token(
    bad_token_credential_provider: EnvMomentoTokenProvider,
    configuration: Configuration,
    cache_name: str,
    default_ttl_seconds: timedelta,
) -> None:
    with SimpleCacheClient(configuration, bad_token_credential_provider, default_ttl_seconds) as client:
        set_response = client.set(cache_name, "foo", "bar")
        assert isinstance(set_response, CacheSet.Error)
        assert set_response.error_code == MomentoErrorCode.AUTHENTICATION_ERROR


def test_set_throws_timeout_error_for_short_request_timeout(
    configuration: Configuration,
    credential_provider: EnvMomentoTokenProvider,
    cache_name: str,
    default_ttl_seconds: timedelta,
) -> None:
    configuration = configuration.with_client_timeout(timedelta(milliseconds=1))
    with SimpleCacheClient(configuration, credential_provider, default_ttl_seconds) as client:
        set_response = client.set(cache_name, "foo", "bar")
        assert isinstance(set_response, CacheSet.Error)
        assert set_response.error_code == MomentoErrorCode.TIMEOUT_ERROR


# Get
def test_get_with_non_existent_cache_name_throws_not_found(
    client: SimpleCacheClient,
) -> None:
    cache_name = uuid_str()
    get_response = client.get(cache_name, "foo")
    assert isinstance(get_response, CacheGet.Error)
    assert get_response.error_code == MomentoErrorCode.NOT_FOUND_ERROR


def test_get_with_null_cache_name_throws_exception(
    client: SimpleCacheClient,
) -> None:
    get_response = client.get(None, "foo")
    assert isinstance(get_response, CacheGet.Error)
    assert get_response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
    assert get_response.inner_exception.message == "Cache name must be a non-empty string"


def test_get_with_empty_cache_name_throws_exception(
    client: SimpleCacheClient,
) -> None:
    get_response = client.get("", "foo")
    assert isinstance(get_response, CacheGet.Error)
    assert get_response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
    assert get_response.inner_exception.message == "Cache header is empty"


def test_get_with_null_key_throws_exception(client: SimpleCacheClient, cache_name: str) -> None:
    get_response = client.get(cache_name, None)
    assert isinstance(get_response, CacheGet.Error)
    assert get_response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR


def test_get_with_bad_cache_name_throws_exception(
    client: SimpleCacheClient,
) -> None:
    get_response = client.get(1, "foo")
    assert isinstance(get_response, CacheGet.Error)
    assert get_response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
    assert get_response.inner_exception.message == "Cache name must be a non-empty string"


def test_get_with_bad_key_throws_exception(client: SimpleCacheClient, cache_name: str) -> None:
    get_response = client.get(cache_name, 1)
    assert isinstance(get_response, CacheGet.Error)
    assert get_response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
    assert get_response.inner_exception.message == "Unsupported type for key: <class 'int'>"


def test_get_throws_authentication_exception_for_bad_token(
    bad_token_credential_provider: EnvMomentoTokenProvider,
    configuration: Configuration,
    cache_name: str,
    default_ttl_seconds: timedelta,
) -> None:
    with SimpleCacheClient(configuration, bad_token_credential_provider, default_ttl_seconds) as client:
        get_response = client.get(cache_name, "foo")
        assert isinstance(get_response, CacheGet.Error)
        assert get_response.error_code == MomentoErrorCode.AUTHENTICATION_ERROR


def test_get_throws_timeout_error_for_short_request_timeout(
    configuration: Configuration,
    credential_provider: EnvMomentoTokenProvider,
    cache_name: str,
    default_ttl_seconds: timedelta,
) -> None:
    configuration = configuration.with_client_timeout(timedelta(milliseconds=1))
    with SimpleCacheClient(configuration, credential_provider, default_ttl_seconds) as client:
        get_response = client.get(cache_name, "foo")
        assert isinstance(get_response, CacheGet.Error)
        assert get_response.error_code == MomentoErrorCode.TIMEOUT_ERROR


# Test delete for key that doesn't exist
def test_delete_key_doesnt_exist(client: SimpleCacheClient, cache_name: str) -> None:
    key = uuid_str()
    get_response = client.get(cache_name, key)
    assert isinstance(get_response, CacheGet.Miss)

    delete_response = client.delete(cache_name, key)
    assert isinstance(delete_response, CacheDelete.Success)
    get_response = client.get(cache_name, key)
    assert isinstance(get_response, CacheGet.Miss)


# Test delete
def test_delete(client: SimpleCacheClient, cache_name: str) -> None:
    # Set an item to then delete...
    key, value = uuid_str(), uuid_str()
    get_response = client.get(cache_name, key)
    assert isinstance(get_response, CacheGet.Miss)
    set_response = client.set(cache_name, key, value)
    assert isinstance(set_response, CacheSet.Success)

    get_response = client.get(cache_name, key)
    assert isinstance(get_response, CacheGet.Hit)

    # Delete
    delete_response = client.delete(cache_name, key)
    assert isinstance(delete_response, CacheDelete.Success)

    # Verify deleted
    get_response = client.get(cache_name, key)
    assert isinstance(get_response, CacheGet.Miss)


def test_configuration_client_timeout_copy_constructor(configuration: Configuration) -> None:
    def snag_deadline(config: Configuration) -> timedelta:
        return config.get_transport_strategy().get_grpc_configuration().get_deadline()  # type: ignore

    original_deadline: timedelta = snag_deadline(configuration)
    assert original_deadline.total_seconds() == 15
    configuration = configuration.with_client_timeout(timedelta(seconds=600))
    assert snag_deadline(configuration).total_seconds() == 600

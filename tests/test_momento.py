import os
import time
from datetime import timedelta

import pytest

import momento.errors as errors
from momento.auth.credential_provider import CredentialProvider, EnvMomentoTokenProvider
from momento.cache_operation_types import CacheGetStatus
from momento.config.configuration import Configuration
from momento.simple_cache_client import SimpleCacheClient
from tests.utils import str_to_bytes, unique_test_cache_name, uuid_bytes, uuid_str


def test_create_cache_get_set_values_and_delete_cache(client: SimpleCacheClient, cache_name: str):
    random_cache_name = unique_test_cache_name()
    key = uuid_str()
    value = uuid_str()

    client.create_cache(random_cache_name)

    set_resp = client.set(random_cache_name, key, value)
    assert set_resp.value() == value

    get_resp = client.get(random_cache_name, key)
    assert get_resp.status() == CacheGetStatus.HIT
    assert get_resp.value() == value

    get_for_key_in_some_other_cache = client.get(cache_name, key)
    assert get_for_key_in_some_other_cache.status() == CacheGetStatus.MISS

    client.delete_cache(random_cache_name)


# Init
def test_init_throws_exception_when_client_uses_negative_default_ttl(
    configuration: Configuration, credential_provider: CredentialProvider
):
    with pytest.raises(errors.InvalidArgumentError, match="TTL timedelta must be a non-negative integer"):
        SimpleCacheClient(configuration, credential_provider, timedelta(seconds=-1))


def test_init_throws_exception_for_non_jwt_token(default_ttl_seconds: int):
    with pytest.raises(errors.InvalidArgumentError, match="Invalid Auth token."):
        os.environ["BAD_AUTH_TOKEN"] = "notanauthtoken"
        EnvMomentoTokenProvider("BAD_AUTH_TOKEN")


def test_init_throws_exception_when_client_uses_negative_request_timeout_ms(
    configuration: Configuration, credential_provider: CredentialProvider, default_ttl_seconds: int
):
    with pytest.raises(errors.InvalidArgumentError, match="Request timeout must be greater than zero."):
        configuration = configuration.with_client_timeout(-1)
        SimpleCacheClient(configuration, credential_provider, default_ttl_seconds)


def test_init_throws_exception_when_client_uses_zero_request_timeout_ms(
    configuration: Configuration, credential_provider: CredentialProvider, default_ttl_seconds: int
):
    with pytest.raises(errors.InvalidArgumentError, match="Request timeout must be greater than zero."):
        configuration = configuration.with_client_timeout(0)
        SimpleCacheClient(configuration, credential_provider, default_ttl_seconds)


# Create cache
def test_create_cache_throws_already_exists_when_creating_existing_cache(client: SimpleCacheClient, cache_name: str):
    with pytest.raises(errors.AlreadyExistsError):
        client.create_cache(cache_name)


def test_create_cache_throws_exception_for_empty_cache_name(
    client: SimpleCacheClient,
):
    with pytest.raises(errors.BadRequestError):
        client.create_cache("")


def test_create_cache_throws_validation_exception_for_null_cache_name(
    client: SimpleCacheClient,
):
    with pytest.raises(errors.InvalidArgumentError, match="Cache name must be a non-empty string"):
        client.create_cache(None)


def test_create_cache_with_bad_cache_name_throws_exception(
    client: SimpleCacheClient,
):
    with pytest.raises(errors.InvalidArgumentError, match="Cache name must be a non-empty string"):
        client.create_cache(1)


def test_create_cache_throws_authentication_exception_for_bad_token(
    configuration: Configuration, bad_token_credential_provider: EnvMomentoTokenProvider, default_ttl_seconds: int
):
    with SimpleCacheClient(configuration, bad_token_credential_provider, default_ttl_seconds) as client:
        with pytest.raises(errors.AuthenticationError):
            client.create_cache(unique_test_cache_name())


# Delete cache
def test_delete_cache_succeeds(client: SimpleCacheClient, cache_name: str):
    cache_name = uuid_str()

    client.create_cache(cache_name)
    client.delete_cache(cache_name)

    with pytest.raises(errors.NotFoundError):
        client.delete_cache(cache_name)


def test_delete_cache_throws_not_found_when_deleting_unknown_cache(
    client: SimpleCacheClient,
):
    cache_name = uuid_str()
    with pytest.raises(errors.NotFoundError):
        client.delete_cache(cache_name)


def test_delete_cache_throws_invalid_input_for_null_cache_name(
    client: SimpleCacheClient,
):
    with pytest.raises(errors.InvalidArgumentError):
        client.delete_cache(None)


def test_delete_cache_throws_exception_for_empty_cache_name(
    client: SimpleCacheClient,
):
    with pytest.raises(errors.BadRequestError):
        client.delete_cache("")


def test_delete_with_bad_cache_name_throws_exception(client: SimpleCacheClient, cache_name: str):
    with pytest.raises(errors.InvalidArgumentError, match="Cache name must be a non-empty string"):
        client.delete_cache(1)


def test_delete_cache_throws_authentication_exception_for_bad_token(
    configuration: Configuration, bad_token_credential_provider: EnvMomentoTokenProvider, default_ttl_seconds: int
):
    with SimpleCacheClient(configuration, bad_token_credential_provider, default_ttl_seconds) as client:
        with pytest.raises(errors.AuthenticationError):
            client.delete_cache(uuid_str())


# List caches
def test_list_caches_succeeds(client: SimpleCacheClient, cache_name: str):
    cache_name = uuid_str()

    caches = (client.list_caches()).caches()
    cache_names = [cache.name() for cache in caches]
    assert cache_name not in cache_names

    try:
        client.create_cache(cache_name)

        list_cache_resp = client.list_caches()
        caches = list_cache_resp.caches()
        cache_names = [cache.name() for cache in caches]
        assert cache_name in cache_names
        assert list_cache_resp.next_token() is None
    finally:
        client.delete_cache(cache_name)


def test_list_caches_throws_authentication_exception_for_bad_token(
    configuration: Configuration, bad_token_credential_provider: EnvMomentoTokenProvider, default_ttl_seconds: int
):
    with SimpleCacheClient(configuration, bad_token_credential_provider, default_ttl_seconds) as client:
        with pytest.raises(errors.AuthenticationError):
            client.list_caches()


def test_list_caches_with_next_token_works(client: SimpleCacheClient, cache_name: str):
    """skip until pagination is actually implemented, see
    https://github.com/momentohq/control-plane-service/issues/83"""
    pass


# Setting and Getting
def test_set_and_get_with_hit(client: SimpleCacheClient, cache_name: str):
    key = uuid_str()
    value = uuid_str()

    set_resp = client.set(cache_name, key, value)
    assert set_resp.value() == value
    assert set_resp.value_as_bytes() == str_to_bytes(value)

    get_resp = client.get(cache_name, key)
    assert get_resp.status() == CacheGetStatus.HIT
    assert get_resp.value() == value
    assert get_resp.value_as_bytes() == str_to_bytes(value)


def test_set_and_get_with_byte_key_values(client: SimpleCacheClient, cache_name: str):
    key = uuid_bytes()
    value = uuid_bytes()

    set_resp = client.set(cache_name, key, value)
    assert set_resp.value_as_bytes() == value

    get_resp = client.get(cache_name, key)
    assert get_resp.status() == CacheGetStatus.HIT
    assert get_resp.value_as_bytes() == value


def test_get_returns_miss(client: SimpleCacheClient, cache_name: str):
    key = uuid_str()

    get_resp = client.get(cache_name, key)
    assert get_resp.status() == CacheGetStatus.MISS
    assert get_resp.value_as_bytes() is None
    assert get_resp.value() is None


def test_expires_items_after_ttl(client: SimpleCacheClient, cache_name: str):
    key = uuid_str()
    val = uuid_str()

    client.set(cache_name, key, val, timedelta(seconds=2))
    get_response = client.get(cache_name, key)
    assert get_response.status() == CacheGetStatus.HIT

    time.sleep(4)
    get_response = client.get(cache_name, key)
    assert get_response.status() == CacheGetStatus.MISS


def test_set_with_different_ttl(client: SimpleCacheClient, cache_name: str):
    key1 = uuid_str()
    key2 = uuid_str()

    client.set(cache_name, key1, "1", timedelta(seconds=2))
    client.set(cache_name, key2, "2")

    # Before
    get_response = client.get(cache_name, key1)
    assert get_response.status() == CacheGetStatus.HIT
    get_response = client.get(cache_name, key2)
    assert get_response.status() == CacheGetStatus.HIT

    time.sleep(4)

    # After
    get_response = client.get(cache_name, key1)
    assert get_response.status() == CacheGetStatus.MISS
    get_response = client.get(cache_name, key2)
    assert get_response.status() == CacheGetStatus.HIT


# Set
def test_set_with_non_existent_cache_name_throws_not_found(
    client: SimpleCacheClient,
):
    cache_name = uuid_str()
    with pytest.raises(errors.NotFoundError):
        client.set(cache_name, "foo", "bar")


def test_set_with_null_cache_name_throws_exception(client: SimpleCacheClient, cache_name: str):
    with pytest.raises(errors.InvalidArgumentError, match="Cache name must be a non-empty string"):
        client.set(None, "foo", "bar")


def test_set_with_empty_cache_name_throws_exception(
    client: SimpleCacheClient,
):
    with pytest.raises(errors.BadRequestError, match="Cache header is empty"):
        client.set("", "foo", "bar")


def test_set_with_null_key_throws_exception(client: SimpleCacheClient, cache_name: str):
    with pytest.raises(errors.InvalidArgumentError):
        client.set(cache_name, None, "bar")


def test_set_with_null_value_throws_exception(client: SimpleCacheClient, cache_name: str):
    with pytest.raises(errors.InvalidArgumentError):
        client.set(cache_name, "foo", None)


def test_set_negative_ttl_throws_exception(client: SimpleCacheClient, cache_name: str):
    with pytest.raises(errors.InvalidArgumentError, match="TTL Seconds must be a non-negative integer"):
        client.set(cache_name, "foo", "bar", timedelta(seconds=-1))


def test_set_with_bad_cache_name_throws_exception(
    client: SimpleCacheClient,
):
    with pytest.raises(errors.InvalidArgumentError, match="Cache name must be a non-empty string"):
        client.set(1, "foo", "bar")


def test_set_with_bad_key_throws_exception(client: SimpleCacheClient, cache_name: str):
    with pytest.raises(errors.InvalidArgumentError, match="Unsupported type for key: <class 'int'>"):
        client.set(cache_name, 1, "bar")


def test_set_with_bad_value_throws_exception(client: SimpleCacheClient, cache_name: str):
    with pytest.raises(errors.InvalidArgumentError, match="Unsupported type for value: <class 'int'>"):
        client.set(cache_name, "foo", 1)


def test_set_throws_authentication_exception_for_bad_token(
    configuration: Configuration,
    bad_token_credential_provider: EnvMomentoTokenProvider,
    cache_name: str,
    default_ttl_seconds: int,
):
    with SimpleCacheClient(configuration, bad_token_credential_provider, default_ttl_seconds) as client:
        with pytest.raises(errors.AuthenticationError):
            client.set(cache_name, "foo", "bar")


def test_set_throws_timeout_error_for_short_request_timeout(
    configuration: Configuration, credential_provider: CredentialProvider, cache_name: str, default_ttl_seconds: int
):
    configuration = configuration.with_client_timeout(timedelta(milliseconds=1))
    with SimpleCacheClient(configuration, credential_provider, default_ttl_seconds) as client:
        with pytest.raises(errors.TimeoutError):
            client.set(cache_name, "foo", "bar")


# Get
def test_get_with_non_existent_cache_name_throws_not_found(
    client: SimpleCacheClient,
):
    cache_name = uuid_str()
    with pytest.raises(errors.NotFoundError):
        client.get(cache_name, "foo")


def test_get_with_null_cache_name_throws_exception(
    client: SimpleCacheClient,
):
    with pytest.raises(errors.InvalidArgumentError, match="Cache name must be a non-empty string"):
        client.get(None, "foo")


def test_get_with_empty_cache_name_throws_exception(
    client: SimpleCacheClient,
):
    with pytest.raises(errors.BadRequestError, match="Cache header is empty"):
        client.get("", "foo")


def test_get_with_null_key_throws_exception(client: SimpleCacheClient, cache_name: str):
    with pytest.raises(errors.InvalidArgumentError):
        client.get(cache_name, None)


def test_get_with_bad_cache_name_throws_exception(
    client: SimpleCacheClient,
):
    with pytest.raises(errors.InvalidArgumentError, match="Cache name must be a non-empty string"):
        client.get(1, "foo")


def test_get_with_bad_key_throws_exception(client: SimpleCacheClient, cache_name: str):
    with pytest.raises(errors.InvalidArgumentError, match="Unsupported type for key: <class 'int'>"):
        client.get(cache_name, 1)


def test_get_throws_authentication_exception_for_bad_token(
    configuration: Configuration,
    bad_token_credential_provider: EnvMomentoTokenProvider,
    cache_name: str,
    default_ttl_seconds: int,
):
    with SimpleCacheClient(configuration, bad_token_credential_provider, default_ttl_seconds) as client:
        with pytest.raises(errors.AuthenticationError):
            client.get(cache_name, "foo")


def test_get_throws_timeout_error_for_short_request_timeout(
    configuration: Configuration, credential_provider: CredentialProvider, cache_name: str, default_ttl_seconds: int
):
    configuration = configuration.with_client_timeout(timedelta(milliseconds=1))
    with SimpleCacheClient(configuration, credential_provider, default_ttl_seconds) as client:
        with pytest.raises(errors.TimeoutError):
            client.get(cache_name, "foo")


# Test delete for key that doesn't exist
def test_delete_key_doesnt_exist(client: SimpleCacheClient, cache_name: str):
    key = uuid_str()
    get_response = client.get(cache_name, key)
    assert get_response.status() == CacheGetStatus.MISS

    client.delete(cache_name, key)
    get_response = client.get(cache_name, key)
    assert get_response.status() == CacheGetStatus.MISS


# Test delete
def test_delete(client: SimpleCacheClient, cache_name: str):
    # Set an item to then delete...
    key, value = uuid_str(), uuid_str()
    get_response = client.get(cache_name, key)
    assert get_response.status() == CacheGetStatus.MISS
    client.set(cache_name, key, value)

    get_response = client.get(cache_name, key)
    assert get_response.status() == CacheGetStatus.HIT

    # Delete
    client.delete(cache_name, key)

    # Verify deleted
    get_response = client.get(cache_name, key)
    assert get_response.status() == CacheGetStatus.MISS


def test_configuration_client_timeout_copy_constructor(configuration: Configuration):
    def snag_deadline(config: Configuration) -> timedelta:
        return config.get_transport_strategy().get_grpc_configuration().get_deadline()

    original_deadline: timedelta = snag_deadline(configuration)
    assert original_deadline.total_seconds() == 15
    configuration = configuration.with_client_timeout(timedelta(seconds=600))
    assert snag_deadline(configuration).total_seconds() == 600

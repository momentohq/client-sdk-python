import time
from datetime import timedelta

from momento import SimpleCacheClient
from momento.auth import EnvMomentoTokenProvider
from momento.config import Configuration
from momento.errors import MomentoErrorCode
from momento.responses import CacheDelete, CacheGet, CacheSet
from tests.utils import str_to_bytes, uuid_bytes, uuid_str
from pytest import fixture
from pytest_describe import behaves_like


def a_cache_name_validator():
    def with_non_existent_cache_name_it_throws_not_found(
        client: SimpleCacheClient, cache_name_validator
    ) -> None:
        cache_name = uuid_str()
        response = cache_name_validator(cache_name)
        assert response.error_code == MomentoErrorCode.NOT_FOUND_ERROR

    def with_null_cache_name_it_throws_exception(
        client: SimpleCacheClient, cache_name_validator
    ) -> None:
        response = cache_name_validator(None)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
        assert response.inner_exception.message == "Cache name must be a non-empty string"

    def with_empty_cache_name_it_throws_exception(
        client: SimpleCacheClient, cache_name_validator
    ) -> None:
        response = cache_name_validator("")
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
        assert response.inner_exception.message == "Cache header is empty"

    def with_bad_cache_name_throws_exception(
        client: SimpleCacheClient, cache_name_validator
    ) -> None:
        response = cache_name_validator(1)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
        assert response.inner_exception.message == "Cache name must be a non-empty string"


def a_key_validator():
    def with_null_key_throws_exception(client: SimpleCacheClient, cache_name: str, key_validator) -> None:
        response = key_validator(cache_name, None)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR

    def with_bad_key_throws_exception(client: SimpleCacheClient, cache_name: str, key_validator) -> None:
        response = key_validator(cache_name, 1)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
        assert response.inner_exception.message == "Unsupported type for key: <class 'int'>"


def describe_set_and_get():
    def with_hit(client: SimpleCacheClient, cache_name: str) -> None:
        key = uuid_str()
        value = uuid_str()

        set_resp = client.set(cache_name, key, value)
        assert isinstance(set_resp, CacheSet.Success)

        get_resp = client.get(cache_name, key)
        isinstance(get_resp, CacheGet.Hit)
        assert get_resp.value_string == value
        assert get_resp.value_bytes == str_to_bytes(value)

    def with_byte_key_values(client: SimpleCacheClient, cache_name: str) -> None:
        key = uuid_bytes()
        value = uuid_bytes()

        set_resp = client.set(cache_name, key, value)
        assert isinstance(set_resp, CacheSet.Success)

        get_resp = client.get(cache_name, key)
        assert isinstance(get_resp, CacheGet.Hit)
        assert get_resp.value_bytes == value


@behaves_like(a_cache_name_validator)
@behaves_like(a_key_validator)
def describe_get():
    @fixture
    def cache_name_validator(client: SimpleCacheClient, cache_name):
        def _cache_name_validator(cache_name):
            key = uuid_str()
            return client.get(cache_name, key)
        
        return _cache_name_validator

    @fixture
    def key_validator(client: SimpleCacheClient, cache_name):
        def _key_validator(cache_name, key):
            return client.get(cache_name, key)
        
        return _key_validator
    
    def returns_miss(client: SimpleCacheClient, cache_name: str) -> None:
        key = uuid_str()

        get_resp = client.get(cache_name, key)
        assert isinstance(get_resp, CacheGet.Miss)

    def throws_authentication_exception_for_bad_token(
        bad_token_credential_provider: EnvMomentoTokenProvider,
        configuration: Configuration,
        cache_name: str,
        default_ttl_seconds: timedelta,
    ) -> None:
        with SimpleCacheClient(configuration, bad_token_credential_provider, default_ttl_seconds) as client:
            get_response = client.get(cache_name, "foo")
            assert isinstance(get_response, CacheGet.Error)
            assert get_response.error_code == MomentoErrorCode.AUTHENTICATION_ERROR

    def throws_timeout_error_for_short_request_timeout(
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


@behaves_like(a_cache_name_validator)
@behaves_like(a_key_validator)
def describe_set():
    @fixture
    def cache_name_validator(client: SimpleCacheClient, cache_name):
        def _cache_name_validator(cache_name):
            key = uuid_str()
            value = uuid_str()
            return client.set(cache_name, key, value)
        
        return _cache_name_validator

    @fixture
    def key_validator(client: SimpleCacheClient, cache_name):
        def _key_validator(cache_name, key):
            value = uuid_str()
            return client.set(cache_name, key, value)
        
        return _key_validator

    def expires_items_after_ttl(client: SimpleCacheClient, cache_name: str) -> None:
        key = uuid_str()
        val = uuid_str()

        client.set(cache_name, key, val, timedelta(seconds=2))
        get_response = client.get(cache_name, key)
        assert isinstance(get_response, CacheGet.Hit)

        time.sleep(4)
        get_response = client.get(cache_name, key)
        assert isinstance(get_response, CacheGet.Miss)

    def with_different_ttl(client: SimpleCacheClient, cache_name: str) -> None:
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

    def with_null_value_throws_exception(client: SimpleCacheClient, cache_name: str) -> None:
        set_response = client.set(cache_name, "foo", None)
        assert isinstance(set_response, CacheSet.Error)
        assert set_response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR

    def negative_ttl_throws_exception(client: SimpleCacheClient, cache_name: str) -> None:
        set_response = client.set(cache_name, "foo", "bar", timedelta(seconds=-1))
        assert isinstance(set_response, CacheSet.Error)
        assert set_response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
        assert set_response.inner_exception.message == "TTL timedelta must be a non-negative integer"

    def with_bad_value_throws_exception(client: SimpleCacheClient, cache_name: str) -> None:
        set_response = client.set(cache_name, "foo", 1)
        assert isinstance(set_response, CacheSet.Error)
        assert set_response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
        assert set_response.inner_exception.message == "Unsupported type for value: <class 'int'>"

    def throws_authentication_exception_for_bad_token(
        bad_token_credential_provider: EnvMomentoTokenProvider,
        configuration: Configuration,
        cache_name: str,
        default_ttl_seconds: timedelta,
    ) -> None:
        with SimpleCacheClient(configuration, bad_token_credential_provider, default_ttl_seconds) as client:
            set_response = client.set(cache_name, "foo", "bar")
            assert isinstance(set_response, CacheSet.Error)
            assert set_response.error_code == MomentoErrorCode.AUTHENTICATION_ERROR

    def throws_timeout_error_for_short_request_timeout(
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


@behaves_like(a_cache_name_validator)
@behaves_like(a_key_validator)
def describe_delete():
    @fixture
    def cache_name_validator(client: SimpleCacheClient, cache_name):
        def _cache_name_validator(cache_name):
            key = uuid_str()
            return client.delete(cache_name, key)
        
        return _cache_name_validator

    @fixture
    def key_validator(client: SimpleCacheClient, cache_name):
        def _key_validator(cache_name, key):
            return client.delete(cache_name, key)
        
        return _key_validator

    def key_doesnt_exist(client: SimpleCacheClient, cache_name: str) -> None:
        key = uuid_str()
        get_response = client.get(cache_name, key)
        assert isinstance(get_response, CacheGet.Miss)

        delete_response = client.delete(cache_name, key)
        assert isinstance(delete_response, CacheDelete.Success)
        get_response = client.get(cache_name, key)
        assert isinstance(get_response, CacheGet.Miss)

    def delete_a_key(client: SimpleCacheClient, cache_name: str) -> None:
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

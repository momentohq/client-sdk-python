import time
from datetime import timedelta

from pytest import fixture
from pytest_describe import behaves_like

from momento import SimpleCacheClientAsync
from momento.auth import EnvMomentoTokenProvider
from momento.config import Configuration
from momento.errors import MomentoErrorCode
from momento.responses import CacheDelete, CacheGet, CacheSet
from tests.utils import str_to_bytes, uuid_bytes, uuid_str


def a_cache_name_validator() -> None:
    async def with_non_existent_cache_name_it_throws_not_found(
        client_async: SimpleCacheClientAsync, cache_name_validator
    ) -> None:
        cache_name = uuid_str()
        response = await cache_name_validator(cache_name)
        assert response.error_code == MomentoErrorCode.NOT_FOUND_ERROR

    async def with_null_cache_name_it_throws_exception(
        client_async: SimpleCacheClientAsync, cache_name_validator
    ) -> None:
        response = await cache_name_validator(None)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
        assert response.inner_exception.message == "Cache name must be a non-empty string"

    async def with_empty_cache_name_it_throws_exception(
        client_async: SimpleCacheClientAsync, cache_name_validator
    ) -> None:
        response = await cache_name_validator("")
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
        assert response.inner_exception.message == "Cache header is empty"

    async def with_bad_cache_name_throws_exception(client_async: SimpleCacheClientAsync, cache_name_validator) -> None:
        response = await cache_name_validator(1)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
        assert response.inner_exception.message == "Cache name must be a non-empty string"


def a_key_validator() -> None:
    async def with_null_key_throws_exception(
        client_async: SimpleCacheClientAsync, cache_name: str, key_validator
    ) -> None:
        response = await key_validator(cache_name, None)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR

    async def with_bad_key_throws_exception(
        client_async: SimpleCacheClientAsync, cache_name: str, key_validator
    ) -> None:
        response = await key_validator(cache_name, 1)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
        assert response.inner_exception.message == "Unsupported type for key: <class 'int'>"


def a_connection_validator() -> None:
    async def throws_authentication_exception_for_bad_token(
        bad_token_credential_provider: EnvMomentoTokenProvider,
        configuration: Configuration,
        cache_name: str,
        default_ttl_seconds: timedelta,
        connection_validator,
    ) -> None:
        async with SimpleCacheClientAsync(
            configuration, bad_token_credential_provider, default_ttl_seconds
        ) as client_async:
            response = await connection_validator(client_async, cache_name)
            assert response.error_code == MomentoErrorCode.AUTHENTICATION_ERROR

    async def throws_timeout_error_for_short_request_timeout(
        configuration: Configuration,
        credential_provider: EnvMomentoTokenProvider,
        cache_name: str,
        default_ttl_seconds: timedelta,
        connection_validator,
    ) -> None:
        configuration = configuration.with_client_timeout(timedelta(milliseconds=1))
        async with SimpleCacheClientAsync(configuration, credential_provider, default_ttl_seconds) as client_async:
            response = await connection_validator(client_async, cache_name)
            assert response.error_code == MomentoErrorCode.TIMEOUT_ERROR


def describe_set_and_get() -> None:
    async def with_hit(client_async: SimpleCacheClientAsync, cache_name: str) -> None:
        key = uuid_str()
        value = uuid_str()

        set_resp = await client_async.set(cache_name, key, value)
        assert isinstance(set_resp, CacheSet.Success)

        get_resp = await client_async.get(cache_name, key)
        isinstance(get_resp, CacheGet.Hit)
        assert get_resp.value_string == value
        assert get_resp.value_bytes == str_to_bytes(value)

    async def with_byte_key_values(client_async: SimpleCacheClientAsync, cache_name: str) -> None:
        key = uuid_bytes()
        value = uuid_bytes()

        set_resp = await client_async.set(cache_name, key, value)
        assert isinstance(set_resp, CacheSet.Success)

        get_resp = await client_async.get(cache_name, key)
        assert isinstance(get_resp, CacheGet.Hit)
        assert get_resp.value_bytes == value


@behaves_like(a_cache_name_validator)
@behaves_like(a_key_validator)
@behaves_like(a_connection_validator)
def describe_get() -> None:
    @fixture
    def cache_name_validator(client_async: SimpleCacheClientAsync):
        def _cache_name_validator(cache_name: str):
            key = uuid_str()
            return client_async.get(cache_name, key)

        return _cache_name_validator

    @fixture
    def key_validator(client_async: SimpleCacheClientAsync):
        def _key_validator(cache_name: str, key: str):
            return client_async.get(cache_name, key)

        return _key_validator

    @fixture
    def connection_validator():
        def _connection_validator(client_async: SimpleCacheClientAsync, cache_name: str):
            key = uuid_str()
            return client_async.get(cache_name, key)

        return _connection_validator

    async def returns_miss(client_async: SimpleCacheClientAsync, cache_name: str) -> None:
        key = uuid_str()

        get_resp = await client_async.get(cache_name, key)
        assert isinstance(get_resp, CacheGet.Miss)


@behaves_like(a_cache_name_validator)
@behaves_like(a_key_validator)
@behaves_like(a_connection_validator)
def describe_set() -> None:
    @fixture
    def cache_name_validator(client_async: SimpleCacheClientAsync):
        def _cache_name_validator(cache_name: str):
            key = uuid_str()
            value = uuid_str()
            return client_async.set(cache_name, key, value)

        return _cache_name_validator

    @fixture
    def key_validator(client_async: SimpleCacheClientAsync):
        def _key_validator(cache_name: str, key: str):
            value = uuid_str()
            return client_async.set(cache_name, key, value)

        return _key_validator

    @fixture
    def connection_validator():
        def _connection_validator(client_async: SimpleCacheClientAsync, cache_name: str):
            key = uuid_str()
            value = uuid_str()
            return client_async.set(cache_name, key, value)

        return _connection_validator

    async def expires_items_after_ttl(client_async: SimpleCacheClientAsync, cache_name: str) -> None:
        key = uuid_str()
        val = uuid_str()

        await client_async.set(cache_name, key, val, timedelta(seconds=2))
        get_response = await client_async.get(cache_name, key)
        assert isinstance(get_response, CacheGet.Hit)

        time.sleep(4)
        get_response = await client_async.get(cache_name, key)
        assert isinstance(get_response, CacheGet.Miss)

    async def with_different_ttl(client_async: SimpleCacheClientAsync, cache_name: str) -> None:
        key1 = uuid_str()
        key2 = uuid_str()

        await client_async.set(cache_name, key1, "1", timedelta(seconds=2))
        await client_async.set(cache_name, key2, "2")

        # Before
        get_response = await client_async.get(cache_name, key1)
        assert isinstance(get_response, CacheGet.Hit)
        get_response = await client_async.get(cache_name, key2)
        assert isinstance(get_response, CacheGet.Hit)

        time.sleep(4)

        # After
        get_response = await client_async.get(cache_name, key1)
        assert isinstance(get_response, CacheGet.Miss)
        get_response = await client_async.get(cache_name, key2)
        assert isinstance(get_response, CacheGet.Hit)

    async def with_null_value_throws_exception(client_async: SimpleCacheClientAsync, cache_name: str) -> None:
        set_response = await client_async.set(cache_name, "foo", None)
        assert isinstance(set_response, CacheSet.Error)
        assert set_response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR

    async def negative_ttl_throws_exception(client_async: SimpleCacheClientAsync, cache_name: str) -> None:
        set_response = await client_async.set(cache_name, "foo", "bar", timedelta(seconds=-1))
        assert isinstance(set_response, CacheSet.Error)
        assert set_response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
        assert set_response.inner_exception.message == "TTL timedelta must be a non-negative integer"

    async def with_bad_value_throws_exception(client_async: SimpleCacheClientAsync, cache_name: str) -> None:
        set_response = await client_async.set(cache_name, "foo", 1)
        assert isinstance(set_response, CacheSet.Error)
        assert set_response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
        assert set_response.inner_exception.message == "Unsupported type for value: <class 'int'>"


@behaves_like(a_cache_name_validator)
@behaves_like(a_key_validator)
@behaves_like(a_connection_validator)
def describe_delete() -> None:
    @fixture
    def cache_name_validator(client_async: SimpleCacheClientAsync):
        def _cache_name_validator(cache_name: str):
            key = uuid_str()
            return client_async.delete(cache_name, key)

        return _cache_name_validator

    @fixture
    def key_validator(client_async: SimpleCacheClientAsync):
        def _key_validator(cache_name: str, key: str):
            return client_async.delete(cache_name, key)

        return _key_validator

    @fixture
    def connection_validator():
        def _connection_validator(client_async: SimpleCacheClientAsync, cache_name: str):
            key = uuid_str()
            return client_async.delete(cache_name, key)

        return _connection_validator

    async def key_doesnt_exist(client_async: SimpleCacheClientAsync, cache_name: str) -> None:
        key = uuid_str()
        get_response = await client_async.get(cache_name, key)
        assert isinstance(get_response, CacheGet.Miss)

        delete_response = await client_async.delete(cache_name, key)
        assert isinstance(delete_response, CacheDelete.Success)
        get_response = await client_async.get(cache_name, key)
        assert isinstance(get_response, CacheGet.Miss)

    async def delete_a_key(client_async: SimpleCacheClientAsync, cache_name: str) -> None:
        # Set an item to then delete...
        key, value = uuid_str(), uuid_str()
        get_response = await client_async.get(cache_name, key)
        assert isinstance(get_response, CacheGet.Miss)
        set_response = await client_async.set(cache_name, key, value)
        assert isinstance(set_response, CacheSet.Success)

        get_response = await client_async.get(cache_name, key)
        assert isinstance(get_response, CacheGet.Hit)

        # Delete
        delete_response = await client_async.delete(cache_name, key)
        assert isinstance(delete_response, CacheDelete.Success)

        # Verify deleted
        get_response = await client_async.get(cache_name, key)
        assert isinstance(get_response, CacheGet.Miss)

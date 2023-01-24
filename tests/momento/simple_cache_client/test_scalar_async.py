import time
from datetime import timedelta

from momento import SimpleCacheClientAsync
from momento.auth import EnvMomentoTokenProvider
from momento.config import Configuration
from momento.errors import MomentoErrorCode
from momento.responses import CacheDelete, CacheGet, CacheSet
from tests.utils import str_to_bytes, uuid_bytes, uuid_str


# Setting and Getting
async def test_set_and_get_with_hit(client_async: SimpleCacheClientAsync, cache_name: str) -> None:
    key = uuid_str()
    value = uuid_str()

    set_resp = await client_async.set(cache_name, key, value)
    assert isinstance(set_resp, CacheSet.Success)

    get_resp = await client_async.get(cache_name, key)
    isinstance(get_resp, CacheGet.Hit)
    assert get_resp.value_string == value
    assert get_resp.value_bytes == str_to_bytes(value)


async def test_set_and_get_with_byte_key_values(client_async: SimpleCacheClientAsync, cache_name: str) -> None:
    key = uuid_bytes()
    value = uuid_bytes()

    set_resp = await client_async.set(cache_name, key, value)
    assert isinstance(set_resp, CacheSet.Success)

    get_resp = await client_async.get(cache_name, key)
    assert isinstance(get_resp, CacheGet.Hit)
    assert get_resp.value_bytes == value


async def test_get_returns_miss(client_async: SimpleCacheClientAsync, cache_name: str) -> None:
    key = uuid_str()

    get_resp = await client_async.get(cache_name, key)
    assert isinstance(get_resp, CacheGet.Miss)


async def test_expires_items_after_ttl(client_async: SimpleCacheClientAsync, cache_name: str) -> None:
    key = uuid_str()
    val = uuid_str()

    await client_async.set(cache_name, key, val, timedelta(seconds=2))
    get_response = await client_async.get(cache_name, key)
    assert isinstance(get_response, CacheGet.Hit)

    time.sleep(4)
    get_response = await client_async.get(cache_name, key)
    assert isinstance(get_response, CacheGet.Miss)


async def test_set_with_different_ttl(client_async: SimpleCacheClientAsync, cache_name: str) -> None:
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


# Set
async def test_set_with_non_existent_cache_name_throws_not_found(
    client_async: SimpleCacheClientAsync,
) -> None:
    cache_name = uuid_str()
    set_response = await client_async.set(cache_name, "foo", "bar")
    assert isinstance(set_response, CacheSet.Error)
    assert set_response.error_code == MomentoErrorCode.NOT_FOUND_ERROR


async def test_set_with_null_cache_name_throws_exception(client_async: SimpleCacheClientAsync, cache_name: str) -> None:
    set_response = await client_async.set(None, "foo", "bar")
    assert isinstance(set_response, CacheSet.Error)
    assert set_response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
    assert set_response.inner_exception.message == "Cache name must be a non-empty string"


async def test_set_with_empty_cache_name_throws_exception(
    client_async: SimpleCacheClientAsync,
) -> None:
    set_response = await client_async.set("", "foo", "bar")
    assert isinstance(set_response, CacheSet.Error)
    assert set_response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
    assert set_response.inner_exception.message == "Cache header is empty"


async def test_set_with_null_key_throws_exception(client_async: SimpleCacheClientAsync, cache_name: str) -> None:
    set_response = await client_async.set(cache_name, None, "bar")
    assert isinstance(set_response, CacheSet.Error)
    assert set_response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR


async def test_set_with_null_value_throws_exception(client_async: SimpleCacheClientAsync, cache_name: str) -> None:
    set_response = await client_async.set(cache_name, "foo", None)
    assert isinstance(set_response, CacheSet.Error)
    assert set_response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR


async def test_set_negative_ttl_throws_exception(client_async: SimpleCacheClientAsync, cache_name: str) -> None:
    set_response = await client_async.set(cache_name, "foo", "bar", timedelta(seconds=-1))
    assert isinstance(set_response, CacheSet.Error)
    assert set_response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
    assert set_response.inner_exception.message == "TTL timedelta must be a non-negative integer"


async def test_set_with_bad_cache_name_throws_exception(
    client_async: SimpleCacheClientAsync,
) -> None:
    set_response = await client_async.set(1, "foo", "bar")
    assert isinstance(set_response, CacheSet.Error)
    assert set_response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
    assert set_response.inner_exception.message == "Cache name must be a non-empty string"


async def test_set_with_bad_key_throws_exception(client_async: SimpleCacheClientAsync, cache_name: str) -> None:
    set_response = await client_async.set(cache_name, 1, "bar")
    assert isinstance(set_response, CacheSet.Error)
    assert set_response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
    assert set_response.inner_exception.message == "Unsupported type for key: <class 'int'>"


async def test_set_with_bad_value_throws_exception(client_async: SimpleCacheClientAsync, cache_name: str) -> None:
    set_response = await client_async.set(cache_name, "foo", 1)
    assert isinstance(set_response, CacheSet.Error)
    assert set_response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
    assert set_response.inner_exception.message == "Unsupported type for value: <class 'int'>"


async def test_set_throws_authentication_exception_for_bad_token(
    bad_token_credential_provider: EnvMomentoTokenProvider,
    configuration: Configuration,
    cache_name: str,
    default_ttl_seconds: timedelta,
) -> None:
    async with SimpleCacheClientAsync(
        configuration, bad_token_credential_provider, default_ttl_seconds
    ) as client_async:
        set_response = await client_async.set(cache_name, "foo", "bar")
        assert isinstance(set_response, CacheSet.Error)
        assert set_response.error_code == MomentoErrorCode.AUTHENTICATION_ERROR


async def test_set_throws_timeout_error_for_short_request_timeout(
    configuration: Configuration,
    credential_provider: EnvMomentoTokenProvider,
    cache_name: str,
    default_ttl_seconds: timedelta,
) -> None:
    configuration = configuration.with_client_timeout(timedelta(milliseconds=1))
    async with SimpleCacheClientAsync(configuration, credential_provider, default_ttl_seconds) as client_async:
        set_response = await client_async.set(cache_name, "foo", "bar")
        assert isinstance(set_response, CacheSet.Error)
        assert set_response.error_code == MomentoErrorCode.TIMEOUT_ERROR


# Get
async def test_get_with_non_existent_cache_name_throws_not_found(
    client_async: SimpleCacheClientAsync,
) -> None:
    cache_name = uuid_str()
    get_response = await client_async.get(cache_name, "foo")
    assert isinstance(get_response, CacheGet.Error)
    assert get_response.error_code == MomentoErrorCode.NOT_FOUND_ERROR


async def test_get_with_null_cache_name_throws_exception(
    client_async: SimpleCacheClientAsync,
) -> None:
    get_response = await client_async.get(None, "foo")
    assert isinstance(get_response, CacheGet.Error)
    assert get_response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
    assert get_response.inner_exception.message == "Cache name must be a non-empty string"


async def test_get_with_empty_cache_name_throws_exception(
    client_async: SimpleCacheClientAsync,
) -> None:
    get_response = await client_async.get("", "foo")
    assert isinstance(get_response, CacheGet.Error)
    assert get_response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
    assert get_response.inner_exception.message == "Cache header is empty"


async def test_get_with_null_key_throws_exception(client_async: SimpleCacheClientAsync, cache_name: str) -> None:
    get_response = await client_async.get(cache_name, None)
    assert isinstance(get_response, CacheGet.Error)
    assert get_response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR


async def test_get_with_bad_cache_name_throws_exception(
    client_async: SimpleCacheClientAsync,
) -> None:
    get_response = await client_async.get(1, "foo")
    assert isinstance(get_response, CacheGet.Error)
    assert get_response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
    assert get_response.inner_exception.message == "Cache name must be a non-empty string"


async def test_get_with_bad_key_throws_exception(client_async: SimpleCacheClientAsync, cache_name: str) -> None:
    get_response = await client_async.get(cache_name, 1)
    assert isinstance(get_response, CacheGet.Error)
    assert get_response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
    assert get_response.inner_exception.message == "Unsupported type for key: <class 'int'>"


async def test_get_throws_authentication_exception_for_bad_token(
    bad_token_credential_provider: EnvMomentoTokenProvider,
    configuration: Configuration,
    cache_name: str,
    default_ttl_seconds: timedelta,
) -> None:
    async with SimpleCacheClientAsync(
        configuration, bad_token_credential_provider, default_ttl_seconds
    ) as client_async:
        get_response = await client_async.get(cache_name, "foo")
        assert isinstance(get_response, CacheGet.Error)
        assert get_response.error_code == MomentoErrorCode.AUTHENTICATION_ERROR


async def test_get_throws_timeout_error_for_short_request_timeout(
    configuration: Configuration,
    credential_provider: EnvMomentoTokenProvider,
    cache_name: str,
    default_ttl_seconds: timedelta,
) -> None:
    configuration = configuration.with_client_timeout(timedelta(milliseconds=1))
    async with SimpleCacheClientAsync(configuration, credential_provider, default_ttl_seconds) as client_async:
        get_response = await client_async.get(cache_name, "foo")
        assert isinstance(get_response, CacheGet.Error)
        assert get_response.error_code == MomentoErrorCode.TIMEOUT_ERROR


# Test delete for key that doesn't exist
async def test_delete_key_doesnt_exist(client_async: SimpleCacheClientAsync, cache_name: str) -> None:
    key = uuid_str()
    get_response = await client_async.get(cache_name, key)
    assert isinstance(get_response, CacheGet.Miss)

    delete_response = await client_async.delete(cache_name, key)
    assert isinstance(delete_response, CacheDelete.Success)
    get_response = await client_async.get(cache_name, key)
    assert isinstance(get_response, CacheGet.Miss)


# Test delete
async def test_delete(client_async: SimpleCacheClientAsync, cache_name: str) -> None:
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

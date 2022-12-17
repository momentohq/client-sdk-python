import time

import pytest

import momento.errors as errors
from momento.aio.simple_cache_client import SimpleCacheClient
from momento.cache_operation_types import CacheGetStatus
from tests.utils import str_to_bytes, uuid_bytes, uuid_str, unique_test_cache_name


async def test_create_cache_get_set_values_and_delete_cache(client_async: SimpleCacheClient, cache_name: str):
    random_cache_name = unique_test_cache_name()
    key = uuid_str()
    value = uuid_str()

    await client_async.create_cache(random_cache_name)

    set_resp = await client_async.set(random_cache_name, key, value)
    assert set_resp.value() == value

    get_resp = await client_async.get(random_cache_name, key)
    assert get_resp.status() == CacheGetStatus.HIT
    assert get_resp.value() == value

    get_for_key_in_some_other_cache = await client_async.get(cache_name, key)
    assert get_for_key_in_some_other_cache.status() == CacheGetStatus.MISS

    await client_async.delete_cache(random_cache_name)


# Init
async def test_init_throws_exception_when_client_uses_negative_default_ttl(
    auth_token: str,
):
    with pytest.raises(errors.InvalidArgumentError) as cm:
        SimpleCacheClient(auth_token, -1)
        assert cm.exception == "TTL Seconds must be a non-negative integer"


async def test_init_throws_exception_for_non_jwt_token(default_ttl_seconds: int):
    with pytest.raises(errors.InvalidArgumentError) as cm:
        SimpleCacheClient("notanauthtoken", default_ttl_seconds)
        assert cm.exception == "Invalid Auth token."


async def test_init_throws_exception_when_client_uses_negative_request_timeout_ms(
    auth_token: str, default_ttl_seconds: int
):
    with pytest.raises(errors.InvalidArgumentError) as cm:
        SimpleCacheClient(auth_token, default_ttl_seconds, -1)
        assert cm.exception == "Request timeout must be greater than zero."


async def test_init_throws_exception_when_client_uses_zero_request_timeout_ms(
    auth_token: str, default_ttl_seconds: int
):
    with pytest.raises(errors.InvalidArgumentError) as cm:
        SimpleCacheClient(auth_token, default_ttl_seconds, 0)
        assert cm.exception == "Request timeout must be greater than zero."


# Create cache
async def test_create_cache_throws_already_exists_when_creating_existing_cache(
    client_async: SimpleCacheClient, cache_name: str
):
    with pytest.raises(errors.AlreadyExistsError):
        await client_async.create_cache(cache_name)


async def test_create_cache_throws_exception_for_empty_cache_name(
    client_async: SimpleCacheClient,
):
    with pytest.raises(errors.BadRequestError):
        await client_async.create_cache("")


async def test_create_cache_throws_validation_exception_for_null_cache_name(
    client_async: SimpleCacheClient,
):
    with pytest.raises(errors.InvalidArgumentError) as cm:
        await client_async.create_cache(None)
        assert cm.exception == "Cache name must be a non-empty string"


async def test_create_cache_with_bad_cache_name_throws_exception(
    client_async: SimpleCacheClient,
):
    with pytest.raises(errors.InvalidArgumentError) as cm:
        await client_async.create_cache(1)
        assert cm.exception == "Cache name must be a non-empty string"


async def test_create_cache_throws_authentication_exception_for_bad_token(
    bad_auth_token: str, default_ttl_seconds: int
):
    async with SimpleCacheClient(bad_auth_token, default_ttl_seconds) as client_async:
        with pytest.raises(errors.AuthenticationError):
            await client_async.create_cache(unique_test_cache_name())


# Delete cache
async def test_delete_cache_succeeds(client_async: SimpleCacheClient, cache_name: str):
    cache_name = uuid_str()

    await client_async.create_cache(cache_name)
    await client_async.delete_cache(cache_name)

    with pytest.raises(errors.NotFoundError):
        await client_async.delete_cache(cache_name)


async def test_delete_cache_throws_not_found_when_deleting_unknown_cache(
    client_async: SimpleCacheClient,
):
    cache_name = uuid_str()
    with pytest.raises(errors.NotFoundError):
        await client_async.delete_cache(cache_name)


async def test_delete_cache_throws_invalid_input_for_null_cache_name(
    client_async: SimpleCacheClient,
):
    with pytest.raises(errors.InvalidArgumentError):
        await client_async.delete_cache(None)


async def test_delete_cache_throws_exception_for_empty_cache_name(
    client_async: SimpleCacheClient,
):
    with pytest.raises(errors.BadRequestError):
        await client_async.delete_cache("")


async def test_delete_with_bad_cache_name_throws_exception(client_async: SimpleCacheClient, cache_name: str):
    with pytest.raises(errors.InvalidArgumentError) as cm:
        await client_async.delete_cache(1)
        assert cm.exception == "Cache name must be a non-empty string"


async def test_delete_cache_throws_authentication_exception_for_bad_token(
    bad_auth_token: str, default_ttl_seconds: int
):
    async with SimpleCacheClient(bad_auth_token, default_ttl_seconds) as client_async:
        with pytest.raises(errors.AuthenticationError):
            await client_async.delete_cache(uuid_str())


# List caches
async def test_list_caches_succeeds(client_async: SimpleCacheClient, cache_name: str):
    cache_name = uuid_str()

    caches = (await client_async.list_caches()).caches()
    cache_names = [cache.name() for cache in caches]
    assert cache_name not in cache_names

    try:
        await client_async.create_cache(cache_name)

        list_cache_resp = await client_async.list_caches()
        caches = list_cache_resp.caches()
        cache_names = [cache.name() for cache in caches]
        assert cache_name in cache_names
        assert list_cache_resp.next_token() is None
    finally:
        await client_async.delete_cache(cache_name)


async def test_list_caches_throws_authentication_exception_for_bad_token(bad_auth_token: str, default_ttl_seconds: int):
    async with SimpleCacheClient(bad_auth_token, default_ttl_seconds) as client_async:
        with pytest.raises(errors.AuthenticationError):
            await client_async.list_caches()


async def test_list_caches_with_next_token_works(client_async: SimpleCacheClient, cache_name: str):
    """skip until pagination is actually implemented, see
    https://github.com/momentohq/control-plane-service/issues/83"""
    pass


# Signing keys
async def test_create_list_revoke_signing_keys(client_async: SimpleCacheClient):
    create_resp = await client_async.create_signing_key(30)
    list_resp = await client_async.list_signing_keys()
    assert create_resp.key_id() in [signing_key.key_id() for signing_key in list_resp.signing_keys()]

    await client_async.revoke_signing_key(create_resp.key_id())
    list_resp = await client_async.list_signing_keys()
    assert create_resp.key_id() not in [signing_key.key_id() for signing_key in list_resp.signing_keys()]


# Setting and Getting
async def test_set_and_get_with_hit(client_async: SimpleCacheClient, cache_name: str):
    key = uuid_str()
    value = uuid_str()

    set_resp = await client_async.set(cache_name, key, value)
    assert set_resp.value() == value
    assert set_resp.value_as_bytes() == str_to_bytes(value)

    get_resp = await client_async.get(cache_name, key)
    assert get_resp.status() == CacheGetStatus.HIT
    assert get_resp.value() == value
    assert get_resp.value_as_bytes() == str_to_bytes(value)


async def test_set_and_get_with_byte_key_values(client_async: SimpleCacheClient, cache_name: str):
    key = uuid_bytes()
    value = uuid_bytes()

    set_resp = await client_async.set(cache_name, key, value)
    assert set_resp.value_as_bytes() == value

    get_resp = await client_async.get(cache_name, key)
    assert get_resp.status() == CacheGetStatus.HIT
    assert get_resp.value_as_bytes() == value


async def test_get_returns_miss(client_async: SimpleCacheClient, cache_name: str):
    key = uuid_str()

    get_resp = await client_async.get(cache_name, key)
    assert get_resp.status() == CacheGetStatus.MISS
    assert get_resp.value_as_bytes() is None
    assert get_resp.value() is None


async def test_expires_items_after_ttl(client_async: SimpleCacheClient, cache_name: str):
    key = uuid_str()
    val = uuid_str()

    await client_async.set(cache_name, key, val, 2)
    get_response = await client_async.get(cache_name, key)
    assert get_response.status() == CacheGetStatus.HIT

    time.sleep(4)
    get_response = await client_async.get(cache_name, key)
    assert get_response.status() == CacheGetStatus.MISS


async def test_set_with_different_ttl(client_async: SimpleCacheClient, cache_name: str):
    key1 = uuid_str()
    key2 = uuid_str()

    await client_async.set(cache_name, key1, "1", 2)
    await client_async.set(cache_name, key2, "2")

    # Before
    get_response = await client_async.get(cache_name, key1)
    assert get_response.status() == CacheGetStatus.HIT
    get_response = await client_async.get(cache_name, key2)
    assert get_response.status() == CacheGetStatus.HIT

    time.sleep(4)

    # After
    get_response = await client_async.get(cache_name, key1)
    assert get_response.status() == CacheGetStatus.MISS
    get_response = await client_async.get(cache_name, key2)
    assert get_response.status() == CacheGetStatus.HIT


# Set
async def test_set_with_non_existent_cache_name_throws_not_found(
    client_async: SimpleCacheClient,
):
    cache_name = uuid_str()
    with pytest.raises(errors.NotFoundError):
        await client_async.set(cache_name, "foo", "bar")


async def test_set_with_null_cache_name_throws_exception(client_async: SimpleCacheClient, cache_name: str):
    with pytest.raises(errors.InvalidArgumentError) as cm:
        await client_async.set(None, "foo", "bar")
        assert cm.exception == "Cache name must be a non-empty string"


async def test_set_with_empty_cache_name_throws_exception(
    client_async: SimpleCacheClient,
):
    with pytest.raises(errors.BadRequestError) as cm:
        await client_async.set("", "foo", "bar")
        assert cm.exception == "Cache header is empty"


async def test_set_with_null_key_throws_exception(client_async: SimpleCacheClient, cache_name: str):
    with pytest.raises(errors.InvalidArgumentError):
        await client_async.set(cache_name, None, "bar")


async def test_set_with_null_value_throws_exception(client_async: SimpleCacheClient, cache_name: str):
    with pytest.raises(errors.InvalidArgumentError):
        await client_async.set(cache_name, "foo", None)


async def test_set_negative_ttl_throws_exception(client_async: SimpleCacheClient, cache_name: str):
    with pytest.raises(errors.InvalidArgumentError) as cm:
        await client_async.set(cache_name, "foo", "bar", -1)
        assert cm.exception == "TTL Seconds must be a non-negative integer"


async def test_set_with_bad_cache_name_throws_exception(
    client_async: SimpleCacheClient,
):
    with pytest.raises(errors.InvalidArgumentError) as cm:
        await client_async.set(1, "foo", "bar")
        assert cm.exception == "Cache name must be a non-empty string"


async def test_set_with_bad_key_throws_exception(client_async: SimpleCacheClient, cache_name: str):
    with pytest.raises(errors.InvalidArgumentError) as cm:
        await client_async.set(cache_name, 1, "bar")
        assert cm.exception == "Unsupported type for key: <class 'int'>"


async def test_set_with_bad_value_throws_exception(client_async: SimpleCacheClient, cache_name: str):
    with pytest.raises(errors.InvalidArgumentError) as cm:
        await client_async.set(cache_name, "foo", 1)
        assert cm.exception == "Unsupported type for value: <class 'int'>"


async def test_set_throws_authentication_exception_for_bad_token(
    bad_auth_token: str, cache_name: str, default_ttl_seconds: int
):
    async with SimpleCacheClient(bad_auth_token, default_ttl_seconds) as client_async:
        with pytest.raises(errors.AuthenticationError):
            await client_async.set(cache_name, "foo", "bar")


async def test_set_throws_timeout_error_for_short_request_timeout(
    auth_token: str, cache_name: str, default_ttl_seconds: int
):
    async with SimpleCacheClient(auth_token, default_ttl_seconds, request_timeout_ms=1) as client_async:
        with pytest.raises(errors.TimeoutError):
            await client_async.set(cache_name, "foo", "bar")


# Get
async def test_get_with_non_existent_cache_name_throws_not_found(
    client_async: SimpleCacheClient,
):
    cache_name = uuid_str()
    with pytest.raises(errors.NotFoundError):
        await client_async.get(cache_name, "foo")


async def test_get_with_null_cache_name_throws_exception(
    client_async: SimpleCacheClient,
):
    with pytest.raises(errors.InvalidArgumentError) as cm:
        await client_async.get(None, "foo")
        assert cm.exception == "Cache name must be a non-empty string"


async def test_get_with_empty_cache_name_throws_exception(
    client_async: SimpleCacheClient,
):
    with pytest.raises(errors.BadRequestError) as cm:
        await client_async.get("", "foo")
        assert cm.exception == "Cache header is empty"


async def test_get_with_null_key_throws_exception(client_async: SimpleCacheClient, cache_name: str):
    with pytest.raises(errors.InvalidArgumentError):
        await client_async.get(cache_name, None)


async def test_get_with_bad_cache_name_throws_exception(
    client_async: SimpleCacheClient,
):
    with pytest.raises(errors.InvalidArgumentError) as cm:
        await client_async.get(1, "foo")
        assert cm.exception == "Cache name must be a non-empty string"


async def test_get_with_bad_key_throws_exception(client_async: SimpleCacheClient, cache_name: str):
    with pytest.raises(errors.InvalidArgumentError) as cm:
        await client_async.get(cache_name, 1)
        assert cm.exception == "Unsupported type for key: <class 'int'>"


async def test_get_throws_authentication_exception_for_bad_token(
    bad_auth_token: str, cache_name: str, default_ttl_seconds: int
):
    async with SimpleCacheClient(bad_auth_token, default_ttl_seconds) as client_async:
        with pytest.raises(errors.AuthenticationError):
            await client_async.get(cache_name, "foo")


async def test_get_throws_timeout_error_for_short_request_timeout(
    auth_token: str, cache_name: str, default_ttl_seconds: int
):
    async with SimpleCacheClient(auth_token, default_ttl_seconds, request_timeout_ms=1) as client_async:
        with pytest.raises(errors.TimeoutError):
            await client_async.get(cache_name, "foo")


# Test delete for key that doesn't exist
async def test_delete_key_doesnt_exist(client_async: SimpleCacheClient, cache_name: str):
    key = uuid_str()
    get_response = await client_async.get(cache_name, key)
    assert get_response.status() == CacheGetStatus.MISS

    await client_async.delete(cache_name, key)
    get_response = await client_async.get(cache_name, key)
    get_response.status() == CacheGetStatus.MISS


# Test delete
async def test_delete(client_async: SimpleCacheClient, cache_name: str):
    # Set an item to then delete...
    key, value = uuid_str(), uuid_str()
    get_response = await client_async.get(cache_name, key)
    assert get_response.status() == CacheGetStatus.MISS
    await client_async.set(cache_name, key, value)

    get_response = await client_async.get(cache_name, key)
    assert get_response.status() == CacheGetStatus.HIT

    # Delete
    await client_async.delete(cache_name, key)

    # Verify deleted
    get_response = await client_async.get(cache_name, key)
    assert get_response.status() == CacheGetStatus.MISS

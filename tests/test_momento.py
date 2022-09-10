import os
import time

import pytest

import momento.simple_cache_client as simple_cache_client
from momento.simple_cache_client import SimpleCacheClient
import momento.errors as errors
from momento.cache_operation_types import CacheGetStatus
from tests.utils import uuid_str, uuid_bytes, str_to_bytes


_AUTH_TOKEN = os.getenv("TEST_AUTH_TOKEN")
_TEST_CACHE_NAME = os.getenv("TEST_CACHE_NAME")
_DEFAULT_TTL_SECONDS = 60
_BAD_AUTH_TOKEN = "eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJpbnRlZ3JhdGlvbiIsImNwIjoiY29udHJvbC5jZWxsLWFscGhhLWRldi5wcmVwcm9kLmEubW9tZW50b2hxLmNvbSIsImMiOiJjYWNoZS5jZWxsLWFscGhhLWRldi5wcmVwcm9kLmEubW9tZW50b2hxLmNvbSJ9.gdghdjjfjyehhdkkkskskmmls76573jnajhjjjhjdhnndy"


@pytest.fixture
def client() -> SimpleCacheClient:
    if not _AUTH_TOKEN:
        raise RuntimeError(
            "Integration tests require TEST_AUTH_TOKEN env var; see README for more details."
        )
    if not _TEST_CACHE_NAME:
        raise RuntimeError(
            "Integration tests require TEST_CACHE_NAME env var; see README for more details."
        )

    with simple_cache_client.init(_AUTH_TOKEN, _DEFAULT_TTL_SECONDS) as _client:
        # Ensure test cache exists
        try:
            _client.create_cache(_TEST_CACHE_NAME)
        except errors.AlreadyExistsError:
            pass

        yield _client


def test_create_cache_get_set_values_and_delete_cache(client: SimpleCacheClient):
    cache_name = uuid_str()
    key = uuid_str()
    value = uuid_str()

    client.create_cache(cache_name)

    set_resp = client.set(cache_name, key, value)
    assert set_resp.value() == value

    get_resp = client.get(cache_name, key)
    assert get_resp.status() == CacheGetStatus.HIT
    assert get_resp.value() == value

    get_for_key_in_some_other_cache = client.get(_TEST_CACHE_NAME, key)
    assert get_for_key_in_some_other_cache.status() == CacheGetStatus.MISS

    client.delete_cache(cache_name)


# Init
def test_init_throws_exception_when_client_uses_negative_default_ttl():
    with pytest.raises(errors.InvalidArgumentError) as cm:
        simple_cache_client.init(_AUTH_TOKEN, -1)
        assert cm.exception == "TTL Seconds must be a non-negative integer"


def test_init_throws_exception_for_non_jwt_token():
    with pytest.raises(errors.InvalidArgumentError) as cm:
        simple_cache_client.init("notanauthtoken", _DEFAULT_TTL_SECONDS)
        assert cm.exception == "Invalid Auth token."


def test_init_throws_exception_when_client_uses_negative_request_timeout_ms():
    with pytest.raises(errors.InvalidArgumentError) as cm:
        simple_cache_client.init(_AUTH_TOKEN, _DEFAULT_TTL_SECONDS, -1)
        assert cm.exception == "Request timeout must be greater than zero."


def test_init_throws_exception_when_client_uses_zero_request_timeout_ms():
    with pytest.raises(errors.InvalidArgumentError) as cm:
        simple_cache_client.init(_AUTH_TOKEN, _DEFAULT_TTL_SECONDS, 0)
        assert cm.exception == "Request timeout must be greater than zero."


# Create cache
def test_create_cache_throws_already_exists_when_creating_existing_cache(
    client: SimpleCacheClient,
):
    with pytest.raises(errors.AlreadyExistsError):
        client.create_cache(_TEST_CACHE_NAME)


def test_create_cache_throws_exception_for_empty_cache_name(
    client: SimpleCacheClient,
):
    with pytest.raises(errors.BadRequestError):
        client.create_cache("")


def test_create_cache_throws_validation_exception_for_null_cache_name(
    client: SimpleCacheClient,
):
    with pytest.raises(errors.InvalidArgumentError) as cm:
        client.create_cache(None)
        assert cm.exception == "Cache name must be a non-empty string"


def test_create_cache_with_bad_cache_name_throws_exception(
    client: SimpleCacheClient,
):
    with pytest.raises(errors.InvalidArgumentError) as cm:
        client.create_cache(1)
        assert cm.exception == "Cache name must be a non-empty string"


def test_create_cache_throws_authentication_exception_for_bad_token():
    with simple_cache_client.init(_BAD_AUTH_TOKEN, _DEFAULT_TTL_SECONDS) as client:
        with pytest.raises(errors.AuthenticationError):
            client.create_cache(uuid_str())


# Delete cache
def test_delete_cache_succeeds(client: SimpleCacheClient):
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


def test_delete_with_bad_cache_name_throws_exception(client: SimpleCacheClient):
    with pytest.raises(errors.InvalidArgumentError) as cm:
        client.delete_cache(1)
        assert cm.exception == "Cache name must be a non-empty string"


def test_delete_cache_throws_authentication_exception_for_bad_token():
    with simple_cache_client.init(_BAD_AUTH_TOKEN, _DEFAULT_TTL_SECONDS) as client:
        with pytest.raises(errors.AuthenticationError):
            client.delete_cache(uuid_str())


# List caches
def test_list_caches_succeeds(client: SimpleCacheClient):
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


def test_list_caches_throws_authentication_exception_for_bad_token():
    with simple_cache_client.init(_BAD_AUTH_TOKEN, _DEFAULT_TTL_SECONDS) as client:
        with pytest.raises(errors.AuthenticationError):
            client.list_caches()


def test_list_caches_with_next_token_works(client: SimpleCacheClient):
    """skip until pagination is actually implemented, see
    https://github.com/momentohq/control-plane-service/issues/83"""
    pass


# Signing keys
def test_create_list_revoke_signing_keys(client: SimpleCacheClient):
    create_resp = client.create_signing_key(30)
    list_resp = client.list_signing_keys()
    assert create_resp.key_id() in [
        signing_key.key_id() for signing_key in list_resp.signing_keys()
    ]

    client.revoke_signing_key(create_resp.key_id())
    list_resp = client.list_signing_keys()
    assert create_resp.key_id() not in [
        signing_key.key_id() for signing_key in list_resp.signing_keys()
    ]


# Setting and Getting
def test_set_and_get_with_hit(client: SimpleCacheClient):
    key = uuid_str()
    value = uuid_str()

    set_resp = client.set(_TEST_CACHE_NAME, key, value)
    assert set_resp.value() == value
    assert set_resp.value_as_bytes() == str_to_bytes(value)

    get_resp = client.get(_TEST_CACHE_NAME, key)
    assert get_resp.status() == CacheGetStatus.HIT
    assert get_resp.value() == value
    assert get_resp.value_as_bytes() == str_to_bytes(value)


def test_set_and_get_with_byte_key_values(client: SimpleCacheClient):
    key = uuid_bytes()
    value = uuid_bytes()

    set_resp = client.set(_TEST_CACHE_NAME, key, value)
    assert set_resp.value_as_bytes() == value

    get_resp = client.get(_TEST_CACHE_NAME, key)
    assert get_resp.status() == CacheGetStatus.HIT
    assert get_resp.value_as_bytes() == value


def test_get_returns_miss(client: SimpleCacheClient):
    key = uuid_str()

    get_resp = client.get(_TEST_CACHE_NAME, key)
    assert get_resp.status() == CacheGetStatus.MISS
    assert get_resp.value_as_bytes() is None
    assert get_resp.value() is None


def test_expires_items_after_ttl(client: SimpleCacheClient):
    key = uuid_str()
    val = uuid_str()

    client.set(_TEST_CACHE_NAME, key, val, 2)
    get_response = client.get(_TEST_CACHE_NAME, key)
    assert get_response.status() == CacheGetStatus.HIT

    time.sleep(4)
    get_response = client.get(_TEST_CACHE_NAME, key)
    assert get_response.status() == CacheGetStatus.MISS


def test_set_with_different_ttl(client: SimpleCacheClient):
    key1 = uuid_str()
    key2 = uuid_str()

    client.set(_TEST_CACHE_NAME, key1, "1", 2)
    client.set(_TEST_CACHE_NAME, key2, "2")

    # Before
    get_response = client.get(_TEST_CACHE_NAME, key1)
    assert get_response.status() == CacheGetStatus.HIT
    get_response = client.get(_TEST_CACHE_NAME, key2)
    assert get_response.status() == CacheGetStatus.HIT

    time.sleep(4)

    # After
    get_response = client.get(_TEST_CACHE_NAME, key1)
    assert get_response.status() == CacheGetStatus.MISS
    get_response = client.get(_TEST_CACHE_NAME, key2)
    assert get_response.status() == CacheGetStatus.HIT


# Set
def test_set_with_non_existent_cache_name_throws_not_found(
    client: SimpleCacheClient,
):
    cache_name = uuid_str()
    with pytest.raises(errors.NotFoundError):
        client.set(cache_name, "foo", "bar")


def test_set_with_null_cache_name_throws_exception(client: SimpleCacheClient):
    with pytest.raises(errors.InvalidArgumentError) as cm:
        client.set(None, "foo", "bar")
        assert cm.exception == "Cache name must be a non-empty string"


def test_set_with_empty_cache_name_throws_exception(client: SimpleCacheClient):
    with pytest.raises(errors.BadRequestError) as cm:
        client.set("", "foo", "bar")
        assert cm.exception == "Cache header is empty"


def test_set_with_null_key_throws_exception(client: SimpleCacheClient):
    with pytest.raises(errors.InvalidArgumentError):
        client.set(_TEST_CACHE_NAME, None, "bar")


def test_set_with_null_value_throws_exception(client: SimpleCacheClient):
    with pytest.raises(errors.InvalidArgumentError):
        client.set(_TEST_CACHE_NAME, "foo", None)


def test_set_negative_ttl_throws_exception(client: SimpleCacheClient):
    with pytest.raises(errors.InvalidArgumentError) as cm:
        client.set(_TEST_CACHE_NAME, "foo", "bar", -1)
        assert cm.exception == "TTL Seconds must be a non-negative integer"


def test_set_with_bad_cache_name_throws_exception(client: SimpleCacheClient):
    with pytest.raises(errors.InvalidArgumentError) as cm:
        client.set(1, "foo", "bar")
        assert cm.exception == "Cache name must be a non-empty string"


def test_set_with_bad_key_throws_exception(client: SimpleCacheClient):
    with pytest.raises(errors.InvalidArgumentError) as cm:
        client.set(_TEST_CACHE_NAME, 1, "bar")
        assert cm.exception == "Unsupported type for key: <class 'int'>"


def test_set_with_bad_value_throws_exception(client: SimpleCacheClient):
    with pytest.raises(errors.InvalidArgumentError) as cm:
        client.set(_TEST_CACHE_NAME, "foo", 1)
        assert cm.exception == "Unsupported type for value: <class 'int'>"


def test_set_throws_authentication_exception_for_bad_token():
    with simple_cache_client.init(_BAD_AUTH_TOKEN, _DEFAULT_TTL_SECONDS) as client:
        with pytest.raises(errors.AuthenticationError):
            client.set(_TEST_CACHE_NAME, "foo", "bar")


def test_set_throws_timeout_error_for_short_request_timeout():
    with simple_cache_client.init(
        _AUTH_TOKEN, _DEFAULT_TTL_SECONDS, request_timeout_ms=1
    ) as client:
        with pytest.raises(errors.TimeoutError):
            client.set(_TEST_CACHE_NAME, "foo", "bar")


# Get
def test_get_with_non_existent_cache_name_throws_not_found(
    client: SimpleCacheClient,
):
    cache_name = uuid_str()
    with pytest.raises(errors.NotFoundError):
        client.get(cache_name, "foo")


def test_get_with_null_cache_name_throws_exception(client: SimpleCacheClient):
    with pytest.raises(errors.InvalidArgumentError) as cm:
        client.get(None, "foo")
        assert cm.exception == "Cache name must be a non-empty string"


def test_get_with_empty_cache_name_throws_exception(client: SimpleCacheClient):
    with pytest.raises(errors.BadRequestError) as cm:
        client.get("", "foo")
        assert cm.exception == "Cache header is empty"


def test_get_with_null_key_throws_exception(client: SimpleCacheClient):
    with pytest.raises(errors.InvalidArgumentError):
        client.get(_TEST_CACHE_NAME, None)


def test_get_with_bad_cache_name_throws_exception(client: SimpleCacheClient):
    with pytest.raises(errors.InvalidArgumentError) as cm:
        client.get(1, "foo")
        assert cm.exception == "Cache name must be a non-empty string"


def test_get_with_bad_key_throws_exception(client: SimpleCacheClient):
    with pytest.raises(errors.InvalidArgumentError) as cm:
        client.get(_TEST_CACHE_NAME, 1)
        assert cm.exception == "Unsupported type for key: <class 'int'>"


def test_get_throws_authentication_exception_for_bad_token():
    with simple_cache_client.init(_BAD_AUTH_TOKEN, _DEFAULT_TTL_SECONDS) as client:
        with pytest.raises(errors.AuthenticationError):
            client.get(_TEST_CACHE_NAME, "foo")


def test_get_throws_timeout_error_for_short_request_timeout():
    with simple_cache_client.init(
        _AUTH_TOKEN, _DEFAULT_TTL_SECONDS, request_timeout_ms=1
    ) as client:
        with pytest.raises(errors.TimeoutError):
            client.get(_TEST_CACHE_NAME, "foo")


# Multi op tests
def test_get_multi_and_set(client: SimpleCacheClient):
    items = [(uuid_str(), uuid_str()) for _ in range(5)]
    set_resp = client.set_multi(cache_name=_TEST_CACHE_NAME, items=dict(items))
    assert dict(items) == set_resp.items()

    get_resp = client.get_multi(
        _TEST_CACHE_NAME, items[4][0], items[0][0], items[1][0], items[2][0]
    )
    values = get_resp.values()
    assert items[4][1] == values[0]
    assert items[0][1] == values[1]
    assert items[1][1] == values[2]
    assert items[2][1] == values[3]


def test_get_multi_failure():
    # Start with a cache client with impossibly small request timeout to force failures
    with simple_cache_client.init(
        _AUTH_TOKEN, _DEFAULT_TTL_SECONDS, request_timeout_ms=1
    ) as client:
        with pytest.raises(errors.TimeoutError):
            client.get_multi(
                _TEST_CACHE_NAME, "key1", "key2", "key3", "key4", "key5", "key6"
            )


def test_set_multi_failure(client: SimpleCacheClient):
    # Start with a cache client with impossibly small request timeout to force failures
    with simple_cache_client.init(
        _AUTH_TOKEN, _DEFAULT_TTL_SECONDS, request_timeout_ms=1
    ) as client:
        with pytest.raises(errors.TimeoutError):
            client.set_multi(
                cache_name=_TEST_CACHE_NAME,
                items={
                    "fizz1": "buzz1",
                    "fizz2": "buzz2",
                    "fizz3": "buzz3",
                    "fizz4": "buzz4",
                    "fizz5": "buzz5",
                },
            )


# Test delete for key that doesn't exist
def test_delete_key_doesnt_exist(client: SimpleCacheClient):
    key = uuid_str()
    get_response = client.get(_TEST_CACHE_NAME, key)
    assert get_response.status() == CacheGetStatus.MISS

    client.delete(_TEST_CACHE_NAME, key)
    get_response = client.get(_TEST_CACHE_NAME, key)
    get_response.status() == CacheGetStatus.MISS


# Test delete
def test_delete(client: SimpleCacheClient):
    # Set an item to then delete...
    key, value = uuid_str(), uuid_str()
    get_response = client.get(_TEST_CACHE_NAME, key)
    assert get_response.status() == CacheGetStatus.MISS
    client.set(_TEST_CACHE_NAME, key, value)

    get_response = client.get(_TEST_CACHE_NAME, key)
    assert get_response.status() == CacheGetStatus.HIT

    # Delete
    client.delete(_TEST_CACHE_NAME, key)

    # Verify deleted
    get_response = client.get(_TEST_CACHE_NAME, key)
    assert get_response.status() == CacheGetStatus.MISS

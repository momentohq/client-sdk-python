import time
from datetime import timedelta
from functools import partial
from typing import Optional

from pytest import fixture
from pytest_describe import behaves_like
from typing_extensions import Protocol

from momento import SimpleCacheClient
from momento.errors import MomentoErrorCode
from momento.responses import CacheDelete, CacheGet, CacheSet, CacheSetIfNotExists
from momento.responses.mixins import ErrorResponseMixin
from momento.responses.response import CacheResponse
from momento.typing import TCacheName, TScalarKey, TScalarValue
from tests.utils import str_to_bytes, uuid_bytes, uuid_str

from .shared_behaviors import (
    TCacheNameValidator,
    TConnectionValidator,
    TKeyValidator,
    a_cache_name_validator,
    a_connection_validator,
    a_key_validator,
)


class TSetter(Protocol):
    def __call__(
        self, cache_name: TCacheName, key: TScalarKey, value: TScalarValue, ttl: Optional[timedelta] = None
    ) -> CacheResponse:
        ...


def a_setter() -> None:
    def expires_items_after_ttl(setter: TSetter, client: SimpleCacheClient, cache_name: str) -> None:
        key = uuid_str()
        val = uuid_str()

        setter(cache_name, key, val, ttl=timedelta(seconds=2))
        get_response = client.get(cache_name, key)
        assert isinstance(get_response, CacheGet.Hit)

        time.sleep(4)
        get_response = client.get(cache_name, key)
        assert isinstance(get_response, CacheGet.Miss)

    def with_different_ttl(setter: TSetter, client: SimpleCacheClient, cache_name: str) -> None:
        key1 = uuid_str()
        key2 = uuid_str()

        setter(cache_name, key1, "1", ttl=timedelta(seconds=2))
        setter(cache_name, key2, "2")

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

    def with_null_value_throws_exception(setter: TSetter, client: SimpleCacheClient, cache_name: str) -> None:
        set_response = setter(cache_name, "foo", None)
        if isinstance(set_response, ErrorResponseMixin):
            assert set_response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
        else:
            assert False

    def negative_ttl_throws_exception(setter: TSetter, client: SimpleCacheClient, cache_name: str) -> None:
        set_response = setter(cache_name, "foo", "bar", ttl=timedelta(seconds=-1))
        if isinstance(set_response, ErrorResponseMixin):
            assert set_response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
            assert set_response.inner_exception.message == "TTL must be a positive amount of time."
        else:
            assert False

    def with_bad_value_throws_exception(setter: TSetter, client: SimpleCacheClient, cache_name: str) -> None:
        set_response = setter(cache_name, "foo", 1)
        if isinstance(set_response, ErrorResponseMixin):
            assert set_response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
            assert set_response.inner_exception.message == "Unsupported type for value: <class 'int'>"
        else:
            assert False


def describe_set_and_get() -> None:
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
@behaves_like(a_connection_validator)
def describe_get() -> None:
    @fixture
    def cache_name_validator(client: SimpleCacheClient) -> TCacheNameValidator:
        key = uuid_str()
        return partial(client.get, key=key)

    @fixture
    def key_validator(client: SimpleCacheClient) -> TKeyValidator:
        return partial(client.get)

    @fixture
    def connection_validator() -> TConnectionValidator:
        def _connection_validator(client: SimpleCacheClient, cache_name: str) -> ErrorResponseMixin:
            key = uuid_str()
            return client.get(cache_name, key)

        return _connection_validator

    def returns_miss(client: SimpleCacheClient, cache_name: str) -> None:
        key = uuid_str()

        get_resp = client.get(cache_name, key)
        assert isinstance(get_resp, CacheGet.Miss)


@behaves_like(a_cache_name_validator)
@behaves_like(a_key_validator)
@behaves_like(a_connection_validator)
@behaves_like(a_setter)
def describe_set() -> None:
    @fixture
    def cache_name_validator(client: SimpleCacheClient) -> TCacheNameValidator:
        key = uuid_str()
        value = uuid_str()
        return partial(client.set, key=key, value=value)

    @fixture
    def key_validator(client: SimpleCacheClient) -> TKeyValidator:
        value = uuid_str()
        return partial(client.set, value=value)

    @fixture
    def connection_validator() -> TConnectionValidator:
        def _connection_validator(client: SimpleCacheClient, cache_name: str) -> ErrorResponseMixin:
            key = uuid_str()
            value = uuid_str()
            return client.set(cache_name, key, value)

        return _connection_validator

    @fixture
    def setter(client: SimpleCacheClient) -> TSetter:
        return partial(client.set)


@behaves_like(a_cache_name_validator)
@behaves_like(a_key_validator)
@behaves_like(a_connection_validator)
@behaves_like(a_setter)
def describe_set_if_not_exists() -> None:
    @fixture
    def cache_name_validator(client: SimpleCacheClient) -> TCacheNameValidator:
        key = uuid_str()
        value = uuid_str()
        return partial(client.set_if_not_exists, key=key, value=value)

    @fixture
    def key_validator(client: SimpleCacheClient) -> TKeyValidator:
        value = uuid_str()
        return partial(client.set_if_not_exists, value=value)

    @fixture
    def connection_validator() -> TConnectionValidator:
        def _connection_validator(client: SimpleCacheClient, cache_name: str) -> ErrorResponseMixin:
            key = uuid_str()
            value = uuid_str()
            return client.set_if_not_exists(cache_name, key, value)

        return _connection_validator

    @fixture
    def setter(client: SimpleCacheClient) -> TSetter:
        return partial(client.set_if_not_exists)

    def it_only_sets_when_the_key_does_not_exist(
        client: SimpleCacheClient,
        cache_name: TCacheName,
    ) -> None:
        key = uuid_str()
        value = uuid_str()

        get_resp = client.get(cache_name, key)
        assert isinstance(get_resp, CacheGet.Miss)

        set_resp = client.set_if_not_exists(cache_name, key, value)
        assert isinstance(set_resp, CacheSetIfNotExists.Stored)

        get_resp = client.get(cache_name, key)
        assert isinstance(get_resp, CacheGet.Hit)
        assert get_resp.value_string == str(value)

        set_resp = client.set_if_not_exists(cache_name, key, uuid_str())
        assert isinstance(set_resp, CacheSetIfNotExists.NotStored)

        get_resp = client.get(cache_name, key)
        assert isinstance(get_resp, CacheGet.Hit)
        assert get_resp.value_string == str(value)


@behaves_like(a_cache_name_validator)
@behaves_like(a_key_validator)
@behaves_like(a_connection_validator)
def describe_delete() -> None:
    @fixture
    def cache_name_validator(client: SimpleCacheClient) -> TCacheNameValidator:
        key = uuid_str()
        return partial(client.delete, key=key)

    @fixture
    def key_validator(client: SimpleCacheClient) -> TKeyValidator:
        return partial(client.delete)

    @fixture
    def connection_validator() -> TConnectionValidator:
        def _connection_validator(client: SimpleCacheClient, cache_name: str) -> ErrorResponseMixin:
            key = uuid_str()
            return client.delete(cache_name, key)

        return _connection_validator

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

import time
from datetime import timedelta
from functools import partial
from typing import Awaitable, Optional

from pytest import fixture
from pytest_describe import behaves_like
from typing_extensions import Protocol

from momento import SimpleCacheClientAsync
from momento.errors import MomentoErrorCode
from momento.responses import CacheDelete, CacheGet, CacheSet, CacheSetIfNotExists
from momento.responses.mixins import ErrorResponseMixin
from momento.responses.response import CacheResponse
from momento.typing import TCacheName, TScalarKey, TScalarValue
from tests.utils import str_to_bytes, uuid_bytes, uuid_str

from .shared_behaviors_async import (
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
    ) -> Awaitable[CacheResponse]:
        ...


def a_setter() -> None:
    async def expires_items_after_ttl(setter: TSetter, client_async: SimpleCacheClientAsync, cache_name: str) -> None:
        key = uuid_str()
        val = uuid_str()

        await setter(cache_name, key, val, timedelta(seconds=2))
        get_response = await client_async.get(cache_name, key)
        assert isinstance(get_response, CacheGet.Hit)

        time.sleep(4)
        get_response = await client_async.get(cache_name, key)
        assert isinstance(get_response, CacheGet.Miss)

    async def with_different_ttl(setter: TSetter, client_async: SimpleCacheClientAsync, cache_name: str) -> None:
        key1 = uuid_str()
        key2 = uuid_str()

        await setter(cache_name, key1, "1", timedelta(seconds=2))
        await setter(cache_name, key2, "2")

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

    async def with_null_value_throws_exception(
        setter: TSetter, client_async: SimpleCacheClientAsync, cache_name: str
    ) -> None:
        set_response = await setter(cache_name, "foo", None)
        if isinstance(set_response, ErrorResponseMixin):
            assert set_response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
        else:
            assert False

    async def negative_ttl_throws_exception(
        setter: TSetter, client_async: SimpleCacheClientAsync, cache_name: str
    ) -> None:
        set_response = await setter(cache_name, "foo", "bar", timedelta(seconds=-1))
        if isinstance(set_response, ErrorResponseMixin):
            assert set_response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
            assert set_response.inner_exception.message == "TTL must be a positive amount of time."
        else:
            assert False

    async def with_bad_value_throws_exception(
        setter: TSetter, client_async: SimpleCacheClientAsync, cache_name: str
    ) -> None:
        set_response = await setter(cache_name, "foo", 1)
        if isinstance(set_response, ErrorResponseMixin):
            assert set_response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
            assert set_response.inner_exception.message == "Unsupported type for value: <class 'int'>"
        else:
            assert False


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
    def cache_name_validator(client_async: SimpleCacheClientAsync) -> TCacheNameValidator:
        key = uuid_str()
        return partial(client_async.get, key=key)

    @fixture
    def key_validator(client_async: SimpleCacheClientAsync) -> TKeyValidator:
        return partial(client_async.get)

    @fixture
    def connection_validator() -> TConnectionValidator:
        async def _connection_validator(client_async: SimpleCacheClientAsync, cache_name: str) -> ErrorResponseMixin:
            key = uuid_str()
            return await client_async.get(cache_name, key)

        return _connection_validator

    async def returns_miss(client_async: SimpleCacheClientAsync, cache_name: str) -> None:
        key = uuid_str()

        get_resp = await client_async.get(cache_name, key)
        assert isinstance(get_resp, CacheGet.Miss)


@behaves_like(a_cache_name_validator)
@behaves_like(a_key_validator)
@behaves_like(a_connection_validator)
@behaves_like(a_setter)
def describe_set() -> None:
    @fixture
    def cache_name_validator(client_async: SimpleCacheClientAsync) -> TCacheNameValidator:
        key = uuid_str()
        value = uuid_str()
        return partial(client_async.set, key=key, value=value)

    @fixture
    def key_validator(client_async: SimpleCacheClientAsync) -> TKeyValidator:
        value = uuid_str()
        return partial(client_async.set, value=value)

    @fixture
    def connection_validator() -> TConnectionValidator:
        async def _connection_validator(client_async: SimpleCacheClientAsync, cache_name: str) -> ErrorResponseMixin:
            key = uuid_str()
            value = uuid_str()
            return await client_async.set(cache_name, key, value)

        return _connection_validator

    @fixture
    def setter(client_async: SimpleCacheClientAsync) -> TSetter:
        return partial(client_async.set)


@behaves_like(a_cache_name_validator)
@behaves_like(a_key_validator)
@behaves_like(a_connection_validator)
@behaves_like(a_setter)
def describe_set_if_not_exists() -> None:
    @fixture
    def cache_name_validator(client_async: SimpleCacheClientAsync) -> TCacheNameValidator:
        key = uuid_str()
        value = uuid_str()
        return partial(client_async.set_if_not_exists, key=key, value=value)

    @fixture
    def key_validator(client_async: SimpleCacheClientAsync) -> TKeyValidator:
        value = uuid_str()
        return partial(client_async.set_if_not_exists, value=value)

    @fixture
    def connection_validator() -> TConnectionValidator:
        async def _connection_validator(client_async: SimpleCacheClientAsync, cache_name: str) -> ErrorResponseMixin:
            key = uuid_str()
            value = uuid_str()
            return await client_async.set_if_not_exists(cache_name, key, value)

        return _connection_validator

    @fixture
    def setter(client_async: SimpleCacheClientAsync) -> TSetter:
        return partial(client_async.set_if_not_exists)

    async def it_only_sets_when_the_key_does_not_exist(
        client_async: SimpleCacheClientAsync,
        cache_name: TCacheName,
    ) -> None:
        key = uuid_str()
        value = uuid_str()

        get_resp = await client_async.get(cache_name, key)
        assert isinstance(get_resp, CacheGet.Miss)

        set_resp = await client_async.set_if_not_exists(cache_name, key, value)
        assert isinstance(set_resp, CacheSetIfNotExists.Stored)

        get_resp = await client_async.get(cache_name, key)
        assert isinstance(get_resp, CacheGet.Hit)
        assert get_resp.value_string == str(value)

        set_resp = await client_async.set_if_not_exists(cache_name, key, uuid_str())
        assert isinstance(set_resp, CacheSetIfNotExists.NotStored)

        get_resp = await client_async.get(cache_name, key)
        assert isinstance(get_resp, CacheGet.Hit)
        assert get_resp.value_string == str(value)


@behaves_like(a_cache_name_validator)
@behaves_like(a_key_validator)
@behaves_like(a_connection_validator)
def describe_delete() -> None:
    @fixture
    def cache_name_validator(client_async: SimpleCacheClientAsync) -> TCacheNameValidator:
        key = uuid_str()
        return partial(client_async.delete, key=key)

    @fixture
    def key_validator(client_async: SimpleCacheClientAsync) -> TKeyValidator:
        return partial(client_async.delete)

    @fixture
    def connection_validator() -> TConnectionValidator:
        async def _connection_validator(client_async: SimpleCacheClientAsync, cache_name: str) -> ErrorResponseMixin:
            key = uuid_str()
            return await client_async.delete(cache_name, key)

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

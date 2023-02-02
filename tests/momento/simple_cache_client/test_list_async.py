from collections import Counter
from datetime import timedelta
from functools import partial
from time import sleep
from typing import Awaitable, Callable

import pytest
from pytest import fixture
from pytest_describe import behaves_like

from momento import SimpleCacheClientAsync
from momento.auth import EnvMomentoTokenProvider
from momento.config import Configuration
from momento.errors import InvalidArgumentException, MomentoErrorCode
from momento.requests import CollectionTtl
from momento.responses import (
    CacheListConcatenateBack,
    CacheListConcatenateFront,
    CacheListFetch,
    CacheListLength,
    CacheListPopBack,
    CacheListPopFront,
    CacheListPushBack,
    CacheListPushFront,
    CacheResponse,
)
from momento.responses.mixins import ErrorResponseMixin
from momento.typing import (
    TCacheName,
    TListName,
    TListValue,
    TListValuesInput,
    TListValuesInputBytes,
    TListValuesInputStr,
)
from tests.utils import uuid_bytes, uuid_str

from .shared_behaviors_async import (
    TCacheNameValidator,
    TConnectionValidator,
    a_cache_name_validator,
    a_connection_validator,
)

TListAdder = Callable[[SimpleCacheClientAsync, TListName, TListValue, CollectionTtl], Awaitable[CacheResponse]]


def a_list_adder() -> None:
    async def it_sets_the_ttl(
        configuration: Configuration,
        credential_provider: EnvMomentoTokenProvider,
        list_adder: TListAdder,
        cache_name: TCacheName,
        list_name: TListName,
        values: TListValuesInput,
    ) -> None:
        async with SimpleCacheClientAsync(configuration, credential_provider, timedelta(hours=1)) as client:
            ttl_seconds = 0.5
            ttl = CollectionTtl(ttl=timedelta(seconds=ttl_seconds), refresh_ttl=False)

            for value in values:
                await list_adder(client, list_name, value, ttl)

            sleep(ttl_seconds * 2)

            fetch_resp = await client.list_fetch(cache_name, list_name)
            assert isinstance(fetch_resp, CacheListFetch.Miss)

    async def it_refreshes_the_ttl(
        client_async: SimpleCacheClientAsync, list_adder: TListAdder, cache_name: TCacheName, list_name: TListName
    ) -> None:
        ttl_seconds = 1
        ttl = CollectionTtl.of(timedelta(seconds=ttl_seconds))
        values = ["one", "two", "three", "four"]

        for value in values:
            await list_adder(client_async, list_name, value, ttl)
            sleep(ttl_seconds / 2)

        fetch_resp = await client_async.list_fetch(cache_name, list_name)
        assert isinstance(fetch_resp, CacheListFetch.Hit)
        assert Counter(fetch_resp.values_string) == Counter(values)

    async def it_uses_the_default_ttl_when_the_collection_ttl_has_no_ttl(
        configuration: Configuration,
        credential_provider: EnvMomentoTokenProvider,
        list_adder: TListAdder,
        cache_name: TCacheName,
        list_name: TListName,
    ) -> None:
        ttl_seconds = 1
        async with SimpleCacheClientAsync(configuration, credential_provider, timedelta(seconds=ttl_seconds)) as client:
            ttl = CollectionTtl.from_cache_ttl().with_no_refresh_ttl_on_updates()

            value = uuid_str()
            await list_adder(client, list_name, value, ttl)

            sleep(ttl_seconds / 2)

            fetch_resp = await client.list_fetch(cache_name, list_name)
            assert isinstance(fetch_resp, CacheListFetch.Hit)
            assert fetch_resp.values_string == [value]

            sleep(ttl_seconds / 2)
            fetch_resp = await client.list_fetch(cache_name, list_name)
            assert isinstance(fetch_resp, CacheListFetch.Miss)


TListConcatenator = Callable[[TCacheName, TListName, TListValuesInput], Awaitable[CacheResponse]]


def a_list_concatenator() -> None:
    async def it_returns_the_new_list_length(
        list_concatenator: TListConcatenator,
        client_async: SimpleCacheClientAsync,
        cache_name: TCacheName,
        list_name: TListName,
        values: TListValuesInput,
    ) -> None:
        resp = await list_concatenator(cache_name, list_name, values)
        length = len(values)
        assert resp.list_length == length

        resp = await list_concatenator(cache_name, list_name, values)
        length += len(values)
        assert resp.list_length == length

    async def with_bytes_it_succeeds(
        list_concatenator: TListConcatenator,
        client_async: SimpleCacheClientAsync,
        cache_name: TCacheName,
        list_name: TListName,
        values_bytes: TListValuesInputBytes,
    ) -> None:
        concat_resp = await list_concatenator(cache_name, list_name, values_bytes)
        assert concat_resp.list_length == len(values_bytes)

        fetch_resp = await client_async.list_fetch(cache_name, list_name)
        assert isinstance(fetch_resp, CacheListFetch.Hit)
        assert fetch_resp.values_bytes == values_bytes

    async def with_strings_it_succeeds(
        list_concatenator: TListConcatenator,
        client_async: SimpleCacheClientAsync,
        cache_name: TCacheName,
        list_name: TListName,
        values_str: TListValuesInputStr,
    ) -> None:
        concat_resp = await list_concatenator(cache_name, list_name, values_str)
        assert concat_resp.list_length == len(values_str)

        fetch_resp = await client_async.list_fetch(cache_name, list_name)
        assert isinstance(fetch_resp, CacheListFetch.Hit)
        assert fetch_resp.values_string == values_str

    async def with_iterable_values_it_succeeds(
        list_concatenator: TListConcatenator,
        client_async: SimpleCacheClientAsync,
        cache_name: TCacheName,
        list_name: TListName,
        values_str: TListValuesInputStr,
    ) -> None:
        iterator = iter(values_str)
        concat_resp = await list_concatenator(cache_name, list_name, iterator)
        assert concat_resp.list_length == len(values_str)

        fetch_resp = await client_async.list_fetch(cache_name, list_name)
        assert isinstance(fetch_resp, CacheListFetch.Hit)
        assert fetch_resp.values_string == values_str

    async def with_other_values_type_it_errors(
        list_concatenator: TListConcatenator,
        client_async: SimpleCacheClientAsync,
        cache_name: TCacheName,
        list_name: TListName,
    ) -> None:
        resp = await list_concatenator(cache_name, list_name, 234)
        assert isinstance(resp, ErrorResponseMixin)
        assert resp.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
        assert resp.message == "Invalid argument passed to Momento client: Unsupported type for values: <class 'int'>"


TListNameValidator = Callable[[TListName], Awaitable[CacheResponse]]


def a_list_name_validator() -> None:
    async def with_null_list_name_it_returns_invalid(
        list_name_validator: TListNameValidator, cache_name: TCacheName
    ) -> None:
        response = await list_name_validator(cache_name, None)  # type: ignore
        assert isinstance(response, ErrorResponseMixin)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
        assert response.inner_exception.message == "List name must be a string"

    async def with_empty_list_name_it_returns_invalid(
        list_name_validator: TListNameValidator, cache_name: TCacheName
    ) -> None:
        response = await list_name_validator(cache_name, "")
        assert isinstance(response, ErrorResponseMixin)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
        assert response.inner_exception.message == "List name must not be empty"

    async def with_bad_list_name_it_returns_invalid(
        list_name_validator: TCacheNameValidator, cache_name: TCacheName
    ) -> None:
        response = await list_name_validator(cache_name, 1)  # type: ignore
        assert isinstance(response, ErrorResponseMixin)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
        assert response.inner_exception.message == "List name must be a string"


TListPusher = Callable[[TCacheName, TListName, TListValue], Awaitable[CacheResponse]]


def a_list_pusher() -> None:
    async def it_returns_the_new_list_length(
        list_pusher: TListPusher,
        client_async: SimpleCacheClientAsync,
        cache_name: TCacheName,
        list_name: TListName,
        values: TListValuesInput,
    ) -> None:
        length = 0
        for value in values:
            resp = await list_pusher(cache_name, list_name, value)
            length += 1
            assert resp.list_length == length

    async def with_bytes_it_succeeds(
        list_pusher: TListConcatenator,
        client_async: SimpleCacheClientAsync,
        cache_name: TCacheName,
        list_name: TListName,
    ) -> None:
        value = uuid_bytes()
        concat_resp = await list_pusher(cache_name, list_name, value)
        assert concat_resp.list_length == 1

        fetch_resp = await client_async.list_fetch(cache_name, list_name)
        assert isinstance(fetch_resp, CacheListFetch.Hit)
        assert fetch_resp.values_bytes == [value]

    async def with_strings_it_succeeds(
        list_pusher: TListConcatenator,
        client_async: SimpleCacheClientAsync,
        cache_name: TCacheName,
        list_name: TListName,
    ) -> None:
        value = uuid_str()
        concat_resp = await list_pusher(cache_name, list_name, value)
        assert concat_resp.list_length == 1

        fetch_resp = await client_async.list_fetch(cache_name, list_name)
        assert isinstance(fetch_resp, CacheListFetch.Hit)
        assert fetch_resp.values_string == [value]

    async def with_other_value_type_it_errors(
        list_pusher: TListPusher,
        client_async: SimpleCacheClientAsync,
        cache_name: TCacheName,
        list_name: TListName,
    ) -> None:
        resp = await list_pusher(cache_name, list_name, 234)
        assert isinstance(resp, ErrorResponseMixin)
        assert resp.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
        assert resp.message == "Invalid argument passed to Momento client: Unsupported type for value: <class 'int'>"


@behaves_like(a_cache_name_validator)
@behaves_like(a_connection_validator)
@behaves_like(a_list_adder)
@behaves_like(a_list_name_validator)
@behaves_like(a_list_concatenator)
def describe_list_concatenate_back() -> None:
    @fixture
    def cache_name_validator(client_async: SimpleCacheClientAsync) -> TCacheNameValidator:
        list_name = uuid_str()
        return partial(client_async.list_concatenate_back, list_name=list_name, values=[uuid_str()])

    @fixture
    def connection_validator() -> TConnectionValidator:
        async def _connection_validator(
            client_async: SimpleCacheClientAsync, cache_name: TCacheName
        ) -> ErrorResponseMixin:
            list_name = uuid_str()
            return await client_async.list_concatenate_back(cache_name, list_name, [uuid_str()])

        return _connection_validator

    @fixture
    def list_adder(
        client_async: SimpleCacheClientAsync, cache_name: TCacheName, list_name: TListName, list_value: TListValue
    ) -> TListAdder:
        async def _list_adder(
            client_async: SimpleCacheClientAsync,
            list_name: TListName,
            list_value: TListValue,
            ttl: CollectionTtl,
        ) -> CacheResponse:
            return await client_async.list_concatenate_back(cache_name, list_name, [list_value], ttl=ttl)

        return _list_adder

    @fixture
    def list_name_validator(
        client_async: SimpleCacheClientAsync, cache_name: TCacheName, list_name: TListName, values: TListValuesInput
    ) -> TListNameValidator:
        return partial(client_async.list_concatenate_back, values=values)

    @fixture
    def list_concatenator(
        client_async: SimpleCacheClientAsync, cache_name: TCacheName, list_name: TListName, values: TListValuesInput
    ) -> TListConcatenator:
        return partial(client_async.list_concatenate_back)

    async def it_truncates_the_front(
        client_async: SimpleCacheClientAsync,
        cache_name: TCacheName,
        list_name: TListName,
    ) -> None:
        truncate_to = 4

        concat_resp = await client_async.list_concatenate_back(
            cache_name=cache_name,
            list_name=list_name,
            values=["one", "two", "three"],
            truncate_front_to_size=truncate_to,
        )
        assert isinstance(concat_resp, CacheListConcatenateBack.Success)
        assert concat_resp.list_length <= truncate_to

        concat_resp = await client_async.list_concatenate_back(
            cache_name=cache_name,
            list_name=list_name,
            values=["four", "five", "six"],
            truncate_front_to_size=truncate_to,
        )
        assert isinstance(concat_resp, CacheListConcatenateBack.Success)
        assert concat_resp.list_length <= truncate_to

        fetch_resp = await client_async.list_fetch(cache_name, list_name)
        assert fetch_resp.values_string == ["three", "four", "five", "six"]


@behaves_like(a_cache_name_validator)
@behaves_like(a_connection_validator)
@behaves_like(a_list_adder)
@behaves_like(a_list_name_validator)
@behaves_like(a_list_concatenator)
def describe_list_concatenate_front() -> None:
    @fixture
    def cache_name_validator(client_async: SimpleCacheClientAsync) -> TCacheNameValidator:
        list_name = uuid_str()
        return partial(client_async.list_concatenate_front, list_name=list_name, values=[uuid_str()])

    @fixture
    def connection_validator() -> TConnectionValidator:
        async def _connection_validator(
            client_async: SimpleCacheClientAsync, cache_name: TCacheName
        ) -> ErrorResponseMixin:
            list_name = uuid_str()
            return await client_async.list_concatenate_front(cache_name, list_name, [uuid_str()])

        return _connection_validator

    @fixture
    def list_adder(
        client_async: SimpleCacheClientAsync, cache_name: TCacheName, list_name: TListName, list_value: TListValue
    ) -> TListAdder:
        async def _list_adder(
            client_async: SimpleCacheClientAsync,
            list_name: TListName,
            list_value: TListValue,
            ttl: CollectionTtl,
        ) -> CacheResponse:
            return await client_async.list_concatenate_front(cache_name, list_name, [list_value], ttl=ttl)

        return _list_adder

    @fixture
    def list_name_validator(
        client_async: SimpleCacheClientAsync, cache_name: TCacheName, list_name: TListName, values: TListValuesInput
    ) -> TListNameValidator:
        return partial(client_async.list_concatenate_front, values=values)

    @fixture
    def list_concatenator(
        client_async: SimpleCacheClientAsync, cache_name: TCacheName, list_name: TListName, values: TListValuesInput
    ) -> TListConcatenator:
        return partial(client_async.list_concatenate_front)

    async def it_truncates_the_back(
        client_async: SimpleCacheClientAsync,
        cache_name: TCacheName,
        list_name: TListName,
    ) -> None:
        truncate_to = 4

        concat_resp = await client_async.list_concatenate_front(
            cache_name=cache_name,
            list_name=list_name,
            values=["one", "two", "three"],
            truncate_back_to_size=truncate_to,
        )
        assert isinstance(concat_resp, CacheListConcatenateFront.Success)
        assert concat_resp.list_length <= truncate_to

        concat_resp = await client_async.list_concatenate_front(
            cache_name=cache_name,
            list_name=list_name,
            values=["four", "five", "six"],
            truncate_back_to_size=truncate_to,
        )
        assert isinstance(concat_resp, CacheListConcatenateFront.Success)
        assert concat_resp.list_length <= truncate_to

        fetch_resp = await client_async.list_fetch(cache_name, list_name)
        assert fetch_resp.values_string == ["four", "five", "six", "one"]


@behaves_like(a_cache_name_validator)
@behaves_like(a_connection_validator)
@behaves_like(a_list_name_validator)
def describe_list_fetch() -> None:
    @fixture
    def cache_name_validator(client_async: SimpleCacheClientAsync) -> TCacheNameValidator:
        list_name = uuid_str()
        return partial(client_async.list_fetch, list_name=list_name)

    @fixture
    def connection_validator() -> TConnectionValidator:
        async def _connection_validator(
            client_async: SimpleCacheClientAsync, cache_name: TCacheName
        ) -> ErrorResponseMixin:
            list_name = uuid_str()
            return await client_async.list_fetch(cache_name, list_name)

        return _connection_validator

    @fixture
    def list_name_validator(
        client_async: SimpleCacheClientAsync, cache_name: TCacheName, list_name: TListName
    ) -> TListNameValidator:
        return partial(client_async.list_fetch)

    async def misses_when_the_list_does_not_exist(
        client_async: SimpleCacheClientAsync, cache_name: TCacheName, list_name: TListName
    ) -> None:
        resp = await client_async.list_fetch(cache_name, list_name)
        assert isinstance(resp, CacheListFetch.Miss)


@behaves_like(a_cache_name_validator)
@behaves_like(a_connection_validator)
@behaves_like(a_list_name_validator)
def describe_list_length() -> None:
    @fixture
    def cache_name_validator(client_async: SimpleCacheClientAsync) -> TCacheNameValidator:
        list_name = uuid_str()
        return partial(client_async.list_length, list_name=list_name)

    @fixture
    def connection_validator() -> TConnectionValidator:
        async def _connection_validator(
            client_async: SimpleCacheClientAsync, cache_name: TCacheName
        ) -> ErrorResponseMixin:
            list_name = uuid_str()
            return await client_async.list_length(cache_name, list_name)

        return _connection_validator

    @fixture
    def list_name_validator(
        client_async: SimpleCacheClientAsync, cache_name: TCacheName, list_name: TListName
    ) -> TListNameValidator:
        return partial(client_async.list_length)

    async def it_returns_the_list_length(
        client_async: SimpleCacheClientAsync, cache_name: TCacheName, list_name: TListName, values: TListValuesInput
    ) -> None:
        await client_async.list_concatenate_back(cache_name, list_name, values)

        resp = await client_async.list_length(cache_name, list_name)
        assert isinstance(resp, CacheListLength.Hit)
        assert resp.length == len(values)

    async def it_misses_when_the_list_does_not_exist(
        client_async: SimpleCacheClientAsync, cache_name: TCacheName, list_name: TListName
    ) -> None:
        resp = await client_async.list_length(cache_name, list_name)
        assert isinstance(resp, CacheListLength.Miss)


@behaves_like(a_cache_name_validator)
@behaves_like(a_connection_validator)
@behaves_like(a_list_name_validator)
def describe_list_pop_back() -> None:
    @fixture
    def cache_name_validator(client_async: SimpleCacheClientAsync) -> TCacheNameValidator:
        list_name = uuid_str()
        return partial(client_async.list_pop_back, list_name=list_name)

    @fixture
    def connection_validator() -> TConnectionValidator:
        async def _connection_validator(
            client_async: SimpleCacheClientAsync, cache_name: TCacheName
        ) -> ErrorResponseMixin:
            list_name = uuid_str()
            return await client_async.list_pop_back(cache_name, list_name)

        return _connection_validator

    @fixture
    def list_name_validator(
        client_async: SimpleCacheClientAsync, cache_name: TCacheName, list_name: TListName
    ) -> TListNameValidator:
        return partial(client_async.list_pop_back)

    async def it_pops_the_back(
        client_async: SimpleCacheClientAsync, cache_name: TCacheName, list_name: TListName
    ) -> None:
        values = ["one", "two", "three"]
        await client_async.list_concatenate_front(cache_name, list_name, values)

        resp = await client_async.list_pop_back(cache_name, list_name)
        assert isinstance(resp, CacheListPopBack.Hit)
        assert resp.value_string == "three"
        assert resp.value_bytes == b"three"

    async def it_misses_when_the_list_does_not_exist(
        client_async: SimpleCacheClientAsync, cache_name: TCacheName, list_name: TListName
    ) -> None:
        resp = await client_async.list_pop_back(cache_name, list_name)
        assert isinstance(resp, CacheListPopBack.Miss)


@behaves_like(a_cache_name_validator)
@behaves_like(a_connection_validator)
@behaves_like(a_list_name_validator)
def describe_list_pop_front() -> None:
    @fixture
    def cache_name_validator(client_async: SimpleCacheClientAsync) -> TCacheNameValidator:
        list_name = uuid_str()
        return partial(client_async.list_pop_front, list_name=list_name)

    @fixture
    def connection_validator() -> TConnectionValidator:
        async def _connection_validator(
            client_async: SimpleCacheClientAsync, cache_name: TCacheName
        ) -> ErrorResponseMixin:
            list_name = uuid_str()
            return await client_async.list_pop_front(cache_name, list_name)

        return _connection_validator

    @fixture
    def list_name_validator(
        client_async: SimpleCacheClientAsync, cache_name: TCacheName, list_name: TListName
    ) -> TListNameValidator:
        return partial(client_async.list_pop_front)

    async def it_pops_the_front(
        client_async: SimpleCacheClientAsync, cache_name: TCacheName, list_name: TListName
    ) -> None:
        values = ["one", "two", "three"]
        await client_async.list_concatenate_front(cache_name, list_name, values)

        resp = await client_async.list_pop_front(cache_name, list_name)
        assert isinstance(resp, CacheListPopFront.Hit)
        assert resp.value_string == "one"
        assert resp.value_bytes == b"one"

    async def it_misses_when_the_list_does_not_exist(
        client_async: SimpleCacheClientAsync, cache_name: TCacheName, list_name: TListName
    ) -> None:
        resp = await client_async.list_pop_front(cache_name, list_name)
        assert isinstance(resp, CacheListPopFront.Miss)


@behaves_like(a_cache_name_validator)
@behaves_like(a_connection_validator)
@behaves_like(a_list_adder)
@behaves_like(a_list_name_validator)
@behaves_like(a_list_pusher)
def describe_list_push_back() -> None:
    @fixture
    def cache_name_validator(client_async: SimpleCacheClientAsync) -> TCacheNameValidator:
        list_name = uuid_str()
        return partial(client_async.list_push_back, list_name=list_name, value=uuid_str())

    @fixture
    def connection_validator() -> TConnectionValidator:
        async def _connection_validator(
            client_async: SimpleCacheClientAsync, cache_name: TCacheName
        ) -> ErrorResponseMixin:
            list_name = uuid_str()
            return await client_async.list_push_back(cache_name=cache_name, list_name=list_name, value=uuid_str())

        return _connection_validator

    @fixture
    def list_adder(
        client_async: SimpleCacheClientAsync, cache_name: TCacheName, list_name: TListName, list_value: TListValue
    ) -> TListAdder:
        async def _list_adder(
            client_async: SimpleCacheClientAsync,
            list_name: TListName,
            list_value: TListValue,
            ttl: CollectionTtl,
        ) -> CacheResponse:
            return await client_async.list_push_back(cache_name, list_name, list_value, ttl=ttl)

        return _list_adder

    @fixture
    def list_name_validator(
        client_async: SimpleCacheClientAsync, cache_name: TCacheName, list_name: TListName
    ) -> TListNameValidator:
        value = uuid_str()
        return partial(client_async.list_push_back, value=value)

    @fixture
    def list_pusher(client_async: SimpleCacheClientAsync, cache_name: TCacheName, list_name: TListName) -> TListPusher:
        return partial(client_async.list_push_back)

    async def it_truncates_the_front(
        client_async: SimpleCacheClientAsync,
        cache_name: TCacheName,
        list_name: TListName,
    ) -> None:
        values = ["1", "2", "3"]
        truncate_to = len(values) - 1
        for value in values:
            concat_resp = await client_async.list_push_back(
                cache_name=cache_name,
                list_name=list_name,
                value=value,
                truncate_front_to_size=truncate_to,
            )
            assert isinstance(concat_resp, CacheListPushBack.Success)

        fetch_resp = await client_async.list_fetch(cache_name, list_name)
        assert fetch_resp.values_string == ["2", "3"]


@behaves_like(a_cache_name_validator)
@behaves_like(a_connection_validator)
@behaves_like(a_list_adder)
@behaves_like(a_list_name_validator)
@behaves_like(a_list_pusher)
def describe_list_push_front() -> None:
    @fixture
    def cache_name_validator(client_async: SimpleCacheClientAsync) -> TCacheNameValidator:
        list_name = uuid_str()
        return partial(client_async.list_push_front, list_name=list_name, value=uuid_str())

    @fixture
    def connection_validator() -> TConnectionValidator:
        async def _connection_validator(
            client_async: SimpleCacheClientAsync, cache_name: TCacheName
        ) -> ErrorResponseMixin:
            list_name = uuid_str()
            return await client_async.list_push_front(cache_name=cache_name, list_name=list_name, value=uuid_str())

        return _connection_validator

    @fixture
    def list_adder(
        client_async: SimpleCacheClientAsync, cache_name: TCacheName, list_name: TListName, list_value: TListValue
    ) -> TListAdder:
        async def _list_adder(
            client_async: SimpleCacheClientAsync,
            list_name: TListName,
            list_value: TListValue,
            ttl: CollectionTtl,
        ) -> CacheResponse:
            return await client_async.list_push_front(cache_name, list_name, list_value, ttl=ttl)

        return _list_adder

    @fixture
    def list_name_validator(
        client_async: SimpleCacheClientAsync, cache_name: TCacheName, list_name: TListName
    ) -> TListNameValidator:
        value = uuid_str()
        return partial(client_async.list_push_front, value=value)

    @fixture
    def list_pusher(
        client_async: SimpleCacheClientAsync, cache_name: TCacheName, list_name: TListName, list_value: TListValue
    ) -> TListPusher:
        return partial(client_async.list_push_front)

    async def it_truncates_the_back(
        client_async: SimpleCacheClientAsync,
        cache_name: TCacheName,
        list_name: TListName,
    ) -> None:
        values = ["1", "2", "3"]
        truncate_to = len(values) - 1
        for value in values:
            concat_resp = await client_async.list_push_front(
                cache_name=cache_name,
                list_name=list_name,
                value=value,
                truncate_back_to_size=truncate_to,
            )
            assert isinstance(concat_resp, CacheListPushFront.Success)

        fetch_resp = await client_async.list_fetch(cache_name, list_name)
        assert fetch_resp.values_string == ["3", "2"]


@behaves_like(a_cache_name_validator)
@behaves_like(a_connection_validator)
@behaves_like(a_list_name_validator)
def describe_list_remove_value() -> None:
    @fixture
    def cache_name_validator(client_async: SimpleCacheClientAsync) -> TCacheNameValidator:
        list_name = uuid_str()
        return partial(client_async.list_remove_value, list_name=list_name, value=uuid_str())

    @fixture
    def connection_validator() -> TConnectionValidator:
        async def _connection_validator(
            client_async: SimpleCacheClientAsync, cache_name: TCacheName
        ) -> ErrorResponseMixin:
            list_name = uuid_str()
            return await client_async.list_remove_value(cache_name=cache_name, list_name=list_name, value=uuid_str())

        return _connection_validator

    @fixture
    def list_name_validator(
        client_async: SimpleCacheClientAsync, cache_name: TCacheName, list_name: TListName
    ) -> TListNameValidator:
        value = uuid_str()
        return partial(client_async.list_remove_value, value=value)

    @pytest.mark.parametrize(
        "values, to_remove, expected_values",
        [
            # strings
            (["up", "up", "down", "down", "left", "right"], "up", ["down", "down", "left", "right"]),
            # bytes
            ([b"number 9", b"that", b"number 9", b"this"], b"number 9", ["that", "this"]),
            # no match
            (["a", "b", "c"], "z", ["a", "b", "c"]),
        ],
    )
    async def it_removes_values(
        values: TListValuesInput,
        to_remove: TListValue,
        expected_values: TListValuesInputStr,
        client_async: SimpleCacheClientAsync,
        cache_name: TCacheName,
        list_name: TListName,
    ) -> None:
        await client_async.list_concatenate_front(cache_name, list_name, values)
        await client_async.list_remove_value(cache_name, list_name, to_remove)

        fetch_resp = await client_async.list_fetch(cache_name, list_name)
        assert fetch_resp.values_string == expected_values

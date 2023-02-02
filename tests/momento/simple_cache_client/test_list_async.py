from functools import partial
from typing import Awaitable, Callable

from pytest import fixture
from pytest_describe import behaves_like

from momento import SimpleCacheClientAsync
from momento.errors import MomentoErrorCode
from momento.responses import (
    CacheListConcatenateBack,
    CacheListConcatenateFront,
    CacheListFetch,
    CacheResponse,
)
from momento.responses.mixins import ErrorResponseMixin
from momento.typing import (
    TCacheName,
    TListName,
    TListValues,
    TListValuesBytes,
    TListValuesStr,
)
from tests.utils import uuid_str

from .shared_behaviors_async import (
    TCacheNameValidator,
    TConnectionValidator,
    a_cache_name_validator,
    a_connection_validator,
)

TListNameValidator = Callable[[TListName], Awaitable[CacheResponse]]
TListConcatenator = Callable[[TListName, TListValues], Awaitable[CacheResponse]]


def a_list_concatenator() -> None:
    async def it_returns_the_new_list_length(
        list_concatenator: TListConcatenator,
        client_async: SimpleCacheClientAsync,
        cache_name: TCacheName,
        list_name: TListName,
        values: TListValues,
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
        values_bytes: TListValuesBytes,
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
        values_str: TListValuesStr,
    ) -> None:
        concat_resp = await list_concatenator(cache_name, list_name, values_str)
        assert concat_resp.list_length == len(values_str)

        fetch_resp = await client_async.list_fetch(cache_name, list_name)
        assert isinstance(fetch_resp, CacheListFetch.Hit)
        assert fetch_resp.values_string == values_str


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
    def list_name_validator(
        client_async: SimpleCacheClientAsync, cache_name: TCacheName, list_name: TListName, values: TListValues
    ) -> TListNameValidator:
        return partial(client_async.list_concatenate_back, values=values)

    @fixture
    def list_concatenator(
        client_async: SimpleCacheClientAsync, list_name: TListName, values: TListValues
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
    def list_name_validator(
        client_async: SimpleCacheClientAsync, cache_name: TCacheName, list_name: TListName, values: TListValues
    ) -> TListNameValidator:
        return partial(client_async.list_concatenate_front, values=values)

    @fixture
    def list_concatenator(
        client_async: SimpleCacheClientAsync, list_name: TListName, values: TListValues
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

from functools import partial
from typing import Callable

from pytest import fixture
from pytest_describe import behaves_like

from momento import SimpleCacheClient
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

from .shared_behaviors import (
    TCacheNameValidator,
    TConnectionValidator,
    a_cache_name_validator,
    a_connection_validator,
)

TListNameValidator = Callable[[TListName], CacheResponse]
TListConcatenator = Callable[[TListName, TListValues], CacheResponse]


def a_list_concatenator() -> None:
    def it_returns_the_new_list_length(
        list_concatenator: TListConcatenator,
        client: SimpleCacheClient,
        cache_name: TCacheName,
        list_name: TListName,
        values: TListValues,
    ) -> None:
        resp = list_concatenator(cache_name, list_name, values)
        length = len(values)
        assert resp.list_length == length

        resp = list_concatenator(cache_name, list_name, values)
        length += len(values)
        assert resp.list_length == length

    def with_bytes_it_succeeds(
        list_concatenator: TListConcatenator,
        client: SimpleCacheClient,
        cache_name: TCacheName,
        list_name: TListName,
        values_bytes: TListValuesBytes,
    ) -> None:
        concat_resp = list_concatenator(cache_name, list_name, values_bytes)
        assert concat_resp.list_length == len(values_bytes)

        fetch_resp = client.list_fetch(cache_name, list_name)
        assert isinstance(fetch_resp, CacheListFetch.Hit)
        assert fetch_resp.values_bytes == values_bytes

    def with_strings_it_succeeds(
        list_concatenator: TListConcatenator,
        client: SimpleCacheClient,
        cache_name: TCacheName,
        list_name: TListName,
        values_str: TListValuesStr,
    ) -> None:
        concat_resp = list_concatenator(cache_name, list_name, values_str)
        assert concat_resp.list_length == len(values_str)

        fetch_resp = client.list_fetch(cache_name, list_name)
        assert isinstance(fetch_resp, CacheListFetch.Hit)
        assert fetch_resp.values_string == values_str


def a_list_name_validator() -> None:
    def with_null_list_name_it_returns_invalid(list_name_validator: TListNameValidator, cache_name: TCacheName) -> None:
        response = list_name_validator(cache_name, None)  # type: ignore
        assert isinstance(response, ErrorResponseMixin)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
        assert response.inner_exception.message == "List name must be a string"

    def with_empty_list_name_it_returns_invalid(
        list_name_validator: TListNameValidator, cache_name: TCacheName
    ) -> None:
        response = list_name_validator(cache_name, "")
        assert isinstance(response, ErrorResponseMixin)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
        assert response.inner_exception.message == "List name must not be empty"

    def with_bad_list_name_it_returns_invalid(list_name_validator: TCacheNameValidator, cache_name: TCacheName) -> None:
        response = list_name_validator(cache_name, 1)  # type: ignore
        assert isinstance(response, ErrorResponseMixin)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
        assert response.inner_exception.message == "List name must be a string"


@behaves_like(a_cache_name_validator)
@behaves_like(a_connection_validator)
@behaves_like(a_list_name_validator)
def describe_list_fetch() -> None:
    @fixture
    def cache_name_validator(client: SimpleCacheClient) -> TCacheNameValidator:
        list_name = uuid_str()
        return partial(client.list_fetch, list_name=list_name)

    @fixture
    def connection_validator() -> TConnectionValidator:
        def _connection_validator(client: SimpleCacheClient, cache_name: TCacheName) -> ErrorResponseMixin:
            list_name = uuid_str()
            return client.list_fetch(cache_name, list_name)

        return _connection_validator

    @fixture
    def list_name_validator(
        client: SimpleCacheClient, cache_name: TCacheName, list_name: TListName
    ) -> TListNameValidator:
        return partial(client.list_fetch)

    def misses_when_the_list_does_not_exist(
        client: SimpleCacheClient, cache_name: TCacheName, list_name: TListName
    ) -> None:
        resp = client.list_fetch(cache_name, list_name)
        assert isinstance(resp, CacheListFetch.Miss)


@behaves_like(a_cache_name_validator)
@behaves_like(a_connection_validator)
@behaves_like(a_list_name_validator)
@behaves_like(a_list_concatenator)
def describe_list_concatenate_back() -> None:
    @fixture
    def cache_name_validator(client: SimpleCacheClient) -> TCacheNameValidator:
        list_name = uuid_str()
        return partial(client.list_concatenate_back, list_name=list_name, values=[uuid_str()])

    @fixture
    def connection_validator() -> TConnectionValidator:
        def _connection_validator(client: SimpleCacheClient, cache_name: TCacheName) -> ErrorResponseMixin:
            list_name = uuid_str()
            return client.list_concatenate_back(cache_name, list_name, [uuid_str()])

        return _connection_validator

    @fixture
    def list_name_validator(
        client: SimpleCacheClient, cache_name: TCacheName, list_name: TListName, values: TListValues
    ) -> TListNameValidator:
        return partial(client.list_concatenate_back, values=values)

    @fixture
    def list_concatenator(client: SimpleCacheClient, list_name: TListName, values: TListValues) -> TListConcatenator:
        return partial(client.list_concatenate_back)

    def it_truncates_the_front(
        client: SimpleCacheClient,
        cache_name: TCacheName,
        list_name: TListName,
    ) -> None:
        truncate_to = 4

        concat_resp = client.list_concatenate_back(
            cache_name=cache_name,
            list_name=list_name,
            values=["one", "two", "three"],
            truncate_front_to_size=truncate_to,
        )
        assert isinstance(concat_resp, CacheListConcatenateBack.Success)
        assert concat_resp.list_length <= truncate_to

        concat_resp = client.list_concatenate_back(
            cache_name=cache_name,
            list_name=list_name,
            values=["four", "five", "six"],
            truncate_front_to_size=truncate_to,
        )
        assert isinstance(concat_resp, CacheListConcatenateBack.Success)
        assert concat_resp.list_length <= truncate_to

        fetch_resp = client.list_fetch(cache_name, list_name)
        assert fetch_resp.values_string == ["three", "four", "five", "six"]


@behaves_like(a_cache_name_validator)
@behaves_like(a_connection_validator)
@behaves_like(a_list_name_validator)
@behaves_like(a_list_concatenator)
def describe_list_concatenate_front() -> None:
    @fixture
    def cache_name_validator(client: SimpleCacheClient) -> TCacheNameValidator:
        list_name = uuid_str()
        return partial(client.list_concatenate_front, list_name=list_name, values=[uuid_str()])

    @fixture
    def connection_validator() -> TConnectionValidator:
        def _connection_validator(client: SimpleCacheClient, cache_name: TCacheName) -> ErrorResponseMixin:
            list_name = uuid_str()
            return client.list_concatenate_front(cache_name, list_name, [uuid_str()])

        return _connection_validator

    @fixture
    def list_name_validator(
        client: SimpleCacheClient, cache_name: TCacheName, list_name: TListName, values: TListValues
    ) -> TListNameValidator:
        return partial(client.list_concatenate_front, values=values)

    @fixture
    def list_concatenator(client: SimpleCacheClient, list_name: TListName, values: TListValues) -> TListConcatenator:
        return partial(client.list_concatenate_front)

    def it_truncates_the_back(
        client: SimpleCacheClient,
        cache_name: TCacheName,
        list_name: TListName,
    ) -> None:
        truncate_to = 4

        concat_resp = client.list_concatenate_front(
            cache_name=cache_name,
            list_name=list_name,
            values=["one", "two", "three"],
            truncate_back_to_size=truncate_to,
        )
        assert isinstance(concat_resp, CacheListConcatenateFront.Success)
        assert concat_resp.list_length <= truncate_to

        concat_resp = client.list_concatenate_front(
            cache_name=cache_name,
            list_name=list_name,
            values=["four", "five", "six"],
            truncate_back_to_size=truncate_to,
        )
        assert isinstance(concat_resp, CacheListConcatenateFront.Success)
        assert concat_resp.list_length <= truncate_to

        fetch_resp = client.list_fetch(cache_name, list_name)
        assert fetch_resp.values_string == ["four", "five", "six", "one"]

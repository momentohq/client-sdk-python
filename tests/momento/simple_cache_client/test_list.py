from functools import partial
from typing import Callable

from pytest import fixture
from pytest_describe import behaves_like

from momento import SimpleCacheClient
from momento.errors import MomentoErrorCode
from momento.responses import CacheListConcatenateBack, CacheListFetch, CacheResponse
from momento.responses.mixins import ErrorResponseMixin
from momento.typing import TCacheName, TListName, TListValuesBytes, TListValuesStr
from tests.utils import uuid_str

from .shared_behaviors import (
    TCacheNameValidator,
    TConnectionValidator,
    a_cache_name_validator,
    a_connection_validator,
)

TListNameValidator = Callable[[TListName], CacheResponse]


def a_list_name_validator() -> None:
    def with_non_existent_list_name_it_throws_not_found(
        list_name_validator: TListNameValidator, list_name: TListName
    ) -> None:
        response = list_name_validator(list_name)
        assert isinstance(response, ErrorResponseMixin)
        assert response.error_code == MomentoErrorCode.NOT_FOUND_ERROR

    def with_null_list_name_it_throws_exception(list_name_validator: TListNameValidator) -> None:
        response = list_name_validator(None)  # type: ignore
        assert isinstance(response, ErrorResponseMixin)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
        assert response.inner_exception.message == "List name must be a non-empty string"

    def with_empty_list_name_it_throws_exception(list_name_validator: TListNameValidator) -> None:
        response = list_name_validator("")
        assert isinstance(response, ErrorResponseMixin)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
        assert response.inner_exception.message == "List name is empty"

    def with_bad_list_name_throws_exception(list_name_validator: TCacheNameValidator) -> None:
        response = list_name_validator(1)  # type: ignore
        assert isinstance(response, ErrorResponseMixin)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
        assert response.inner_exception.message == "List name must be a non-empty string"


@behaves_like(a_cache_name_validator)
@behaves_like(a_connection_validator)
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

    def misses_when_the_list_does_not_exist(
        client: SimpleCacheClient, cache_name: TCacheName, list_name: TListName
    ) -> None:
        resp = client.list_fetch(cache_name, list_name)
        assert isinstance(resp, CacheListFetch.Miss)


@behaves_like(a_cache_name_validator)
@behaves_like(a_connection_validator)
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

    def it_returns_the_new_list_length(
        client: SimpleCacheClient,
        cache_name: TCacheName,
        list_name: TListName,
        values_bytes: TListValuesBytes,
    ) -> None:
        resp = client.list_concatenate_back(cache_name, list_name, values_bytes)
        length = len(values_bytes)
        assert resp.list_length == length

        resp = client.list_concatenate_back(cache_name, list_name, values_bytes)
        length += len(values_bytes)
        assert resp.list_length == length

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

    def with_bytes_it_succeeds(
        client: SimpleCacheClient,
        cache_name: TCacheName,
        list_name: TListName,
        values_bytes: TListValuesBytes,
    ) -> None:
        concat_resp = client.list_concatenate_back(cache_name, list_name, values_bytes)
        assert isinstance(concat_resp, CacheListConcatenateBack.Success)

        fetch_resp = client.list_fetch(cache_name, list_name)
        assert isinstance(fetch_resp, CacheListFetch.Hit)
        assert fetch_resp.values_bytes == values_bytes

    def with_strings_it_succeeds(
        client: SimpleCacheClient, cache_name: TCacheName, list_name: TListName, values_str: TListValuesStr
    ) -> None:
        resp = client.list_concatenate_back(cache_name, list_name, values_str)
        assert isinstance(resp, CacheListConcatenateBack.Success)

        fetch_resp = client.list_fetch(cache_name, list_name)
        assert isinstance(fetch_resp, CacheListFetch.Hit)
        assert fetch_resp.values_string == values_str

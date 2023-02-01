from functools import partial
from typing import Callable

from pytest import fixture
from pytest_describe import behaves_like

from momento import SimpleCacheClient
from momento.errors import MomentoErrorCode
from momento.responses import CacheListConcatenateBack, CacheListFetch, CacheResponse
from momento.responses.mixins import ErrorResponseMixin
from momento.typing import TCacheName, TListName, TListValuesBytes, TListValuesStr
from tests.utils import uuid_bytes, uuid_str

from .shared_behaviors import (
    TCacheNameValidator,
    TConnectionValidator,
    a_cache_name_validator,
    a_connection_validator,
)

TListNameValidator = Callable[[str], CacheResponse]


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

    def with_bytes_it_succeeds(
        client: SimpleCacheClient,
        cache_name: TCacheName,
        list_name: TListName,
        values_bytes: TListValuesBytes,
    ) -> None:
        resp = client.list_concatenate_back(cache_name, list_name, values_bytes)
        assert isinstance(resp, CacheListConcatenateBack.Success)

    def with_strings_it_succeeds(
        client: SimpleCacheClient, cache_name: TCacheName, list_name: TListName, values_str: TListValuesStr
    ) -> None:
        resp = client.list_concatenate_back(cache_name, list_name, values_str)
        assert isinstance(resp, CacheListConcatenateBack.Success)

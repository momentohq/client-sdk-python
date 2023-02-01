from functools import partial
from typing import Callable

from pytest import fixture
from pytest_describe import behaves_like

from momento import SimpleCacheClient
from momento.errors import MomentoErrorCode
from momento.responses import CacheListFetch, CacheResponse
from momento.responses.mixins import ErrorResponseMixin
from tests.utils import uuid_str

from .shared_behaviors import (
    TCacheNameValidator,
    TConnectionValidator,
    a_cache_name_validator,
    a_connection_validator,
)

TListNameValidator = Callable[[str], CacheResponse]


def a_list_name_validator() -> None:
    def with_non_existent_list_name_it_throws_not_found(list_name_validator: TListNameValidator) -> None:
        list_name = uuid_str()
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
        def _connection_validator(client: SimpleCacheClient, cache_name: str) -> ErrorResponseMixin:
            list_name = uuid_str()
            return client.list_fetch(cache_name, list_name)

        return _connection_validator

    def misses_when_the_list_does_not_exist(client: SimpleCacheClient, cache_name: str) -> None:
        list_name = uuid_str()

        resp = client.list_fetch(cache_name, list_name)
        assert isinstance(resp, CacheListFetch.Miss)

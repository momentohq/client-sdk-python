from functools import partial
from typing import Awaitable, Callable

from pytest import fixture
from pytest_describe import behaves_like

from momento import SimpleCacheClientAsync
from momento.errors import MomentoErrorCode
from momento.responses import CacheListFetch, CacheResponse
from momento.responses.mixins import ErrorResponseMixin
from tests.utils import uuid_str

from .shared_behaviors_async import (
    TCacheNameValidator,
    TConnectionValidator,
    a_cache_name_validator,
    a_connection_validator,
)

TListNameValidator = Callable[[str], Awaitable[CacheResponse]]


def a_list_name_validator() -> None:
    async def with_non_existent_list_name_it_throws_not_found(list_name_validator: TListNameValidator) -> None:
        list_name = uuid_str()
        response = await list_name_validator(list_name)
        assert isinstance(response, ErrorResponseMixin)
        assert response.error_code == MomentoErrorCode.NOT_FOUND_ERROR

    async def with_null_list_name_it_throws_exception(list_name_validator: TListNameValidator) -> None:
        response = await list_name_validator(None)  # type: ignore
        assert isinstance(response, ErrorResponseMixin)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
        assert response.inner_exception.message == "List name must be a non-empty string"

    async def with_empty_list_name_it_throws_exception(list_name_validator: TListNameValidator) -> None:
        response = await list_name_validator("")
        assert isinstance(response, ErrorResponseMixin)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
        assert response.inner_exception.message == "List name is empty"

    async def with_bad_list_name_throws_exception(list_name_validator: TCacheNameValidator) -> None:
        response = await list_name_validator(1)  # type: ignore
        assert isinstance(response, ErrorResponseMixin)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
        assert response.inner_exception.message == "List name must be a non-empty string"


@behaves_like(a_cache_name_validator)
@behaves_like(a_connection_validator)
def describe_list_fetch() -> None:
    @fixture
    def cache_name_validator(client_async: SimpleCacheClientAsync) -> TCacheNameValidator:
        list_name = uuid_str()
        return partial(client_async.list_fetch, list_name=list_name)

    @fixture
    def connection_validator() -> TConnectionValidator:
        async def _connection_validator(client_async: SimpleCacheClientAsync, cache_name: str) -> ErrorResponseMixin:
            list_name = uuid_str()
            return await client_async.list_fetch(cache_name, list_name)

        return _connection_validator

    async def misses_when_the_list_does_not_exist(client_async: SimpleCacheClientAsync, cache_name: str) -> None:
        list_name = uuid_str()

        resp = await client_async.list_fetch(cache_name, list_name)
        assert isinstance(resp, CacheListFetch.Miss)

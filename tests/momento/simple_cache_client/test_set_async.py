from datetime import timedelta
from functools import partial
from time import sleep
from typing import Awaitable, Callable

import pytest
from pytest import fixture
from pytest_describe import behaves_like

from momento import SimpleCacheClientAsync
from momento.errors import MomentoErrorCode
from momento.requests import CollectionTtl
from momento.responses import (
    CacheResponse,
    CacheSetAddElement,
    CacheSetAddElements,
    CacheSetFetch,
    CacheSetRemoveElement,
    CacheSetRemoveElements,
)
from momento.responses.mixins import ErrorResponseMixin
from momento.typing import TCacheName, TSetElement, TSetElementsInput, TSetName
from tests.utils import uuid_bytes, uuid_str

from .shared_behaviors_async import (
    TCacheNameValidator,
    TConnectionValidator,
    a_cache_name_validator,
    a_connection_validator,
)

TSetNameValidator = Callable[[TCacheName, TSetName], Awaitable[CacheResponse]]


def a_set_name_validator() -> None:
    async def with_null_set_name_it_returns_invalid(
        set_name_validator: TSetNameValidator, cache_name: TCacheName
    ) -> None:
        response = await set_name_validator(cache_name, None)  # type: ignore
        assert isinstance(response, ErrorResponseMixin)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
        assert response.inner_exception.message == "Set name must be a string"

    async def with_empty_set_name_it_returns_invalid(
        set_name_validator: TSetNameValidator, cache_name: TCacheName
    ) -> None:
        response = await set_name_validator(cache_name, "")
        assert isinstance(response, ErrorResponseMixin)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
        assert response.inner_exception.message == "Set name must not be empty"

    async def with_bad_set_name_it_returns_invalid(
        set_name_validator: TCacheNameValidator, cache_name: TCacheName
    ) -> None:
        response = await set_name_validator(cache_name, 1)  # type: ignore
        assert isinstance(response, ErrorResponseMixin)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
        assert response.inner_exception.message == "Set name must be a string"


@behaves_like(a_cache_name_validator)
@behaves_like(a_connection_validator)
@behaves_like(a_set_name_validator)
def describe_set_add_element() -> None:
    @fixture
    def cache_name_validator(
        client_async: SimpleCacheClientAsync, set_name: TSetName, element: TSetElement
    ) -> TCacheNameValidator:
        return partial(client_async.set_add_element, set_name=set_name, element=element)

    @fixture
    def connection_validator() -> TConnectionValidator:
        async def _connection_validator(
            client_async: SimpleCacheClientAsync, cache_name: TCacheName
        ) -> ErrorResponseMixin:
            set_name = uuid_str()
            element = uuid_str()
            return await client_async.set_add_element(cache_name=cache_name, set_name=set_name, element=element)

        return _connection_validator

    @fixture
    def set_name_validator(client_async: SimpleCacheClientAsync, element: TSetElement) -> TSetNameValidator:
        return partial(client_async.set_add_element, element=element)


@behaves_like(a_cache_name_validator)
@behaves_like(a_connection_validator)
@behaves_like(a_set_name_validator)
def describe_set_add_elements() -> None:
    @fixture
    def cache_name_validator(
        client_async: SimpleCacheClientAsync, set_name: TSetName, elements: TSetElementsInput
    ) -> TCacheNameValidator:
        return partial(client_async.set_add_elements, set_name=set_name, elements=elements)

    @fixture
    def connection_validator() -> TConnectionValidator:
        async def _connection_validator(
            client_async: SimpleCacheClientAsync, cache_name: TCacheName
        ) -> ErrorResponseMixin:
            set_name = uuid_str()
            elements = {uuid_str()}
            return await client_async.set_add_elements(cache_name=cache_name, set_name=set_name, elements=elements)

        return _connection_validator

    @fixture
    def set_name_validator(client_async: SimpleCacheClientAsync, elements: TSetElementsInput) -> TSetNameValidator:
        return partial(client_async.set_add_elements, elements=elements)


@behaves_like(a_cache_name_validator)
@behaves_like(a_connection_validator)
@behaves_like(a_set_name_validator)
def describe_set_fetch() -> None:
    @fixture
    def cache_name_validator(client_async: SimpleCacheClientAsync, set_name: TSetName) -> TCacheNameValidator:
        return partial(client_async.set_fetch, set_name=set_name)

    @fixture
    def connection_validator() -> TConnectionValidator:
        async def _connection_validator(
            client_async: SimpleCacheClientAsync, cache_name: TCacheName
        ) -> ErrorResponseMixin:
            set_name = uuid_str()
            return await client_async.set_fetch(cache_name=cache_name, set_name=set_name)

        return _connection_validator

    @fixture
    def set_name_validator(client_async: SimpleCacheClientAsync) -> TSetNameValidator:
        return partial(client_async.set_fetch)

    async def when_the_set_exists_it_fetches(
        client_async: SimpleCacheClientAsync, cache_name: TCacheName, set_name: TSetName
    ) -> None:
        elements = {"one", "two"}
        await client_async.set_add_elements(cache_name, set_name, elements)

        resp = await client_async.set_fetch(cache_name, set_name)
        assert isinstance(resp, CacheSetFetch.Hit)
        assert resp.value_set_string == elements
        assert resp.value_set_bytes == {b"one", b"two"}

    async def when_the_set_does_not_exist_it_misses(
        client_async: SimpleCacheClientAsync, cache_name: TCacheName, set_name: TSetName
    ) -> None:
        resp = await client_async.set_fetch(cache_name, set_name)
        assert isinstance(resp, CacheSetFetch.Miss)


@behaves_like(a_cache_name_validator)
@behaves_like(a_connection_validator)
@behaves_like(a_set_name_validator)
def describe_set_remove_element() -> None:
    @fixture
    def cache_name_validator(
        client_async: SimpleCacheClientAsync, set_name: TSetName, element: TSetElement
    ) -> TCacheNameValidator:
        return partial(client_async.set_remove_element, set_name=set_name, element=element)

    @fixture
    def connection_validator() -> TConnectionValidator:
        async def _connection_validator(
            client_async: SimpleCacheClientAsync, cache_name: TCacheName
        ) -> ErrorResponseMixin:
            set_name = uuid_str()
            element = uuid_str()
            return await client_async.set_remove_element(cache_name=cache_name, set_name=set_name, element=element)

        return _connection_validator

    @fixture
    def set_name_validator(client_async: SimpleCacheClientAsync, element: TSetElement) -> TSetNameValidator:
        return partial(client_async.set_remove_element, element=element)


@behaves_like(a_cache_name_validator)
@behaves_like(a_connection_validator)
@behaves_like(a_set_name_validator)
def describe_set_remove_elements() -> None:
    @fixture
    def cache_name_validator(
        client_async: SimpleCacheClientAsync, set_name: TSetName, elements: TSetElementsInput
    ) -> TCacheNameValidator:
        return partial(client_async.set_remove_elements, set_name=set_name, elements=elements)

    @fixture
    def connection_validator() -> TConnectionValidator:
        async def _connection_validator(
            client_async: SimpleCacheClientAsync, cache_name: TCacheName
        ) -> ErrorResponseMixin:
            set_name = uuid_str()
            elements = {uuid_str()}
            return await client_async.set_remove_elements(cache_name=cache_name, set_name=set_name, elements=elements)

        return _connection_validator

    @fixture
    def set_name_validator(client_async: SimpleCacheClientAsync, elements: TSetElementsInput) -> TSetNameValidator:
        return partial(client_async.set_remove_elements, elements=elements)

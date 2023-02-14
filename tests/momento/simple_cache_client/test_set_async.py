from __future__ import annotations

from datetime import timedelta
from functools import partial
from time import sleep
from typing import Awaitable

from pytest import fixture
from pytest_describe import behaves_like
from typing_extensions import Protocol

from momento import SimpleCacheClientAsync
from momento.auth import CredentialProvider
from momento.config import Configuration
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


class TSetAdder(Protocol):
    def __call__(
        self,
        client_async: SimpleCacheClientAsync,
        cache_name: TCacheName,
        set_name: TSetName,
        element: TSetElement,
        *,
        ttl: CollectionTtl,
    ) -> Awaitable[CacheResponse]:
        ...


def a_set_adder() -> None:
    async def it_sets_the_ttl(
        configuration: Configuration,
        credential_provider: CredentialProvider,
        set_adder: TSetAdder,
        cache_name: TCacheName,
        set_name: TSetName,
        elements: TSetElementsInput,
    ) -> None:
        async with SimpleCacheClientAsync(configuration, credential_provider, timedelta(hours=1)) as client:
            ttl_seconds = 0.5
            ttl = CollectionTtl(ttl=timedelta(seconds=ttl_seconds), refresh_ttl=False)

            for element in elements:
                await set_adder(client, cache_name, set_name, element, ttl=ttl)

            sleep(ttl_seconds * 2)

            fetch_resp = await client.set_fetch(cache_name, set_name)
            assert isinstance(fetch_resp, CacheSetFetch.Miss)

    async def it_refreshes_the_ttl(
        client_async: SimpleCacheClientAsync, set_adder: TSetAdder, cache_name: TCacheName, set_name: TSetName
    ) -> None:
        ttl_seconds = 1
        ttl = CollectionTtl.of(timedelta(seconds=ttl_seconds))
        elements = {"one", "two", "three", "four"}

        for element in elements:
            await set_adder(client_async, cache_name, set_name, element, ttl=ttl)
            sleep(ttl_seconds / 2)

        fetch_resp = await client_async.set_fetch(cache_name, set_name)
        assert isinstance(fetch_resp, CacheSetFetch.Hit)
        assert fetch_resp.value_set_string == elements

    async def it_uses_the_default_ttl_when_the_collection_ttl_has_no_ttl(
        configuration: Configuration,
        credential_provider: CredentialProvider,
        set_adder: TSetAdder,
        cache_name: TCacheName,
        set_name: TSetName,
    ) -> None:
        ttl_seconds = 1
        async with SimpleCacheClientAsync(configuration, credential_provider, timedelta(seconds=ttl_seconds)) as client:
            ttl = CollectionTtl.from_cache_ttl().with_no_refresh_ttl_on_updates()

            element = uuid_str()
            await set_adder(client, cache_name, set_name, element, ttl=ttl)

            sleep(ttl_seconds / 2)

            fetch_resp = await client.set_fetch(cache_name, set_name)
            assert isinstance(fetch_resp, CacheSetFetch.Hit)
            assert fetch_resp.value_set_string == {element}

            sleep(ttl_seconds / 2)
            fetch_resp = await client.set_fetch(cache_name, set_name)
            assert isinstance(fetch_resp, CacheSetFetch.Miss)


class TSetWhichTakesAnElement(Protocol):
    def __call__(self, cache_name: TCacheName, set_name: TSetName, element: TSetElement) -> Awaitable[CacheResponse]:
        ...


def a_set_which_takes_an_element() -> None:
    async def it_errors_with_the_wrong_type(
        set_which_takes_an_element: TSetWhichTakesAnElement,
        cache_name: TCacheName,
        set_name: TSetName,
    ) -> None:
        resp = await set_which_takes_an_element(cache_name, set_name, 1)  # type:ignore[arg-type]
        if isinstance(resp, ErrorResponseMixin):
            assert resp.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
            # This error is wrong. See https://github.com/momentohq/client-sdk-python/issues/242
            # Should be "Could not convert an int to bytes"
            assert resp.inner_exception.message == "Could not decode bytes to UTF-8<class 'int'>"
        else:
            assert False

    async def it_errors_with_none(
        set_which_takes_an_element: TSetWhichTakesAnElement,
        cache_name: TCacheName,
        set_name: TSetName,
    ) -> None:
        resp = await set_which_takes_an_element(cache_name, set_name, None)  # type:ignore[arg-type]
        if isinstance(resp, ErrorResponseMixin):
            assert resp.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
            # This error is wrong. See https://github.com/momentohq/client-sdk-python/issues/242
            # Should be "Could not convert an int to bytes"
            assert resp.inner_exception.message == "Could not decode bytes to UTF-8<class 'NoneType'>"
        else:
            assert False


class TSetNameValidator(Protocol):
    def __call__(self, set_name: TSetName) -> Awaitable[CacheResponse]:
        ...


def a_set_name_validator() -> None:
    async def with_null_set_name_it_returns_invalid(set_name_validator: TSetNameValidator) -> None:
        response = await set_name_validator(set_name=None)  # type: ignore
        if isinstance(response, ErrorResponseMixin):
            assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
            assert response.inner_exception.message == "Set name must be a string"
        else:
            assert False

    async def with_empty_set_name_it_returns_invalid(set_name_validator: TSetNameValidator) -> None:
        response = await set_name_validator(set_name="")
        assert isinstance(response, ErrorResponseMixin)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
        assert response.inner_exception.message == "Set name must not be empty"

    async def with_bad_set_name_it_returns_invalid(set_name_validator: TCacheNameValidator) -> None:
        response = await set_name_validator(set_name=1)  # type: ignore
        assert isinstance(response, ErrorResponseMixin)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
        assert response.inner_exception.message == "Set name must be a string"


@behaves_like(a_cache_name_validator)
@behaves_like(a_connection_validator)
@behaves_like(a_set_adder)
@behaves_like(a_set_name_validator)
@behaves_like(a_set_which_takes_an_element)
def describe_set_add_element() -> None:
    @fixture
    def cache_name_validator(
        client_async: SimpleCacheClientAsync, set_name: TSetName, element: TSetElement
    ) -> TCacheNameValidator:
        return partial(client_async.set_add_element, set_name=set_name, element=element)

    @fixture
    def connection_validator(cache_name: TCacheName) -> TConnectionValidator:
        async def _connection_validator(client_async: SimpleCacheClientAsync) -> CacheResponse:
            set_name = uuid_str()
            element = uuid_str()
            return await client_async.set_add_element(cache_name=cache_name, set_name=set_name, element=element)

        return _connection_validator

    @fixture
    def set_adder(client_async: SimpleCacheClientAsync) -> TSetAdder:
        async def _set_adder(
            client_async: SimpleCacheClientAsync,
            cache_name: TCacheName,
            set_name: TSetName,
            element: TSetElement,
            *,
            ttl: CollectionTtl,
        ) -> CacheResponse:
            return await client_async.set_add_element(cache_name, set_name, element, ttl=ttl)

        return _set_adder

    @fixture
    def set_name_validator(
        client_async: SimpleCacheClientAsync, cache_name: TCacheName, element: TSetElement
    ) -> TSetNameValidator:
        return partial(client_async.set_add_element, cache_name=cache_name, element=element)

    @fixture
    def set_which_takes_an_element(client_async: SimpleCacheClientAsync) -> TSetWhichTakesAnElement:
        async def _set_which_takes_an_element(
            cache_name: TCacheName, set_name: TSetName, element: TSetElement
        ) -> CacheResponse:
            return await client_async.set_add_element(cache_name, set_name, element)

        return _set_which_takes_an_element

    async def it_adds_a_string_element(
        client_async: SimpleCacheClientAsync, cache_name: TCacheName, set_name: TSetName
    ) -> None:
        element1 = uuid_str()
        element2 = uuid_str()

        add_resp = await client_async.set_add_element(cache_name, set_name, element1)
        assert isinstance(add_resp, CacheSetAddElement.Success)

        fetch_resp = await client_async.set_fetch(cache_name, set_name)
        assert isinstance(fetch_resp, CacheSetFetch.Hit)
        assert fetch_resp.value_set_string == {element1}

        add_resp = await client_async.set_add_element(cache_name, set_name, element1)
        assert isinstance(add_resp, CacheSetAddElement.Success)
        add_resp = await client_async.set_add_element(cache_name, set_name, element2)
        assert isinstance(add_resp, CacheSetAddElement.Success)

        fetch_resp = await client_async.set_fetch(cache_name, set_name)
        assert isinstance(fetch_resp, CacheSetFetch.Hit)
        assert fetch_resp.value_set_string == {element1, element2}

    async def it_adds_a_byte_element(
        client_async: SimpleCacheClientAsync, cache_name: TCacheName, set_name: TSetName
    ) -> None:
        element1 = uuid_bytes()
        element2 = uuid_bytes()

        add_resp = await client_async.set_add_element(cache_name, set_name, element1)
        assert isinstance(add_resp, CacheSetAddElement.Success)

        fetch_resp = await client_async.set_fetch(cache_name, set_name)
        assert isinstance(fetch_resp, CacheSetFetch.Hit)
        assert fetch_resp.value_set_bytes == {element1}

        add_resp = await client_async.set_add_element(cache_name, set_name, element1)
        assert isinstance(add_resp, CacheSetAddElement.Success)
        add_resp = await client_async.set_add_element(cache_name, set_name, element2)
        assert isinstance(add_resp, CacheSetAddElement.Success)

        fetch_resp = await client_async.set_fetch(cache_name, set_name)
        assert isinstance(fetch_resp, CacheSetFetch.Hit)
        assert fetch_resp.value_set_bytes == {element1, element2}


@behaves_like(a_cache_name_validator)
@behaves_like(a_connection_validator)
@behaves_like(a_set_adder)
@behaves_like(a_set_name_validator)
@behaves_like(a_set_which_takes_an_element)
def describe_set_add_elements() -> None:
    @fixture
    def cache_name_validator(
        client_async: SimpleCacheClientAsync, set_name: TSetName, elements: TSetElementsInput
    ) -> TCacheNameValidator:
        return partial(client_async.set_add_elements, set_name=set_name, elements=elements)

    @fixture
    def connection_validator(cache_name: TCacheName) -> TConnectionValidator:
        async def _connection_validator(client_async: SimpleCacheClientAsync) -> CacheResponse:
            set_name = uuid_str()
            elements = {uuid_str()}
            return await client_async.set_add_elements(cache_name=cache_name, set_name=set_name, elements=elements)

        return _connection_validator

    @fixture
    def set_adder(client_async: SimpleCacheClientAsync) -> TSetAdder:
        async def _set_adder(
            client_async: SimpleCacheClientAsync,
            cache_name: TCacheName,
            set_name: TSetName,
            element: TSetElement,
            *,
            ttl: CollectionTtl,
        ) -> CacheResponse:
            return await client_async.set_add_elements(cache_name, set_name, {element}, ttl=ttl)

        return _set_adder

    @fixture
    def set_name_validator(
        client_async: SimpleCacheClientAsync, cache_name: TCacheName, elements: TSetElementsInput
    ) -> TSetNameValidator:
        return partial(client_async.set_add_elements, cache_name=cache_name, elements=elements)

    @fixture
    def set_which_takes_an_element(client_async: SimpleCacheClientAsync) -> TSetWhichTakesAnElement:
        async def _set_which_takes_an_element(
            cache_name: TCacheName, set_name: TSetName, element: TSetElement
        ) -> CacheResponse:
            return await client_async.set_add_elements(cache_name, set_name, {element})

        return _set_which_takes_an_element

    async def it_adds_string_elements(
        client_async: SimpleCacheClientAsync,
        cache_name: TCacheName,
        set_name: TSetName,
        elements_str: set[str],
    ) -> None:
        add_resp = await client_async.set_add_elements(cache_name, set_name, elements_str)
        assert isinstance(add_resp, CacheSetAddElements.Success)

        fetch_resp = await client_async.set_fetch(cache_name, set_name)
        assert isinstance(fetch_resp, CacheSetFetch.Hit)
        assert fetch_resp.value_set_string == elements_str

        elements_str.add("another")
        add_resp = await client_async.set_add_elements(cache_name, set_name, elements_str)
        assert isinstance(add_resp, CacheSetAddElements.Success)

        fetch_resp = await client_async.set_fetch(cache_name, set_name)
        assert isinstance(fetch_resp, CacheSetFetch.Hit)
        assert fetch_resp.value_set_string == elements_str

    async def it_adds_byte_elements(
        client_async: SimpleCacheClientAsync,
        cache_name: TCacheName,
        set_name: TSetName,
        elements_bytes: set[bytes],
    ) -> None:
        add_resp = await client_async.set_add_elements(cache_name, set_name, elements_bytes)
        assert isinstance(add_resp, CacheSetAddElements.Success)

        fetch_resp = await client_async.set_fetch(cache_name, set_name)
        assert isinstance(fetch_resp, CacheSetFetch.Hit)
        assert fetch_resp.value_set_bytes == elements_bytes

        elements_bytes.add(b"another")

        add_resp = await client_async.set_add_elements(cache_name, set_name, elements_bytes)
        assert isinstance(add_resp, CacheSetAddElements.Success)

        fetch_resp = await client_async.set_fetch(cache_name, set_name)
        assert isinstance(fetch_resp, CacheSetFetch.Hit)
        assert fetch_resp.value_set_bytes == elements_bytes


@behaves_like(a_cache_name_validator)
@behaves_like(a_connection_validator)
@behaves_like(a_set_name_validator)
def describe_set_fetch() -> None:
    @fixture
    def cache_name_validator(client_async: SimpleCacheClientAsync, set_name: TSetName) -> TCacheNameValidator:
        return partial(client_async.set_fetch, set_name=set_name)

    @fixture
    def connection_validator(cache_name: TCacheName) -> TConnectionValidator:
        async def _connection_validator(client_async: SimpleCacheClientAsync) -> CacheResponse:
            set_name = uuid_str()
            return await client_async.set_fetch(cache_name=cache_name, set_name=set_name)

        return _connection_validator

    @fixture
    def set_name_validator(client_async: SimpleCacheClientAsync, cache_name: TCacheName) -> TSetNameValidator:
        return partial(client_async.set_fetch, cache_name=cache_name)

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
@behaves_like(a_set_which_takes_an_element)
def describe_set_remove_element() -> None:
    @fixture
    def cache_name_validator(
        client_async: SimpleCacheClientAsync, set_name: TSetName, element: TSetElement
    ) -> TCacheNameValidator:
        return partial(client_async.set_remove_element, set_name=set_name, element=element)

    @fixture
    def connection_validator(cache_name: TCacheName) -> TConnectionValidator:
        async def _connection_validator(client_async: SimpleCacheClientAsync) -> CacheResponse:
            set_name = uuid_str()
            element = uuid_str()
            return await client_async.set_remove_element(cache_name=cache_name, set_name=set_name, element=element)

        return _connection_validator

    @fixture
    def set_name_validator(
        client_async: SimpleCacheClientAsync, cache_name: TCacheName, element: TSetElement
    ) -> TSetNameValidator:
        return partial(client_async.set_remove_element, cache_name=cache_name, element=element)

    @fixture
    def set_which_takes_an_element(client_async: SimpleCacheClientAsync) -> TSetWhichTakesAnElement:
        async def _set_which_takes_an_element(
            cache_name: TCacheName, set_name: TSetName, element: TSetElement
        ) -> CacheResponse:
            return await client_async.set_remove_element(cache_name, set_name, element)

        return _set_which_takes_an_element

    async def it_removes_a_string_element(
        client_async: SimpleCacheClientAsync,
        cache_name: TCacheName,
        set_name: TSetName,
    ) -> None:
        element = uuid_str()

        remove_resp = await client_async.set_remove_element(cache_name, set_name, element)
        assert isinstance(remove_resp, CacheSetRemoveElement.Success)

        new_elements = {uuid_str(), uuid_str()}
        await client_async.set_add_element(cache_name, set_name, element)
        await client_async.set_add_elements(cache_name, set_name, new_elements)

        fetch_resp = await client_async.set_fetch(cache_name, set_name)
        assert isinstance(fetch_resp, CacheSetFetch.Hit)
        assert fetch_resp.value_set_string == {element}.union(new_elements)

        remove_resp = await client_async.set_remove_element(cache_name, set_name, element)
        assert isinstance(remove_resp, CacheSetRemoveElement.Success)

        fetch_resp = await client_async.set_fetch(cache_name, set_name)
        assert isinstance(fetch_resp, CacheSetFetch.Hit)
        assert fetch_resp.value_set_string == new_elements

    async def it_removes_a_byte_element(
        client_async: SimpleCacheClientAsync,
        cache_name: TCacheName,
        set_name: TSetName,
    ) -> None:
        element = uuid_bytes()

        remove_resp = await client_async.set_remove_element(cache_name, set_name, element)
        assert isinstance(remove_resp, CacheSetRemoveElement.Success)

        new_elements = {uuid_bytes(), uuid_bytes()}
        await client_async.set_add_element(cache_name, set_name, element)
        await client_async.set_add_elements(cache_name, set_name, new_elements)

        fetch_resp = await client_async.set_fetch(cache_name, set_name)
        assert isinstance(fetch_resp, CacheSetFetch.Hit)
        assert fetch_resp.value_set_bytes == {element}.union(new_elements)

        remove_resp = await client_async.set_remove_element(cache_name, set_name, element)
        assert isinstance(remove_resp, CacheSetRemoveElement.Success)

        fetch_resp = await client_async.set_fetch(cache_name, set_name)
        assert isinstance(fetch_resp, CacheSetFetch.Hit)
        assert fetch_resp.value_set_bytes == new_elements


@behaves_like(a_cache_name_validator)
@behaves_like(a_connection_validator)
@behaves_like(a_set_name_validator)
@behaves_like(a_set_which_takes_an_element)
def describe_set_remove_elements() -> None:
    @fixture
    def cache_name_validator(
        client_async: SimpleCacheClientAsync, set_name: TSetName, elements: TSetElementsInput
    ) -> TCacheNameValidator:
        return partial(client_async.set_remove_elements, set_name=set_name, elements=elements)

    @fixture
    def connection_validator(cache_name: TCacheName) -> TConnectionValidator:
        async def _connection_validator(client_async: SimpleCacheClientAsync) -> CacheResponse:
            set_name = uuid_str()
            elements = {uuid_str()}
            return await client_async.set_remove_elements(cache_name=cache_name, set_name=set_name, elements=elements)

        return _connection_validator

    @fixture
    def set_name_validator(
        client_async: SimpleCacheClientAsync, cache_name: TCacheName, elements: TSetElementsInput
    ) -> TSetNameValidator:
        return partial(client_async.set_remove_elements, cache_name=cache_name, elements=elements)

    @fixture
    def set_which_takes_an_element(client_async: SimpleCacheClientAsync) -> TSetWhichTakesAnElement:
        async def _set_which_takes_an_element(
            cache_name: TCacheName, set_name: TSetName, element: TSetElement
        ) -> CacheResponse:
            return await client_async.set_add_elements(cache_name, set_name, {element})

        return _set_which_takes_an_element

    async def it_removes_string_elements(
        client_async: SimpleCacheClientAsync,
        cache_name: TCacheName,
        set_name: TSetName,
        elements_str: set[str],
    ) -> None:
        remove_resp = await client_async.set_remove_elements(cache_name, set_name, elements_str)
        assert isinstance(remove_resp, CacheSetRemoveElements.Success)

        new_elements = {uuid_str(), uuid_str()}
        await client_async.set_add_elements(cache_name, set_name, elements_str.union(new_elements))

        fetch_resp = await client_async.set_fetch(cache_name, set_name)
        assert isinstance(fetch_resp, CacheSetFetch.Hit)
        assert fetch_resp.value_set_string == elements_str.union(new_elements)

        remove_resp = await client_async.set_remove_elements(cache_name, set_name, elements_str)
        assert isinstance(remove_resp, CacheSetRemoveElements.Success)

        fetch_resp = await client_async.set_fetch(cache_name, set_name)
        assert isinstance(fetch_resp, CacheSetFetch.Hit)
        assert fetch_resp.value_set_string == new_elements

    async def it_removes_bytes_elements(
        client_async: SimpleCacheClientAsync,
        cache_name: TCacheName,
        set_name: TSetName,
        elements_bytes: set[bytes],
    ) -> None:
        remove_resp = await client_async.set_remove_elements(cache_name, set_name, elements_bytes)
        assert isinstance(remove_resp, CacheSetRemoveElements.Success)

        new_elements = {uuid_bytes(), uuid_bytes()}
        await client_async.set_add_elements(cache_name, set_name, elements_bytes.union(new_elements))

        fetch_resp = await client_async.set_fetch(cache_name, set_name)
        assert isinstance(fetch_resp, CacheSetFetch.Hit)
        assert fetch_resp.value_set_bytes == elements_bytes.union(new_elements)

        remove_resp = await client_async.set_remove_elements(cache_name, set_name, elements_bytes)
        assert isinstance(remove_resp, CacheSetRemoveElements.Success)

        fetch_resp = await client_async.set_fetch(cache_name, set_name)
        assert isinstance(fetch_resp, CacheSetFetch.Hit)
        assert fetch_resp.value_set_bytes == new_elements

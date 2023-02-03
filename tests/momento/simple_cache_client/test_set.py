from datetime import timedelta
from functools import partial
from time import sleep
from typing import Callable

import pytest
from pytest import fixture
from pytest_describe import behaves_like

from momento import SimpleCacheClient
from momento.auth import EnvMomentoTokenProvider
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
from momento.typing import (
    TCacheName,
    TSetElement,
    TSetElementsInput,
    TSetElementsInputBytes,
    TSetElementsInputStr,
    TSetName,
)
from tests.utils import uuid_bytes, uuid_str

from .shared_behaviors import (
    TCacheNameValidator,
    TConnectionValidator,
    a_cache_name_validator,
    a_connection_validator,
)

TSetAdder = Callable[[SimpleCacheClient, TCacheName, TSetName, TSetElement, CollectionTtl], CacheResponse]


def a_set_adder() -> None:
    def it_sets_the_ttl(
        configuration: Configuration,
        credential_provider: EnvMomentoTokenProvider,
        set_adder: TSetAdder,
        cache_name: TCacheName,
        set_name: TSetName,
        elements: TSetElementsInput,
    ) -> None:
        with SimpleCacheClient(configuration, credential_provider, timedelta(hours=1)) as client:
            ttl_seconds = 0.5
            ttl = CollectionTtl(ttl=timedelta(seconds=ttl_seconds), refresh_ttl=False)

            for element in elements:
                set_adder(client, cache_name, set_name, element, ttl)

            sleep(ttl_seconds * 2)

            fetch_resp = client.set_fetch(cache_name, set_name)
            assert isinstance(fetch_resp, CacheSetFetch.Miss)

    def it_refreshes_the_ttl(
        client: SimpleCacheClient, set_adder: TSetAdder, cache_name: TCacheName, set_name: TSetName
    ) -> None:
        ttl_seconds = 1
        ttl = CollectionTtl.of(timedelta(seconds=ttl_seconds))
        elements = {"one", "two", "three", "four"}

        for element in elements:
            set_adder(client, cache_name, set_name, element, ttl)
            sleep(ttl_seconds / 2)

        fetch_resp = client.set_fetch(cache_name, set_name)
        assert isinstance(fetch_resp, CacheSetFetch.Hit)
        assert fetch_resp.value_set_string == elements

    def it_uses_the_default_ttl_when_the_collection_ttl_has_no_ttl(
        configuration: Configuration,
        credential_provider: EnvMomentoTokenProvider,
        set_adder: TSetAdder,
        cache_name: TCacheName,
        set_name: TSetName,
    ) -> None:
        ttl_seconds = 1
        with SimpleCacheClient(configuration, credential_provider, timedelta(seconds=ttl_seconds)) as client:
            ttl = CollectionTtl.from_cache_ttl().with_no_refresh_ttl_on_updates()

            element = uuid_str()
            set_adder(client, cache_name, set_name, element, ttl)

            sleep(ttl_seconds / 2)

            fetch_resp = client.set_fetch(cache_name, set_name)
            assert isinstance(fetch_resp, CacheSetFetch.Hit)
            assert fetch_resp.value_set_string == {element}

            sleep(ttl_seconds / 2)
            fetch_resp = client.set_fetch(cache_name, set_name)
            assert isinstance(fetch_resp, CacheSetFetch.Miss)


TSetNameValidator = Callable[[TCacheName, TSetName], CacheResponse]


def a_set_name_validator() -> None:
    def with_null_set_name_it_returns_invalid(set_name_validator: TSetNameValidator, cache_name: TCacheName) -> None:
        response = set_name_validator(cache_name, None)  # type: ignore
        assert isinstance(response, ErrorResponseMixin)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
        assert response.inner_exception.message == "Set name must be a string"

    def with_empty_set_name_it_returns_invalid(set_name_validator: TSetNameValidator, cache_name: TCacheName) -> None:
        response = set_name_validator(cache_name, "")
        assert isinstance(response, ErrorResponseMixin)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
        assert response.inner_exception.message == "Set name must not be empty"

    def with_bad_set_name_it_returns_invalid(set_name_validator: TCacheNameValidator, cache_name: TCacheName) -> None:
        response = set_name_validator(cache_name, 1)  # type: ignore
        assert isinstance(response, ErrorResponseMixin)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
        assert response.inner_exception.message == "Set name must be a string"


@behaves_like(a_cache_name_validator)
@behaves_like(a_connection_validator)
@behaves_like(a_set_adder)
@behaves_like(a_set_name_validator)
def describe_set_add_element() -> None:
    @fixture
    def cache_name_validator(
        client: SimpleCacheClient, set_name: TSetName, element: TSetElement
    ) -> TCacheNameValidator:
        return partial(client.set_add_element, set_name=set_name, element=element)

    @fixture
    def connection_validator() -> TConnectionValidator:
        def _connection_validator(client: SimpleCacheClient, cache_name: TCacheName) -> ErrorResponseMixin:
            set_name = uuid_str()
            element = uuid_str()
            return client.set_add_element(cache_name=cache_name, set_name=set_name, element=element)

        return _connection_validator

    @fixture
    def set_adder(client: SimpleCacheClient) -> TSetAdder:
        def _set_adder(
            client: SimpleCacheClient,
            cache_name: TCacheName,
            set_name: TSetName,
            element: TSetElement,
            ttl: CollectionTtl,
        ) -> CacheResponse:
            return client.set_add_element(cache_name, set_name, element, ttl)

        return _set_adder

    @fixture
    def set_name_validator(client: SimpleCacheClient, element: TSetElement) -> TSetNameValidator:
        return partial(client.set_add_element, element=element)

    def it_adds_a_string_element(client: SimpleCacheClient, cache_name: TCacheName, set_name: TSetName) -> None:
        element1 = uuid_str()
        element2 = uuid_str()

        add_resp = client.set_add_element(cache_name, set_name, element1)
        assert isinstance(add_resp, CacheSetAddElement.Success)

        fetch_resp = client.set_fetch(cache_name, set_name)
        assert isinstance(fetch_resp, CacheSetFetch.Hit)
        assert fetch_resp.value_set_string == {element1}

        add_resp = client.set_add_element(cache_name, set_name, element1)
        assert isinstance(add_resp, CacheSetAddElement.Success)
        add_resp = client.set_add_element(cache_name, set_name, element2)
        assert isinstance(add_resp, CacheSetAddElement.Success)

        fetch_resp = client.set_fetch(cache_name, set_name)
        assert isinstance(fetch_resp, CacheSetFetch.Hit)
        assert fetch_resp.value_set_string == {element1, element2}

    def it_adds_a_byte_element(client: SimpleCacheClient, cache_name: TCacheName, set_name: TSetName) -> None:
        element1 = uuid_bytes()
        element2 = uuid_bytes()

        add_resp = client.set_add_element(cache_name, set_name, element1)
        assert isinstance(add_resp, CacheSetAddElement.Success)

        fetch_resp = client.set_fetch(cache_name, set_name)
        assert isinstance(fetch_resp, CacheSetFetch.Hit)
        assert fetch_resp.value_set_bytes == {element1}

        add_resp = client.set_add_element(cache_name, set_name, element1)
        assert isinstance(add_resp, CacheSetAddElement.Success)
        add_resp = client.set_add_element(cache_name, set_name, element2)
        assert isinstance(add_resp, CacheSetAddElement.Success)

        fetch_resp = client.set_fetch(cache_name, set_name)
        assert isinstance(fetch_resp, CacheSetFetch.Hit)
        assert fetch_resp.value_set_bytes == {element1, element2}


@behaves_like(a_cache_name_validator)
@behaves_like(a_connection_validator)
@behaves_like(a_set_adder)
@behaves_like(a_set_name_validator)
def describe_set_add_elements() -> None:
    @fixture
    def cache_name_validator(
        client: SimpleCacheClient, set_name: TSetName, elements: TSetElementsInput
    ) -> TCacheNameValidator:
        return partial(client.set_add_elements, set_name=set_name, elements=elements)

    @fixture
    def connection_validator() -> TConnectionValidator:
        def _connection_validator(client: SimpleCacheClient, cache_name: TCacheName) -> ErrorResponseMixin:
            set_name = uuid_str()
            elements = {uuid_str()}
            return client.set_add_elements(cache_name=cache_name, set_name=set_name, elements=elements)

        return _connection_validator

    @fixture
    def set_adder(client: SimpleCacheClient) -> TSetAdder:
        def _set_adder(
            client: SimpleCacheClient,
            cache_name: TCacheName,
            set_name: TSetName,
            element: TSetElement,
            ttl: CollectionTtl,
        ) -> CacheResponse:
            return client.set_add_elements(cache_name, set_name, {element}, ttl)

        return _set_adder

    @fixture
    def set_name_validator(client: SimpleCacheClient, elements: TSetElementsInput) -> TSetNameValidator:
        return partial(client.set_add_elements, elements=elements)

    def it_adds_string_elements(
        client: SimpleCacheClient,
        cache_name: TCacheName,
        set_name: TSetName,
        elements_str: TSetElementsInputStr,
    ) -> None:
        add_resp = client.set_add_elements(cache_name, set_name, elements_str)
        assert isinstance(add_resp, CacheSetAddElements.Success)

        fetch_resp = client.set_fetch(cache_name, set_name)
        assert isinstance(fetch_resp, CacheSetFetch.Hit)
        assert fetch_resp.value_set_string == elements_str

        elements_str.add("another")
        add_resp = client.set_add_elements(cache_name, set_name, elements_str)
        assert isinstance(add_resp, CacheSetAddElements.Success)

        fetch_resp = client.set_fetch(cache_name, set_name)
        assert isinstance(fetch_resp, CacheSetFetch.Hit)
        assert fetch_resp.value_set_string == elements_str

    def it_adds_byte_elements(
        client: SimpleCacheClient,
        cache_name: TCacheName,
        set_name: TSetName,
        elements_bytes: TSetElementsInputBytes,
    ) -> None:
        add_resp = client.set_add_elements(cache_name, set_name, elements_bytes)
        assert isinstance(add_resp, CacheSetAddElements.Success)

        fetch_resp = client.set_fetch(cache_name, set_name)
        assert isinstance(fetch_resp, CacheSetFetch.Hit)
        assert fetch_resp.value_set_bytes == elements_bytes

        elements_bytes.add(b"another")

        add_resp = client.set_add_elements(cache_name, set_name, elements_bytes)
        assert isinstance(add_resp, CacheSetAddElements.Success)

        fetch_resp = client.set_fetch(cache_name, set_name)
        assert isinstance(fetch_resp, CacheSetFetch.Hit)
        assert fetch_resp.value_set_bytes == elements_bytes


@behaves_like(a_cache_name_validator)
@behaves_like(a_connection_validator)
@behaves_like(a_set_name_validator)
def describe_set_fetch() -> None:
    @fixture
    def cache_name_validator(client: SimpleCacheClient, set_name: TSetName) -> TCacheNameValidator:
        return partial(client.set_fetch, set_name=set_name)

    @fixture
    def connection_validator() -> TConnectionValidator:
        def _connection_validator(client: SimpleCacheClient, cache_name: TCacheName) -> ErrorResponseMixin:
            set_name = uuid_str()
            return client.set_fetch(cache_name=cache_name, set_name=set_name)

        return _connection_validator

    @fixture
    def set_name_validator(client: SimpleCacheClient) -> TSetNameValidator:
        return partial(client.set_fetch)

    def when_the_set_exists_it_fetches(client: SimpleCacheClient, cache_name: TCacheName, set_name: TSetName) -> None:
        elements = {"one", "two"}
        client.set_add_elements(cache_name, set_name, elements)

        resp = client.set_fetch(cache_name, set_name)
        assert isinstance(resp, CacheSetFetch.Hit)
        assert resp.value_set_string == elements
        assert resp.value_set_bytes == {b"one", b"two"}

    def when_the_set_does_not_exist_it_misses(
        client: SimpleCacheClient, cache_name: TCacheName, set_name: TSetName
    ) -> None:
        resp = client.set_fetch(cache_name, set_name)
        assert isinstance(resp, CacheSetFetch.Miss)


@behaves_like(a_cache_name_validator)
@behaves_like(a_connection_validator)
@behaves_like(a_set_name_validator)
def describe_set_remove_element() -> None:
    @fixture
    def cache_name_validator(
        client: SimpleCacheClient, set_name: TSetName, element: TSetElement
    ) -> TCacheNameValidator:
        return partial(client.set_remove_element, set_name=set_name, element=element)

    @fixture
    def connection_validator() -> TConnectionValidator:
        def _connection_validator(client: SimpleCacheClient, cache_name: TCacheName) -> ErrorResponseMixin:
            set_name = uuid_str()
            element = uuid_str()
            return client.set_remove_element(cache_name=cache_name, set_name=set_name, element=element)

        return _connection_validator

    @fixture
    def set_name_validator(client: SimpleCacheClient, element: TSetElement) -> TSetNameValidator:
        return partial(client.set_remove_element, element=element)

    def it_removes_a_string_element(
        client: SimpleCacheClient,
        cache_name: TCacheName,
        set_name: TSetName,
    ) -> None:
        element = uuid_str()

        remove_resp = client.set_remove_element(cache_name, set_name, element)
        assert isinstance(remove_resp, CacheSetRemoveElement.Success)

        new_elements = {uuid_str(), uuid_str()}
        client.set_add_element(cache_name, set_name, element)
        client.set_add_elements(cache_name, set_name, new_elements)

        fetch_resp = client.set_fetch(cache_name, set_name)
        assert isinstance(fetch_resp, CacheSetFetch.Hit)
        assert fetch_resp.value_set_string == {element}.union(new_elements)

        remove_resp = client.set_remove_element(cache_name, set_name, element)
        assert isinstance(remove_resp, CacheSetRemoveElement.Success)

        fetch_resp = client.set_fetch(cache_name, set_name)
        assert isinstance(fetch_resp, CacheSetFetch.Hit)
        assert fetch_resp.value_set_string == new_elements

    def it_removes_a_byte_element(
        client: SimpleCacheClient,
        cache_name: TCacheName,
        set_name: TSetName,
    ) -> None:
        element = uuid_bytes()

        remove_resp = client.set_remove_element(cache_name, set_name, element)
        assert isinstance(remove_resp, CacheSetRemoveElement.Success)

        new_elements = {uuid_bytes(), uuid_bytes()}
        client.set_add_element(cache_name, set_name, element)
        client.set_add_elements(cache_name, set_name, new_elements)

        fetch_resp = client.set_fetch(cache_name, set_name)
        assert isinstance(fetch_resp, CacheSetFetch.Hit)
        assert fetch_resp.value_set_bytes == {element}.union(new_elements)

        remove_resp = client.set_remove_element(cache_name, set_name, element)
        assert isinstance(remove_resp, CacheSetRemoveElement.Success)

        fetch_resp = client.set_fetch(cache_name, set_name)
        assert isinstance(fetch_resp, CacheSetFetch.Hit)
        assert fetch_resp.value_set_bytes == new_elements


@behaves_like(a_cache_name_validator)
@behaves_like(a_connection_validator)
@behaves_like(a_set_name_validator)
def describe_set_remove_elements() -> None:
    @fixture
    def cache_name_validator(
        client: SimpleCacheClient, set_name: TSetName, elements: TSetElementsInput
    ) -> TCacheNameValidator:
        return partial(client.set_remove_elements, set_name=set_name, elements=elements)

    @fixture
    def connection_validator() -> TConnectionValidator:
        def _connection_validator(client: SimpleCacheClient, cache_name: TCacheName) -> ErrorResponseMixin:
            set_name = uuid_str()
            elements = {uuid_str()}
            return client.set_remove_elements(cache_name=cache_name, set_name=set_name, elements=elements)

        return _connection_validator

    @fixture
    def set_name_validator(client: SimpleCacheClient, elements: TSetElementsInput) -> TSetNameValidator:
        return partial(client.set_remove_elements, elements=elements)

    def it_removes_string_elements(
        client: SimpleCacheClient,
        cache_name: TCacheName,
        set_name: TSetName,
        elements_str: TSetElementsInputStr,
    ) -> None:
        remove_resp = client.set_remove_elements(cache_name, set_name, elements_str)
        assert isinstance(remove_resp, CacheSetRemoveElements.Success)

        new_elements = {uuid_str(), uuid_str()}
        client.set_add_elements(cache_name, set_name, elements_str.union(new_elements))

        fetch_resp = client.set_fetch(cache_name, set_name)
        assert isinstance(fetch_resp, CacheSetFetch.Hit)
        assert fetch_resp.value_set_string == elements_str.union(new_elements)

        remove_resp = client.set_remove_elements(cache_name, set_name, elements_str)
        assert isinstance(remove_resp, CacheSetRemoveElements.Success)

        fetch_resp = client.set_fetch(cache_name, set_name)
        assert isinstance(fetch_resp, CacheSetFetch.Hit)
        assert fetch_resp.value_set_string == new_elements

    def it_removes_bytes_elements(
        client: SimpleCacheClient,
        cache_name: TCacheName,
        set_name: TSetName,
        elements_bytes: TSetElementsInputStr,
    ) -> None:
        remove_resp = client.set_remove_elements(cache_name, set_name, elements_bytes)
        assert isinstance(remove_resp, CacheSetRemoveElements.Success)

        new_elements = {uuid_bytes(), uuid_bytes()}
        client.set_add_elements(cache_name, set_name, elements_bytes.union(new_elements))

        fetch_resp = client.set_fetch(cache_name, set_name)
        assert isinstance(fetch_resp, CacheSetFetch.Hit)
        assert fetch_resp.value_set_bytes == elements_bytes.union(new_elements)

        remove_resp = client.set_remove_elements(cache_name, set_name, elements_bytes)
        assert isinstance(remove_resp, CacheSetRemoveElements.Success)

        fetch_resp = client.set_fetch(cache_name, set_name)
        assert isinstance(fetch_resp, CacheSetFetch.Hit)
        assert fetch_resp.value_set_bytes == new_elements

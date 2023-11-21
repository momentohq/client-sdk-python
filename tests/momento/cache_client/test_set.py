from __future__ import annotations

from datetime import timedelta
from functools import partial
from time import sleep

from momento import CacheClient
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
from pytest import fixture
from pytest_describe import behaves_like
from typing_extensions import Protocol

from tests.utils import uuid_bytes, uuid_str

from .shared_behaviors import (
    TCacheNameValidator,
    TConnectionValidator,
    a_cache_name_validator,
    a_connection_validator,
)


class TSetAdder(Protocol):
    def __call__(
        self,
        client: CacheClient,
        cache_name: TCacheName,
        set_name: TSetName,
        element: TSetElement,
        *,
        ttl: CollectionTtl,
    ) -> CacheResponse:
        ...


def a_set_adder() -> None:
    def it_sets_the_ttl(
        configuration: Configuration,
        credential_provider: CredentialProvider,
        set_adder: TSetAdder,
        cache_name: TCacheName,
        set_name: TSetName,
        elements: TSetElementsInput,
    ) -> None:
        with CacheClient(configuration, credential_provider, timedelta(hours=1)) as client:
            ttl_seconds = 0.5
            ttl = CollectionTtl(ttl=timedelta(seconds=ttl_seconds), refresh_ttl=False)

            for element in elements:
                set_adder(client, cache_name, set_name, element, ttl=ttl)

            sleep(ttl_seconds * 2)

            fetch_resp = client.set_fetch(cache_name, set_name)
            assert isinstance(fetch_resp, CacheSetFetch.Miss)

    def it_refreshes_the_ttl(
        client: CacheClient, set_adder: TSetAdder, cache_name: TCacheName, set_name: TSetName
    ) -> None:
        ttl_seconds = 1
        ttl = CollectionTtl.of(timedelta(seconds=ttl_seconds))
        elements = {"one", "two", "three", "four"}

        for element in elements:
            set_adder(client, cache_name, set_name, element, ttl=ttl)
            sleep(ttl_seconds / 2)

        fetch_resp = client.set_fetch(cache_name, set_name)
        assert isinstance(fetch_resp, CacheSetFetch.Hit)
        assert fetch_resp.value_set_string == elements

    def it_uses_the_default_ttl_when_the_collection_ttl_has_no_ttl(
        configuration: Configuration,
        credential_provider: CredentialProvider,
        set_adder: TSetAdder,
        cache_name: TCacheName,
        set_name: TSetName,
    ) -> None:
        ttl_seconds = 1
        with CacheClient(configuration, credential_provider, timedelta(seconds=ttl_seconds)) as client:
            ttl = CollectionTtl.from_cache_ttl().with_no_refresh_ttl_on_updates()

            element = uuid_str()
            set_adder(client, cache_name, set_name, element, ttl=ttl)

            sleep(ttl_seconds / 2)

            fetch_resp = client.set_fetch(cache_name, set_name)
            assert isinstance(fetch_resp, CacheSetFetch.Hit)
            assert fetch_resp.value_set_string == {element}

            sleep(ttl_seconds / 2)
            fetch_resp = client.set_fetch(cache_name, set_name)
            assert isinstance(fetch_resp, CacheSetFetch.Miss)


class TSetWhichTakesAnElement(Protocol):
    def __call__(self, cache_name: TCacheName, set_name: TSetName, element: TSetElement) -> CacheResponse:
        ...


def a_set_which_takes_an_element() -> None:
    def it_errors_with_the_wrong_type(
        set_which_takes_an_element: TSetWhichTakesAnElement,
        cache_name: TCacheName,
        set_name: TSetName,
    ) -> None:
        resp = set_which_takes_an_element(cache_name, set_name, 1)  # type:ignore[arg-type]
        if isinstance(resp, ErrorResponseMixin):
            assert resp.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
            assert resp.inner_exception.message == "Could not convert the given type to bytes: <class 'int'>"
        else:
            raise AssertionError("Expected an error response")

    def it_errors_with_none(
        set_which_takes_an_element: TSetWhichTakesAnElement,
        cache_name: TCacheName,
        set_name: TSetName,
    ) -> None:
        resp = set_which_takes_an_element(cache_name, set_name, None)  # type:ignore[arg-type]
        if isinstance(resp, ErrorResponseMixin):
            assert resp.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
            assert resp.inner_exception.message == "Could not convert the given type to bytes: <class 'NoneType'>"
        else:
            raise AssertionError("Expected an error response")


class TSetNameValidator(Protocol):
    def __call__(self, set_name: TSetName) -> CacheResponse:
        ...


def a_set_name_validator() -> None:
    def with_null_set_name_it_returns_invalid(set_name_validator: TSetNameValidator) -> None:
        response = set_name_validator(set_name=None)  # type: ignore
        if isinstance(response, ErrorResponseMixin):
            assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
            assert response.inner_exception.message == "Set name must be a string"
        else:
            raise AssertionError("Expected an error response")

    def with_empty_set_name_it_returns_invalid(set_name_validator: TSetNameValidator) -> None:
        response = set_name_validator(set_name="")
        assert isinstance(response, ErrorResponseMixin)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
        assert response.inner_exception.message == "Set name must not be empty"

    def with_bad_set_name_it_returns_invalid(set_name_validator: TCacheNameValidator) -> None:
        response = set_name_validator(set_name=1)  # type: ignore
        assert isinstance(response, ErrorResponseMixin)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
        assert response.inner_exception.message == "Set name must be a string"


@behaves_like(
    a_cache_name_validator,
    a_connection_validator,
    a_set_adder,
    a_set_name_validator,
    a_set_which_takes_an_element,
)
def describe_set_add_element() -> None:
    @fixture
    def cache_name_validator(client: CacheClient, set_name: TSetName, element: TSetElement) -> TCacheNameValidator:
        return partial(client.set_add_element, set_name=set_name, element=element)

    @fixture
    def connection_validator(cache_name: TCacheName) -> TConnectionValidator:
        def _connection_validator(client: CacheClient) -> CacheResponse:
            set_name = uuid_str()
            element = uuid_str()
            return client.set_add_element(cache_name=cache_name, set_name=set_name, element=element)

        return _connection_validator

    @fixture
    def set_adder(client: CacheClient) -> TSetAdder:
        def _set_adder(
            client: CacheClient,
            cache_name: TCacheName,
            set_name: TSetName,
            element: TSetElement,
            *,
            ttl: CollectionTtl,
        ) -> CacheResponse:
            return client.set_add_element(cache_name, set_name, element, ttl=ttl)

        return _set_adder

    @fixture
    def set_name_validator(client: CacheClient, cache_name: TCacheName, element: TSetElement) -> TSetNameValidator:
        return partial(client.set_add_element, cache_name=cache_name, element=element)

    @fixture
    def set_which_takes_an_element(client: CacheClient) -> TSetWhichTakesAnElement:
        def _set_which_takes_an_element(
            cache_name: TCacheName, set_name: TSetName, element: TSetElement
        ) -> CacheResponse:
            return client.set_add_element(cache_name, set_name, element)

        return _set_which_takes_an_element

    def it_adds_a_string_element(client: CacheClient, cache_name: TCacheName, set_name: TSetName) -> None:
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

    def it_adds_a_byte_element(client: CacheClient, cache_name: TCacheName, set_name: TSetName) -> None:
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


@behaves_like(
    a_cache_name_validator,
    a_connection_validator,
    a_set_adder,
    a_set_name_validator,
    a_set_which_takes_an_element,
)
def describe_set_add_elements() -> None:
    @fixture
    def cache_name_validator(
        client: CacheClient, set_name: TSetName, elements: TSetElementsInput
    ) -> TCacheNameValidator:
        return partial(client.set_add_elements, set_name=set_name, elements=elements)

    @fixture
    def connection_validator(cache_name: TCacheName) -> TConnectionValidator:
        def _connection_validator(client: CacheClient) -> CacheResponse:
            set_name = uuid_str()
            elements = {uuid_str()}
            return client.set_add_elements(cache_name=cache_name, set_name=set_name, elements=elements)

        return _connection_validator

    @fixture
    def set_adder(client: CacheClient) -> TSetAdder:
        def _set_adder(
            client: CacheClient,
            cache_name: TCacheName,
            set_name: TSetName,
            element: TSetElement,
            *,
            ttl: CollectionTtl,
        ) -> CacheResponse:
            return client.set_add_elements(cache_name, set_name, {element}, ttl=ttl)

        return _set_adder

    @fixture
    def set_name_validator(
        client: CacheClient, cache_name: TCacheName, elements: TSetElementsInput
    ) -> TSetNameValidator:
        return partial(client.set_add_elements, cache_name=cache_name, elements=elements)

    @fixture
    def set_which_takes_an_element(client: CacheClient) -> TSetWhichTakesAnElement:
        def _set_which_takes_an_element(
            cache_name: TCacheName, set_name: TSetName, element: TSetElement
        ) -> CacheResponse:
            return client.set_add_elements(cache_name, set_name, {element})

        return _set_which_takes_an_element

    def it_adds_string_elements(
        client: CacheClient,
        cache_name: TCacheName,
        set_name: TSetName,
        elements_str: set[str],
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
        client: CacheClient,
        cache_name: TCacheName,
        set_name: TSetName,
        elements_bytes: set[bytes],
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


@behaves_like(
    a_cache_name_validator,
    a_connection_validator,
    a_set_name_validator,
)
def describe_set_fetch() -> None:
    @fixture
    def cache_name_validator(client: CacheClient, set_name: TSetName) -> TCacheNameValidator:
        return partial(client.set_fetch, set_name=set_name)

    @fixture
    def connection_validator(cache_name: TCacheName) -> TConnectionValidator:
        def _connection_validator(client: CacheClient) -> CacheResponse:
            set_name = uuid_str()
            return client.set_fetch(cache_name=cache_name, set_name=set_name)

        return _connection_validator

    @fixture
    def set_name_validator(client: CacheClient, cache_name: TCacheName) -> TSetNameValidator:
        return partial(client.set_fetch, cache_name=cache_name)

    def when_the_set_exists_it_fetches(client: CacheClient, cache_name: TCacheName, set_name: TSetName) -> None:
        elements = {"one", "two"}
        client.set_add_elements(cache_name, set_name, elements)

        resp = client.set_fetch(cache_name, set_name)
        assert isinstance(resp, CacheSetFetch.Hit)
        assert resp.value_set_string == elements
        assert resp.value_set_bytes == {b"one", b"two"}

    def when_the_set_does_not_exist_it_misses(client: CacheClient, cache_name: TCacheName, set_name: TSetName) -> None:
        resp = client.set_fetch(cache_name, set_name)
        assert isinstance(resp, CacheSetFetch.Miss)


@behaves_like(
    a_cache_name_validator,
    a_connection_validator,
    a_set_name_validator,
    a_set_which_takes_an_element,
)
def describe_set_remove_element() -> None:
    @fixture
    def cache_name_validator(client: CacheClient, set_name: TSetName, element: TSetElement) -> TCacheNameValidator:
        return partial(client.set_remove_element, set_name=set_name, element=element)

    @fixture
    def connection_validator(cache_name: TCacheName) -> TConnectionValidator:
        def _connection_validator(client: CacheClient) -> CacheResponse:
            set_name = uuid_str()
            element = uuid_str()
            return client.set_remove_element(cache_name=cache_name, set_name=set_name, element=element)

        return _connection_validator

    @fixture
    def set_name_validator(client: CacheClient, cache_name: TCacheName, element: TSetElement) -> TSetNameValidator:
        return partial(client.set_remove_element, cache_name=cache_name, element=element)

    @fixture
    def set_which_takes_an_element(client: CacheClient) -> TSetWhichTakesAnElement:
        def _set_which_takes_an_element(
            cache_name: TCacheName, set_name: TSetName, element: TSetElement
        ) -> CacheResponse:
            return client.set_remove_element(cache_name, set_name, element)

        return _set_which_takes_an_element

    def it_removes_a_string_element(
        client: CacheClient,
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
        client: CacheClient,
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


@behaves_like(
    a_cache_name_validator,
    a_connection_validator,
    a_set_name_validator,
    a_set_which_takes_an_element,
)
def describe_set_remove_elements() -> None:
    @fixture
    def cache_name_validator(
        client: CacheClient, set_name: TSetName, elements: TSetElementsInput
    ) -> TCacheNameValidator:
        return partial(client.set_remove_elements, set_name=set_name, elements=elements)

    @fixture
    def connection_validator(cache_name: TCacheName) -> TConnectionValidator:
        def _connection_validator(client: CacheClient) -> CacheResponse:
            set_name = uuid_str()
            elements = {uuid_str()}
            return client.set_remove_elements(cache_name=cache_name, set_name=set_name, elements=elements)

        return _connection_validator

    @fixture
    def set_name_validator(
        client: CacheClient, cache_name: TCacheName, elements: TSetElementsInput
    ) -> TSetNameValidator:
        return partial(client.set_remove_elements, cache_name=cache_name, elements=elements)

    @fixture
    def set_which_takes_an_element(client: CacheClient) -> TSetWhichTakesAnElement:
        def _set_which_takes_an_element(
            cache_name: TCacheName, set_name: TSetName, element: TSetElement
        ) -> CacheResponse:
            return client.set_add_elements(cache_name, set_name, {element})

        return _set_which_takes_an_element

    def it_removes_string_elements(
        client: CacheClient,
        cache_name: TCacheName,
        set_name: TSetName,
        elements_str: set[str],
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
        client: CacheClient,
        cache_name: TCacheName,
        set_name: TSetName,
        elements_bytes: set[bytes],
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

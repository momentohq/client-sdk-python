from datetime import timedelta
from functools import partial
from time import sleep
from typing import Awaitable, Callable

from pytest import fixture
from pytest_describe import behaves_like

from momento import SimpleCacheClientAsync
from momento.auth import CredentialProvider
from momento.config import Configuration
from momento.errors import MomentoErrorCode
from momento.internal._utilities import _dictionary_items_as_bytes
from momento.requests import CollectionTtl
from momento.responses import (
    CacheDictionaryFetch,
    CacheDictionaryRemoveField,
    CacheDictionaryRemoveFields,
    CacheDictionarySetField,
    CacheDictionarySetFields,
    CacheResponse,
)
from momento.responses.mixins import ErrorResponseMixin
from momento.typing import (
    TCacheName,
    TDictionaryField,
    TDictionaryFields,
    TDictionaryItems,
    TDictionaryName,
    TDictionaryValue,
)

from .shared_behaviors_async import (
    TCacheNameValidator,
    TConnectionValidator,
    a_cache_name_validator,
    a_connection_validator,
)

TFieldValidator = Callable[[TDictionaryField], Awaitable[CacheResponse]]


def a_dictionary_field_validator() -> None:
    async def with_null_field_throws_exception(dictionary_field_validator: TFieldValidator) -> None:
        response = await dictionary_field_validator(field=None)  # type: ignore
        assert isinstance(response, ErrorResponseMixin)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR

    async def with_wrong_container_throws_exception(dictionary_field_validator: TFieldValidator) -> None:
        response = await dictionary_field_validator(field=Exception())  # type: ignore
        assert isinstance(response, ErrorResponseMixin)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR


TFieldsValidator = Callable[[TDictionaryFields], Awaitable[CacheResponse]]


def a_dictionary_fields_validator() -> None:
    async def with_null_fields_throws_exception(dictionary_fields_validator: TFieldsValidator) -> None:
        response = await dictionary_fields_validator(fields=None)  # type: ignore
        assert isinstance(response, ErrorResponseMixin)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR

    async def with_wrong_container_throws_exception(dictionary_fields_validator: TFieldsValidator) -> None:
        response = await dictionary_fields_validator(fields=Exception())  # type: ignore
        assert isinstance(response, ErrorResponseMixin)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR


TItemsValidator = Callable[[TDictionaryItems], Awaitable[CacheResponse]]


def a_dictionary_items_validator() -> None:
    async def with_null_items_throws_exception(dictionary_items_validator: TItemsValidator) -> None:
        response = await dictionary_items_validator(items=None)  # type: ignore
        assert isinstance(response, ErrorResponseMixin)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR

    async def with_wrong_container_throws_exception(dictionary_items_validator: TItemsValidator) -> None:
        response = await dictionary_items_validator(items=[1, 2, 3])  # type: ignore
        assert isinstance(response, ErrorResponseMixin)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR


TDictionaryNameValidator = Callable[[TDictionaryName], Awaitable[CacheResponse]]


def a_dictionary_name_validator() -> None:
    async def with_null_dictionary_name_it_returns_invalid(dictionary_name_validator: TDictionaryNameValidator) -> None:
        response = await dictionary_name_validator(dictionary_name=None)  # type: ignore
        assert isinstance(response, ErrorResponseMixin)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
        assert response.inner_exception.message == "Dictionary name must be a string"

    async def with_empty_dictionary_name_it_returns_invalid(
        dictionary_name_validator: TDictionaryNameValidator,
    ) -> None:
        response = await dictionary_name_validator(dictionary_name="")
        assert isinstance(response, ErrorResponseMixin)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
        assert response.inner_exception.message == "Dictionary name must not be empty"

    async def with_bad_dictionary_name_it_returns_invalid(dictionary_name_validator: TDictionaryNameValidator) -> None:
        response = await dictionary_name_validator(dictionary_name=1)  # type: ignore
        assert isinstance(response, ErrorResponseMixin)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
        assert response.inner_exception.message == "Dictionary name must be a string"


TDictionaryRemover = Callable[[SimpleCacheClientAsync, TDictionaryName, TDictionaryField], Awaitable[CacheResponse]]


def a_dictionary_remover() -> None:
    async def it_removes_an_item(
        client_async: SimpleCacheClientAsync,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
        dictionary_field: TDictionaryField,
        dictionary_value: TDictionaryValue,
        dictionary_remover: TDictionaryRemover,
    ) -> None:
        set_response = await client_async.dictionary_set_field(
            cache_name, dictionary_name, dictionary_field, dictionary_value
        )
        assert isinstance(set_response, CacheDictionarySetField.Success)

        remove_response = await dictionary_remover(client_async, dictionary_name, dictionary_field)
        assert not isinstance(remove_response, ErrorResponseMixin)

        fetch_response = await client_async.dictionary_fetch(cache_name, dictionary_name)
        assert isinstance(fetch_response, CacheDictionaryFetch.Miss)


TDictionarySetter = Callable[
    [SimpleCacheClientAsync, TDictionaryName, TDictionaryField, TDictionaryValue, CollectionTtl],
    Awaitable[CacheResponse],
]


def a_dictionary_setter() -> None:
    async def it_sets_the_ttl(
        configuration: Configuration,
        credential_provider: CredentialProvider,
        dictionary_setter: TDictionarySetter,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
        dictionary_items: TDictionaryItems,
    ) -> None:
        async with SimpleCacheClientAsync(configuration, credential_provider, timedelta(hours=1)) as client:
            ttl_seconds = 0.5
            ttl = CollectionTtl(ttl=timedelta(seconds=ttl_seconds), refresh_ttl=False)

            for field, value in dictionary_items.items():
                await dictionary_setter(client, dictionary_name, field, value, ttl)

            sleep(ttl_seconds * 2)

            fetch_response = await client.dictionary_fetch(cache_name, dictionary_name)
            assert isinstance(fetch_response, CacheDictionaryFetch.Miss)

    async def it_refreshes_the_ttl(
        client_async: SimpleCacheClientAsync,
        dictionary_setter: TDictionarySetter,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
    ) -> None:
        ttl_seconds = 1
        ttl = CollectionTtl.of(timedelta(seconds=ttl_seconds))
        items = dict([("one", "1"), ("two", "2"), ("three", "3"), ("four", "4")])

        for field, value in items.items():
            await dictionary_setter(client_async, dictionary_name, field, value, ttl)
            sleep(ttl_seconds / 2)

        fetch_response = await client_async.dictionary_fetch(cache_name, dictionary_name)
        assert isinstance(fetch_response, CacheDictionaryFetch.Hit)
        assert fetch_response.value_dictionary_string_string == items

    async def it_uses_the_default_ttl_when_the_collection_ttl_has_no_ttl(
        configuration: Configuration,
        credential_provider: CredentialProvider,
        dictionary_setter: TDictionarySetter,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
        dictionary_field: TDictionaryField,
        dictionary_value: TDictionaryValue,
    ) -> None:
        ttl_seconds = 1
        async with SimpleCacheClientAsync(configuration, credential_provider, timedelta(seconds=ttl_seconds)) as client:
            ttl = CollectionTtl.from_cache_ttl().with_no_refresh_ttl_on_updates()

            await dictionary_setter(client, dictionary_name, dictionary_field, dictionary_value, ttl)

            sleep(ttl_seconds / 2)

            fetch_response = await client.dictionary_fetch(cache_name, dictionary_name)
            assert isinstance(fetch_response, CacheDictionaryFetch.Hit)
            assert fetch_response.value_dictionary_string_string == {dictionary_field: dictionary_value}

            sleep(ttl_seconds / 2)
            fetch_response = await client.dictionary_fetch(cache_name, dictionary_name)
            assert isinstance(fetch_response, CacheDictionaryFetch.Miss)

    async def with_bytes_it_succeeds(
        dictionary_setter: TDictionarySetter,
        client_async: SimpleCacheClientAsync,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
        dictionary_field_bytes: TDictionaryField,
        dictionary_value_bytes: TDictionaryValue,
    ) -> None:
        await dictionary_setter(
            client_async, dictionary_name, dictionary_field_bytes, dictionary_value_bytes, CollectionTtl()
        )
        fetch_response = await client_async.dictionary_fetch(cache_name, dictionary_name)
        assert isinstance(fetch_response, CacheDictionaryFetch.Hit)
        assert fetch_response.value_dictionary_bytes_bytes == {dictionary_field_bytes: dictionary_value_bytes}

    async def with_strings_it_succeeds(
        dictionary_setter: TDictionarySetter,
        client_async: SimpleCacheClientAsync,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
        dictionary_field: TDictionaryField,
        dictionary_value: TDictionaryValue,
    ) -> None:
        await dictionary_setter(client_async, dictionary_name, dictionary_field, dictionary_value, CollectionTtl())
        fetch_response = await client_async.dictionary_fetch(cache_name, dictionary_name)
        assert isinstance(fetch_response, CacheDictionaryFetch.Hit)
        assert fetch_response.value_dictionary_string_string == {dictionary_field: dictionary_value}

    async def with_other_values_type_it_errors(
        dictionary_setter: TDictionarySetter,
        client_async: SimpleCacheClientAsync,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
    ) -> None:
        for field, value, bad_type in [
            (None, "value", "NoneType"),
            ("field", None, "NoneType"),
            (234, "value", "int"),
            ("field", 234, "int"),
        ]:
            response = await dictionary_setter(client_async, dictionary_name, field, value, CollectionTtl())  # type: ignore
            assert isinstance(response, ErrorResponseMixin)
            assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
            assert (
                response.message
                == f"Invalid argument passed to Momento client: Could not decode bytes to UTF-8<class '{bad_type}'>"
            )


@behaves_like(a_cache_name_validator)
@behaves_like(a_connection_validator)
@behaves_like(a_dictionary_name_validator)
def describe_list_fetch() -> None:
    @fixture
    def cache_name_validator(
        client_async: SimpleCacheClientAsync, dictionary_name: TDictionaryName
    ) -> TCacheNameValidator:
        return partial(client_async.dictionary_fetch, dictionary_name=dictionary_name)

    @fixture
    def connection_validator(dictionary_name: TDictionaryName) -> TConnectionValidator:
        async def _connection_validator(client_async: SimpleCacheClientAsync, cache_name: TCacheName) -> CacheResponse:
            return await client_async.dictionary_fetch(cache_name, dictionary_name=dictionary_name)

        return _connection_validator

    @fixture
    def dictionary_name_validator(
        client_async: SimpleCacheClientAsync, cache_name: TCacheName
    ) -> TDictionaryNameValidator:
        return partial(client_async.dictionary_fetch, cache_name=cache_name)

    async def misses_when_the_dictionary_does_not_exist(
        client_async: SimpleCacheClientAsync, cache_name: TCacheName, dictionary_name: TDictionaryName
    ) -> None:
        fetch_response = await client_async.dictionary_fetch(cache_name, dictionary_name)
        assert isinstance(fetch_response, CacheDictionaryFetch.Miss)


@behaves_like(a_cache_name_validator)
@behaves_like(a_connection_validator)
@behaves_like(a_dictionary_remover)
@behaves_like(a_dictionary_name_validator)
@behaves_like(a_dictionary_field_validator)
def describe_dictionary_remove_field() -> None:
    @fixture
    def cache_name_validator(
        client_async: SimpleCacheClientAsync, dictionary_name: TDictionaryName, dictionary_field: TDictionaryField
    ) -> TCacheNameValidator:
        return partial(client_async.dictionary_remove_field, dictionary_name=dictionary_name, field=dictionary_field)

    @fixture
    def connection_validator(
        dictionary_name: TDictionaryName, dictionary_field: TDictionaryField
    ) -> TConnectionValidator:
        async def _connection_validator(client_async: SimpleCacheClientAsync, cache_name: TCacheName) -> CacheResponse:
            return await client_async.dictionary_remove_field(
                cache_name, dictionary_name=dictionary_name, field=dictionary_field
            )

        return _connection_validator

    @fixture
    def dictionary_field_validator(
        client_async: SimpleCacheClientAsync,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
    ) -> TDictionaryNameValidator:
        return partial(client_async.dictionary_remove_field, cache_name=cache_name, dictionary_name=dictionary_name)

    @fixture
    def dictionary_name_validator(
        client_async: SimpleCacheClientAsync,
        cache_name: TCacheName,
        dictionary_field: TDictionaryField,
    ) -> TDictionaryNameValidator:
        return partial(client_async.dictionary_remove_field, cache_name=cache_name, field=dictionary_field)

    @fixture
    def dictionary_remover(cache_name: TCacheName) -> TDictionaryRemover:
        async def _dictionary_remover(
            client_async: SimpleCacheClientAsync,
            dictionary_name: TDictionaryName,
            field: TDictionaryField,
        ) -> CacheResponse:
            return await client_async.dictionary_remove_field(cache_name, dictionary_name, field)

        return _dictionary_remover

    async def it_removes_a_field(
        client_async: SimpleCacheClientAsync,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
        dictionary_field: TDictionaryField,
        dictionary_field_bytes: TDictionaryField,
        dictionary_value: TDictionaryValue,
    ) -> None:
        for field in [dictionary_field, dictionary_field_bytes]:
            set_response = await client_async.dictionary_set_field(cache_name, dictionary_name, field, dictionary_value)
            assert isinstance(set_response, CacheDictionarySetField.Success)

            remove_response = await client_async.dictionary_remove_field(cache_name, dictionary_name, field)
            assert isinstance(remove_response, CacheDictionaryRemoveField.Success)

            fetch_response = await client_async.dictionary_fetch(cache_name, dictionary_name)
            assert isinstance(fetch_response, CacheDictionaryFetch.Miss)


# TODO these don't work for this case?
# @behaves_like(a_cache_name_validator)
# @behaves_like(a_connection_validator)
@behaves_like(a_dictionary_remover)
@behaves_like(a_dictionary_name_validator)
@behaves_like(a_dictionary_fields_validator)
def describe_dictionary_remove_fields() -> None:
    @fixture
    def cache_name_validator(
        client_async: SimpleCacheClientAsync,
        dictionary_name: TDictionaryName,
        dictionary_fields: TDictionaryFields,
    ) -> TCacheNameValidator:
        return partial(client_async.dictionary_remove_fields, dictionary_name=dictionary_name, fields=dictionary_fields)

    @fixture
    def connection_validator(
        dictionary_name: TDictionaryName, dictionary_fields: TDictionaryFields
    ) -> TConnectionValidator:
        async def _connection_validator(client_async: SimpleCacheClientAsync, cache_name: TCacheName) -> CacheResponse:
            return await client_async.dictionary_remove_fields(
                cache_name, dictionary_name=dictionary_name, fields=dictionary_fields
            )

        return _connection_validator

    @fixture
    def dictionary_fields_validator(
        client_async: SimpleCacheClientAsync,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
    ) -> TDictionaryNameValidator:
        return partial(client_async.dictionary_remove_fields, cache_name=cache_name, dictionary_name=dictionary_name)

    @fixture
    def dictionary_name_validator(
        client_async: SimpleCacheClientAsync,
        cache_name: TCacheName,
        dictionary_fields: TDictionaryFields,
    ) -> TDictionaryNameValidator:
        return partial(client_async.dictionary_remove_fields, cache_name=cache_name, fields=dictionary_fields)

    @fixture
    def dictionary_remover(cache_name: TCacheName) -> TDictionaryRemover:
        async def _dictionary_remover(
            client_async: SimpleCacheClientAsync,
            dictionary_name: TDictionaryName,
            field: TDictionaryField,
        ) -> CacheResponse:
            return await client_async.dictionary_remove_fields(cache_name, dictionary_name, [field])

        return _dictionary_remover

    async def it_removes_multiple_items(
        client_async: SimpleCacheClientAsync,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
        dictionary_items: TDictionaryItems,
    ) -> None:
        set_response = await client_async.dictionary_set_fields(
            cache_name, dictionary_name, dictionary_items, CollectionTtl()
        )
        assert isinstance(set_response, CacheDictionarySetFields.Success)

        fields = [field for field, _ in dictionary_items.items()]
        remove_response = await client_async.dictionary_remove_fields(cache_name, dictionary_name, fields)
        assert isinstance(remove_response, CacheDictionaryRemoveFields.Success)

        fetch_response = await client_async.dictionary_fetch(cache_name, dictionary_name)
        assert isinstance(fetch_response, CacheDictionaryFetch.Miss)


@behaves_like(a_cache_name_validator)
@behaves_like(a_connection_validator)
@behaves_like(a_dictionary_setter)
@behaves_like(a_dictionary_name_validator)
def describe_dictionary_set_field() -> None:
    @fixture
    def cache_name_validator(
        client_async: SimpleCacheClientAsync,
        dictionary_name: TDictionaryName,
        dictionary_field: TDictionaryField,
        dictionary_value: TDictionaryValue,
    ) -> TCacheNameValidator:
        return partial(
            client_async.dictionary_set_field,
            dictionary_name=dictionary_name,
            field=dictionary_field,
            value=dictionary_value,
        )

    @fixture
    def connection_validator(
        dictionary_name: TDictionaryName, dictionary_field: TDictionaryField, dictionary_value: TDictionaryValue
    ) -> TConnectionValidator:
        async def _connection_validator(client_async: SimpleCacheClientAsync, cache_name: TCacheName) -> CacheResponse:
            return await client_async.dictionary_set_field(
                cache_name,
                dictionary_name=dictionary_name,
                field=dictionary_field,
                value=dictionary_value,
            )

        return _connection_validator

    @fixture
    def dictionary_name_validator(
        client_async: SimpleCacheClientAsync,
        cache_name: TCacheName,
        dictionary_field: TDictionaryField,
        dictionary_value: TDictionaryValue,
    ) -> TDictionaryNameValidator:
        return partial(
            client_async.dictionary_set_field,
            cache_name=cache_name,
            field=dictionary_field,
            value=dictionary_value,
            ttl=CollectionTtl(),
        )

    @fixture
    def dictionary_setter(cache_name: TCacheName) -> TDictionarySetter:
        async def _dictionary_setter(
            client_async: SimpleCacheClientAsync,
            dictionary_name: TDictionaryName,
            field: TDictionaryField,
            value: TDictionaryValue,
            ttl: CollectionTtl,
        ) -> CacheResponse:
            return await client_async.dictionary_set_field(cache_name, dictionary_name, field, value, ttl=ttl)

        return _dictionary_setter


@behaves_like(a_cache_name_validator)
@behaves_like(a_connection_validator)
@behaves_like(a_dictionary_setter)
@behaves_like(a_dictionary_name_validator)
@behaves_like(a_dictionary_items_validator)
def describe_dictionary_set_fields() -> None:
    @fixture
    def cache_name_validator(
        client_async: SimpleCacheClientAsync,
        dictionary_name: TDictionaryName,
        dictionary_items: TDictionaryItems,
    ) -> TCacheNameValidator:
        return partial(client_async.dictionary_set_fields, dictionary_name=dictionary_name, items=dictionary_items)

    @fixture
    def connection_validator(
        dictionary_name: TDictionaryName, dictionary_items: TDictionaryItems
    ) -> TConnectionValidator:
        async def _connection_validator(client_async: SimpleCacheClientAsync, cache_name: TCacheName) -> CacheResponse:
            return await client_async.dictionary_set_fields(
                cache_name, dictionary_name=dictionary_name, items=dictionary_items
            )

        return _connection_validator

    @fixture
    def dictionary_items_validator(
        client_async: SimpleCacheClientAsync,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
    ) -> TDictionaryNameValidator:
        return partial(
            client_async.dictionary_set_fields,
            cache_name=cache_name,
            dictionary_name=dictionary_name,
            ttl=CollectionTtl(),
        )

    @fixture
    def dictionary_name_validator(
        client_async: SimpleCacheClientAsync,
        cache_name: TCacheName,
        dictionary_items: TDictionaryItems,
    ) -> TDictionaryNameValidator:
        return partial(
            client_async.dictionary_set_fields, cache_name=cache_name, items=dictionary_items, ttl=CollectionTtl()
        )

    @fixture
    def dictionary_setter(cache_name: TCacheName) -> TDictionarySetter:
        async def _dictionary_setter(
            client_async: SimpleCacheClientAsync,
            dictionary_name: TDictionaryName,
            field: TDictionaryField,
            value: TDictionaryValue,
            ttl: CollectionTtl,
        ) -> CacheResponse:
            return await client_async.dictionary_set_fields(cache_name, dictionary_name, {field: value}, ttl=ttl)

        return _dictionary_setter

    async def it_sets_multiple_items(
        client_async: SimpleCacheClientAsync,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
        dictionary_items: TDictionaryItems,
    ) -> None:
        set_response = await client_async.dictionary_set_fields(
            cache_name, dictionary_name, dictionary_items, CollectionTtl()
        )
        assert isinstance(set_response, CacheDictionarySetFields.Success)

        fetch_response = await client_async.dictionary_fetch(cache_name, dictionary_name)
        assert isinstance(fetch_response, CacheDictionaryFetch.Hit)
        assert fetch_response.value_dictionary_bytes_bytes == _dictionary_items_as_bytes(dictionary_items)

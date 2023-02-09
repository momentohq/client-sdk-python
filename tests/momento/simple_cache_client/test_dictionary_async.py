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
from momento.internal._utilities import _gen_dictionary_items_as_bytes
from momento.requests import CollectionTtl
from momento.responses import (
    CacheDictionaryFetch,
    CacheDictionaryGetField,
    CacheDictionaryGetFieldResponse,
    CacheDictionaryGetFields,
    CacheDictionaryIncrement,
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
from tests.utils import uuid_str

from .shared_behaviors_async import (
    TCacheNameValidator,
    TConnectionValidator,
    a_cache_name_validator,
    a_connection_validator,
)


class TDictionaryFieldValidator(Protocol):
    def __call__(self, field: TDictionaryField) -> Awaitable[CacheResponse]:
        ...


def a_dictionary_field_validator() -> None:
    async def with_null_field_throws_exception(dictionary_field_validator: TDictionaryFieldValidator) -> None:
        response = await dictionary_field_validator(field=None)  # type: ignore
        assert isinstance(response, ErrorResponseMixin)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR

    async def with_wrong_container_throws_exception(dictionary_field_validator: TDictionaryFieldValidator) -> None:
        response = await dictionary_field_validator(field=Exception())  # type: ignore
        assert isinstance(response, ErrorResponseMixin)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR


class TDictionaryFieldsValidator(Protocol):
    def __call__(fields: TDictionaryFields) -> Awaitable[CacheResponse]:
        ...


def a_dictionary_fields_validator() -> None:
    async def with_null_fields_throws_exception(dictionary_fields_validator: TDictionaryFieldsValidator) -> None:
        response = await dictionary_fields_validator(fields=None)  # type: ignore
        assert isinstance(response, ErrorResponseMixin)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR

    async def with_wrong_container_throws_exception(dictionary_fields_validator: TDictionaryFieldsValidator) -> None:
        response = await dictionary_fields_validator(fields=Exception())  # type: ignore
        assert isinstance(response, ErrorResponseMixin)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR


class TDictionaryItemsValidator(Protocol):
    def __call__(self, items: TDictionaryItems) -> Awaitable[CacheResponse]:
        ...


def a_dictionary_items_validator() -> None:
    async def with_null_items_throws_exception(dictionary_items_validator: TDictionaryItemsValidator) -> None:
        response = await dictionary_items_validator(items=None)  # type: ignore
        assert isinstance(response, ErrorResponseMixin)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR

    async def with_wrong_container_throws_exception(dictionary_items_validator: TDictionaryItemsValidator) -> None:
        response = await dictionary_items_validator(items=[1, 2, 3])  # type: ignore
        assert isinstance(response, ErrorResponseMixin)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR


class TDictionaryNameValidator(Protocol):
    def __call__(self, dictionary_name: TDictionaryName) -> Awaitable[CacheResponse]:
        ...


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


class TDictionaryGetter(Protocol):
    def __call__(
        self, cache_name: TCacheName, dictionary_name: TDictionaryName, field: TDictionaryField
    ) -> Awaitable[CacheDictionaryGetFieldResponse]:
        ...


def a_dictionary_getter() -> None:
    async def with_bytes_it_succeeds(
        dictionary_getter: TDictionaryGetter,
        client_async: SimpleCacheClientAsync,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
        dictionary_field_bytes: bytes,
        dictionary_value_bytes: TDictionaryValue,
    ) -> None:
        set_response = await client_async.dictionary_set_field(
            cache_name, dictionary_name, dictionary_field_bytes, dictionary_value_bytes
        )
        assert isinstance(set_response, CacheDictionarySetField.Success)

        get_response = await dictionary_getter(
            cache_name=cache_name, dictionary_name=dictionary_name, field=dictionary_field_bytes
        )
        assert isinstance(get_response, CacheDictionaryGetField.Hit)
        assert get_response.field_bytes == dictionary_field_bytes
        assert get_response.value_bytes == dictionary_value_bytes

    async def with_string_it_succeeds(
        dictionary_getter: TDictionaryGetter,
        client_async: SimpleCacheClientAsync,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
        dictionary_field_str: str,
        dictionary_value_str: str,
    ) -> None:
        set_response = await client_async.dictionary_set_field(
            cache_name, dictionary_name, dictionary_field_str, dictionary_value_str
        )
        assert isinstance(set_response, CacheDictionarySetField.Success)

        get_response = await dictionary_getter(
            cache_name=cache_name, dictionary_name=dictionary_name, field=dictionary_field_str
        )
        assert isinstance(get_response, CacheDictionaryGetField.Hit)
        assert get_response.field_string == dictionary_field_str
        assert get_response.value_string == dictionary_value_str


class TDictionaryRemover(Protocol):
    def __call__(
        self, cache_name: TCacheName, dictionary_name: TDictionaryName, field: TDictionaryField
    ) -> Awaitable[CacheResponse]:
        ...


def a_dictionary_remover() -> None:
    async def with_string_field_succeeds(
        client_async: SimpleCacheClientAsync,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
        dictionary_field_str: str,
        dictionary_value_str: str,
        dictionary_remover: TDictionaryRemover,
    ) -> None:
        set_response = await client_async.dictionary_set_field(
            cache_name, dictionary_name, dictionary_field_str, dictionary_value_str
        )
        assert isinstance(set_response, CacheDictionarySetField.Success)

        remove_response = await dictionary_remover(
            cache_name=cache_name, dictionary_name=dictionary_name, field=dictionary_field_str
        )
        assert not isinstance(remove_response, ErrorResponseMixin)

        fetch_response = await client_async.dictionary_fetch(cache_name, dictionary_name)
        assert isinstance(fetch_response, CacheDictionaryFetch.Miss)

    async def with_bytes_field_succeeds(
        dictionary_remover: TDictionaryRemover,
        client_async: SimpleCacheClientAsync,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
        dictionary_field_bytes: bytes,
        dictionary_value_str: str,
    ) -> None:
        set_response = await client_async.dictionary_set_field(
            cache_name, dictionary_name, dictionary_field_bytes, dictionary_value_str
        )
        assert isinstance(set_response, CacheDictionarySetField.Success)

        remove_response = await dictionary_remover(
            cache_name=cache_name, dictionary_name=dictionary_name, field=dictionary_field_bytes
        )
        assert not isinstance(remove_response, ErrorResponseMixin)

        fetch_response = await client_async.dictionary_fetch(cache_name, dictionary_name)
        assert isinstance(fetch_response, CacheDictionaryFetch.Miss)


class TDictionarySetter(Protocol):
    def __call__(
        self,
        client_async: SimpleCacheClientAsync,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
        field: TDictionaryField,
        value: TDictionaryValue,
        *,
        ttl: CollectionTtl,
    ) -> Awaitable[CacheResponse]:
        ...


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
                await dictionary_setter(client, cache_name, dictionary_name, field, value, ttl=ttl)

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
            await dictionary_setter(client_async, cache_name, dictionary_name, field, value, ttl=ttl)
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
        dictionary_field_str: str,
        dictionary_value_str: str,
    ) -> None:
        ttl_seconds = 1
        async with SimpleCacheClientAsync(configuration, credential_provider, timedelta(seconds=ttl_seconds)) as client:
            ttl = CollectionTtl.from_cache_ttl().with_no_refresh_ttl_on_updates()

            await dictionary_setter(
                client, cache_name, dictionary_name, dictionary_field_str, dictionary_value_str, ttl=ttl
            )

            sleep(ttl_seconds / 2)

            fetch_response = await client.dictionary_fetch(cache_name, dictionary_name)
            assert isinstance(fetch_response, CacheDictionaryFetch.Hit)
            assert fetch_response.value_dictionary_string_string == {dictionary_field_str: dictionary_value_str}

            sleep(ttl_seconds / 2)
            fetch_response = await client.dictionary_fetch(cache_name, dictionary_name)
            assert isinstance(fetch_response, CacheDictionaryFetch.Miss)

    async def with_bytes_it_succeeds(
        dictionary_setter: TDictionarySetter,
        client_async: SimpleCacheClientAsync,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
        dictionary_field_bytes: bytes,
        dictionary_value_bytes: TDictionaryValue,
    ) -> None:
        await dictionary_setter(
            client_async,
            cache_name,
            dictionary_name,
            dictionary_field_bytes,
            dictionary_value_bytes,
        )
        fetch_response = await client_async.dictionary_fetch(cache_name, dictionary_name)
        assert isinstance(fetch_response, CacheDictionaryFetch.Hit)
        assert fetch_response.value_dictionary_bytes_bytes == {dictionary_field_bytes: dictionary_value_bytes}

    async def with_strings_it_succeeds(
        dictionary_setter: TDictionarySetter,
        client_async: SimpleCacheClientAsync,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
        dictionary_field_str: str,
        dictionary_value_str: str,
    ) -> None:
        await dictionary_setter(client_async, cache_name, dictionary_name, dictionary_field_str, dictionary_value_str)
        fetch_response = await client_async.dictionary_fetch(cache_name, dictionary_name)
        assert isinstance(fetch_response, CacheDictionaryFetch.Hit)
        assert fetch_response.value_dictionary_string_string == {dictionary_field_str: dictionary_value_str}

    async def with_other_values_type_it_errors(
        dictionary_setter: TDictionarySetter,
        client_async: SimpleCacheClientAsync,
        dictionary_name: TDictionaryName,
    ) -> None:
        for field, value, bad_type in [
            (None, "value", "NoneType"),
            ("field", None, "NoneType"),
            (234, "value", "int"),
            ("field", 234, "int"),
        ]:
            cache_name = uuid_str()
            response = await dictionary_setter(
                client_async, cache_name, dictionary_name, field, value, ttl=CollectionTtl()  # type:ignore[arg-type]
            )
            assert isinstance(response, ErrorResponseMixin)
            assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
            assert (
                response.message
                == f"Invalid argument passed to Momento client: Could not decode bytes to UTF-8<class '{bad_type}'>"
            )


class TDictionaryValueValidator(Protocol):
    def __call__(self, value: TDictionaryValue) -> Awaitable[CacheResponse]:
        ...


def a_dictionary_value_validator() -> None:
    async def with_null_value_throws_exception(dictionary_value_validator: TDictionaryValueValidator) -> None:
        response = await dictionary_value_validator(value=None)  # type: ignore
        assert isinstance(response, ErrorResponseMixin)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR

    async def with_wrong_container_throws_exception(dictionary_value_validator: TDictionaryValueValidator) -> None:
        response = await dictionary_value_validator(value=Exception())  # type: ignore
        assert isinstance(response, ErrorResponseMixin)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR


@behaves_like(a_cache_name_validator)
@behaves_like(a_connection_validator)
@behaves_like(a_dictionary_getter)
@behaves_like(a_dictionary_name_validator)
@behaves_like(a_dictionary_field_validator)
def describe_dictionary_get_field() -> None:
    @fixture
    def cache_name_validator(
        client_async: SimpleCacheClientAsync, dictionary_name: TDictionaryName, dictionary_field_str: str
    ) -> TCacheNameValidator:
        return partial(client_async.dictionary_get_field, dictionary_name=dictionary_name, field=dictionary_field_str)

    @fixture
    def connection_validator(
        cache_name: TCacheName, dictionary_name: TDictionaryName, dictionary_field_str: str
    ) -> TConnectionValidator:
        async def _connection_validator(client_async: SimpleCacheClientAsync) -> CacheResponse:
            return await client_async.dictionary_get_field(
                cache_name, dictionary_name=dictionary_name, field=dictionary_field_str
            )

        return _connection_validator

    @fixture
    def dictionary_field_validator(
        client_async: SimpleCacheClientAsync,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
    ) -> TDictionaryFieldValidator:
        return partial(client_async.dictionary_get_field, cache_name=cache_name, dictionary_name=dictionary_name)

    @fixture
    def dictionary_name_validator(
        client_async: SimpleCacheClientAsync,
        cache_name: TCacheName,
        dictionary_field_str: str,
    ) -> TDictionaryNameValidator:
        return partial(client_async.dictionary_get_field, cache_name=cache_name, field=dictionary_field_str)

    @fixture
    def dictionary_getter(client_async: SimpleCacheClientAsync) -> TDictionaryGetter:
        return partial(client_async.dictionary_get_field)

    async def misses_when_the_dictionary_does_not_exist(
        client_async: SimpleCacheClientAsync,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
        dictionary_field_str: str,
    ) -> None:
        get_response = await client_async.dictionary_get_field(cache_name, dictionary_name, dictionary_field_str)
        assert isinstance(get_response, CacheDictionaryGetField.Miss)


@behaves_like(a_cache_name_validator)
@behaves_like(a_connection_validator)
@behaves_like(a_dictionary_getter)
@behaves_like(a_dictionary_name_validator)
@behaves_like(a_dictionary_fields_validator)
def describe_dictionary_get_fields() -> None:
    @fixture
    def cache_name_validator(
        client_async: SimpleCacheClientAsync, dictionary_name: TDictionaryName, dictionary_fields: TDictionaryFields
    ) -> TCacheNameValidator:
        return partial(client_async.dictionary_get_fields, dictionary_name=dictionary_name, fields=dictionary_fields)

    @fixture
    def connection_validator(
        cache_name: TCacheName, dictionary_name: TDictionaryName, dictionary_fields: TDictionaryFields
    ) -> TConnectionValidator:
        async def _connection_validator(client_async: SimpleCacheClientAsync) -> CacheResponse:
            return await client_async.dictionary_get_fields(cache_name, dictionary_name, dictionary_fields)

        return _connection_validator

    @fixture
    def dictionary_fields_validator(
        client_async: SimpleCacheClientAsync,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
    ) -> TDictionaryFieldsValidator:
        return partial(client_async.dictionary_get_fields, cache_name=cache_name, dictionary_name=dictionary_name)

    @fixture
    def dictionary_name_validator(
        client_async: SimpleCacheClientAsync,
        cache_name: TCacheName,
        dictionary_field_str: str,
    ) -> TDictionaryNameValidator:
        return partial(client_async.dictionary_get_fields, cache_name=cache_name, fields=dictionary_field_str)

    @fixture
    def dictionary_getter(client_async: SimpleCacheClientAsync) -> TDictionaryGetter:
        async def _dictionary_getter(
            cache_name: TCacheName, dictionary_name: TDictionaryName, field: TDictionaryField
        ) -> CacheDictionaryGetFieldResponse:
            response = await client_async.dictionary_get_fields(cache_name, dictionary_name, [field])
            if isinstance(response, CacheDictionaryGetFields.Error):
                return CacheDictionaryGetField.Error(response.inner_exception)
            elif isinstance(response, CacheDictionaryGetFields.Miss):
                return CacheDictionaryGetField.Miss()
            assert isinstance(response, CacheDictionaryGetFields.Hit)
            return response.responses[0]

        return _dictionary_getter

    async def it_misses_when_the_dictionary_does_not_exist(
        client_async: SimpleCacheClientAsync,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
        dictionary_fields: TDictionaryFields,
    ) -> None:
        get_response = await client_async.dictionary_get_fields(cache_name, dictionary_name, dictionary_fields)
        assert isinstance(get_response, CacheDictionaryGetFields.Miss)

    async def it_decodes_responses_to_dictionaries_correctly(
        client_async: SimpleCacheClientAsync,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
    ) -> None:
        dictionary_items: dict[str, str] = dict([(uuid_str(), uuid_str()), (uuid_str(), uuid_str())])
        set_response = await client_async.dictionary_set_fields(cache_name, dictionary_name, dictionary_items)
        assert isinstance(set_response, CacheDictionarySetFields.Success)

        get_response = await client_async.dictionary_get_fields(cache_name, dictionary_name, dictionary_items.keys())
        assert isinstance(get_response, CacheDictionaryGetFields.Hit)
        assert len(get_response.responses) == 2
        for (key, value), response in zip(dictionary_items.items(), get_response.responses):
            assert isinstance(response, CacheDictionaryGetField.Hit)
            assert key == response.field_string and value == response.value_string

        assert get_response.value_dictionary_bytes_bytes == {
            k.encode(): v.encode() for k, v in dictionary_items.items()
        }
        assert get_response.value_dictionary_bytes_string == {k.encode(): v for k, v in dictionary_items.items()}
        assert get_response.value_dictionary_string_bytes == {k: v.encode() for k, v in dictionary_items.items()}
        assert get_response.value_dictionary_string_string == dictionary_items

    async def it_excludes_misses_from_dictionary(
        client_async: SimpleCacheClientAsync, cache_name: TCacheName, dictionary_name: TDictionaryName
    ) -> None:
        dictionary_items: dict[str, str] = dict([(uuid_str(), uuid_str()), (uuid_str(), uuid_str())])
        set_response = await client_async.dictionary_set_fields(cache_name, dictionary_name, dictionary_items)
        assert isinstance(set_response, CacheDictionarySetFields.Success)

        missing_key = uuid_str()
        get_response = await client_async.dictionary_get_fields(
            cache_name, dictionary_name, list(dictionary_items.keys()) + [missing_key]
        )
        assert isinstance(get_response, CacheDictionaryGetFields.Hit)
        assert len(get_response.responses) == 3
        for (key, value), response in zip(dictionary_items.items(), get_response.responses[:3]):
            assert isinstance(response, CacheDictionaryGetField.Hit)
            assert key == response.field_string and value == response.value_string
        assert isinstance(get_response.responses[-1], CacheDictionaryGetField.Miss)

        assert missing_key not in get_response.value_dictionary_string_string


@behaves_like(a_cache_name_validator)
@behaves_like(a_connection_validator)
@behaves_like(a_dictionary_name_validator)
def describe_dictionary_fetch() -> None:
    @fixture
    def cache_name_validator(
        client_async: SimpleCacheClientAsync, dictionary_name: TDictionaryName
    ) -> TCacheNameValidator:
        return partial(client_async.dictionary_fetch, dictionary_name=dictionary_name)

    @fixture
    def connection_validator(cache_name: TCacheName, dictionary_name: TDictionaryName) -> TConnectionValidator:
        async def _connection_validator(client_async: SimpleCacheClientAsync) -> CacheResponse:
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
@behaves_like(a_dictionary_name_validator)
@behaves_like(a_dictionary_field_validator)
def describe_dictionary_increment() -> None:
    @fixture
    def cache_name_validator(
        client_async: SimpleCacheClientAsync,
        dictionary_name: TDictionaryName,
        dictionary_field_str: str,
        increment_amount: int,
    ) -> TCacheNameValidator:
        return partial(
            client_async.dictionary_increment,
            dictionary_name=dictionary_name,
            field=dictionary_field_str,
            amount=increment_amount,
        )

    @fixture
    def connection_validator(
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
        dictionary_field_str: str,
        increment_amount: int,
    ) -> TConnectionValidator:
        async def _connection_validator(client_async: SimpleCacheClientAsync) -> CacheResponse:
            return await client_async.dictionary_increment(
                cache_name, dictionary_name=dictionary_name, field=dictionary_field_str, amount=increment_amount
            )

        return _connection_validator

    @fixture
    def dictionary_field_validator(
        client_async: SimpleCacheClientAsync,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
        increment_amount: int,
    ) -> TDictionaryFieldValidator:
        return partial(
            client_async.dictionary_increment,
            cache_name=cache_name,
            dictionary_name=dictionary_name,
            amount=increment_amount,
        )

    @fixture
    def dictionary_name_validator(
        client_async: SimpleCacheClientAsync,
        cache_name: TCacheName,
        dictionary_field_str: str,
        increment_amount: int,
    ) -> TDictionaryNameValidator:
        return partial(
            client_async.dictionary_increment,
            cache_name=cache_name,
            field=dictionary_field_str,
            amount=increment_amount,
        )

    async def it_starts_at_zero(
        client_async: SimpleCacheClientAsync,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
        dictionary_field_str: str,
    ) -> None:
        increment_response = await client_async.dictionary_increment(
            cache_name, dictionary_name, dictionary_field_str, 0
        )
        assert isinstance(increment_response, CacheDictionaryIncrement.Success)
        assert increment_response.value == 0

    async def it_increments_when_the_field_does_not_exist(
        client_async: SimpleCacheClientAsync,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
        dictionary_field_str: str,
        increment_amount: int,
    ) -> None:
        increment_response = await client_async.dictionary_increment(
            cache_name, dictionary_name, dictionary_field_str, increment_amount
        )
        assert isinstance(increment_response, CacheDictionaryIncrement.Success)
        assert increment_response.value == increment_amount

    async def it_increments_an_existing_field(
        client_async: SimpleCacheClientAsync,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
        dictionary_field_str: str,
        increment_amount: int,
    ) -> None:
        set_response = await client_async.dictionary_set_field(
            cache_name, dictionary_name, dictionary_field_str, str(increment_amount)
        )
        assert isinstance(set_response, CacheDictionarySetField.Success)

        increment_response = await client_async.dictionary_increment(
            cache_name, dictionary_name, dictionary_field_str, increment_amount
        )
        assert isinstance(increment_response, CacheDictionaryIncrement.Success)
        assert increment_response.value == 2 * increment_amount

    async def it_errors_on_bad_initial_values(
        client_async: SimpleCacheClientAsync,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
        dictionary_field_str: str,
        increment_amount: int,
    ) -> None:
        set_response = await client_async.dictionary_set_field(
            cache_name, dictionary_name, dictionary_field_str, "hello, world!"
        )
        assert isinstance(set_response, CacheDictionarySetField.Success)

        increment_response = await client_async.dictionary_increment(
            cache_name, dictionary_name, dictionary_field_str, increment_amount
        )
        assert isinstance(increment_response, CacheDictionaryIncrement.Error)
        assert increment_response.error_code == MomentoErrorCode.FAILED_PRECONDITION_ERROR


@behaves_like(a_cache_name_validator)
@behaves_like(a_connection_validator)
@behaves_like(a_dictionary_remover)
@behaves_like(a_dictionary_name_validator)
@behaves_like(a_dictionary_field_validator)
def describe_dictionary_remove_field() -> None:
    @fixture
    def cache_name_validator(
        client_async: SimpleCacheClientAsync, dictionary_name: TDictionaryName, dictionary_field_str: str
    ) -> TCacheNameValidator:
        return partial(
            client_async.dictionary_remove_field, dictionary_name=dictionary_name, field=dictionary_field_str
        )

    @fixture
    def connection_validator(
        cache_name: TCacheName, dictionary_name: TDictionaryName, dictionary_field_str: str
    ) -> TConnectionValidator:
        async def _connection_validator(client_async: SimpleCacheClientAsync) -> CacheResponse:
            return await client_async.dictionary_remove_field(
                cache_name, dictionary_name=dictionary_name, field=dictionary_field_str
            )

        return _connection_validator

    @fixture
    def dictionary_field_validator(
        client_async: SimpleCacheClientAsync,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
    ) -> TDictionaryFieldValidator:
        return partial(client_async.dictionary_remove_field, cache_name=cache_name, dictionary_name=dictionary_name)

    @fixture
    def dictionary_name_validator(
        client_async: SimpleCacheClientAsync,
        cache_name: TCacheName,
        dictionary_field_str: str,
    ) -> TDictionaryNameValidator:
        return partial(client_async.dictionary_remove_field, cache_name=cache_name, field=dictionary_field_str)

    @fixture
    def dictionary_remover(client_async: SimpleCacheClientAsync) -> TDictionaryRemover:
        return partial(client_async.dictionary_remove_field)

    async def it_removes_a_field(
        client_async: SimpleCacheClientAsync,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
        dictionary_field_str: str,
        dictionary_field_bytes: bytes,
        dictionary_value_str: str,
    ) -> None:
        extra_field, extra_value = uuid_str(), uuid_str()
        set_response = await client_async.dictionary_set_field(cache_name, dictionary_name, extra_field, extra_value)
        assert isinstance(set_response, CacheDictionarySetField.Success)

        for field in [dictionary_field_str, dictionary_field_bytes]:
            set_response = await client_async.dictionary_set_field(
                cache_name, dictionary_name, field, dictionary_value_str
            )
            assert isinstance(set_response, CacheDictionarySetField.Success)

            remove_response = await client_async.dictionary_remove_field(cache_name, dictionary_name, field)
            assert isinstance(remove_response, CacheDictionaryRemoveField.Success)

            fetch_response = await client_async.dictionary_fetch(cache_name, dictionary_name)
            assert isinstance(fetch_response, CacheDictionaryFetch.Hit)
            assert fetch_response.value_dictionary_string_string == {extra_field: extra_value}


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
        cache_name: TCacheName, dictionary_name: TDictionaryName, dictionary_fields: TDictionaryFields
    ) -> TConnectionValidator:
        async def _connection_validator(client_async: SimpleCacheClientAsync) -> CacheResponse:
            return await client_async.dictionary_remove_fields(
                cache_name, dictionary_name=dictionary_name, fields=dictionary_fields
            )

        return _connection_validator

    @fixture
    def dictionary_fields_validator(
        client_async: SimpleCacheClientAsync,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
    ) -> TDictionaryFieldsValidator:
        return partial(client_async.dictionary_remove_fields, cache_name=cache_name, dictionary_name=dictionary_name)

    @fixture
    def dictionary_name_validator(
        client_async: SimpleCacheClientAsync,
        cache_name: TCacheName,
        dictionary_fields: TDictionaryFields,
    ) -> TDictionaryNameValidator:
        return partial(client_async.dictionary_remove_fields, cache_name=cache_name, fields=dictionary_fields)

    @fixture
    def dictionary_remover(client_async: SimpleCacheClientAsync) -> TDictionaryRemover:
        async def _dictionary_remover(
            cache_name: TCacheName, dictionary_name: TDictionaryName, field: TDictionaryField
        ) -> CacheResponse:
            return await client_async.dictionary_remove_fields(
                cache_name=cache_name, dictionary_name=dictionary_name, fields=[field]
            )

        return _dictionary_remover

    async def it_removes_multiple_items(
        client_async: SimpleCacheClientAsync,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
        dictionary_items: TDictionaryItems,
    ) -> None:
        set_response = await client_async.dictionary_set_fields(cache_name, dictionary_name, dictionary_items)
        assert isinstance(set_response, CacheDictionarySetFields.Success)

        extra_field, extra_value = uuid_str(), uuid_str()
        unary_set_response = await client_async.dictionary_set_field(
            cache_name, dictionary_name, extra_field, extra_value
        )
        assert isinstance(unary_set_response, CacheDictionarySetField.Success)

        fields = [field for field, _ in dictionary_items.items()]
        remove_response = await client_async.dictionary_remove_fields(cache_name, dictionary_name, fields)
        assert isinstance(remove_response, CacheDictionaryRemoveFields.Success)

        fetch_response = await client_async.dictionary_fetch(cache_name, dictionary_name)
        assert isinstance(fetch_response, CacheDictionaryFetch.Hit)
        assert fetch_response.value_dictionary_string_string == {extra_field: extra_value}


@behaves_like(a_cache_name_validator)
@behaves_like(a_connection_validator)
@behaves_like(a_dictionary_setter)
@behaves_like(a_dictionary_name_validator)
@behaves_like(a_dictionary_value_validator)
def describe_dictionary_set_field() -> None:
    @fixture
    def cache_name_validator(
        client_async: SimpleCacheClientAsync,
        dictionary_name: TDictionaryName,
        dictionary_field_str: str,
        dictionary_value_str: str,
    ) -> TCacheNameValidator:
        return partial(
            client_async.dictionary_set_field,
            dictionary_name=dictionary_name,
            field=dictionary_field_str,
            value=dictionary_value_str,
        )

    @fixture
    def connection_validator(
        cache_name: TCacheName, dictionary_name: TDictionaryName, dictionary_field_str: str, dictionary_value_str: str
    ) -> TConnectionValidator:
        async def _connection_validator(client_async: SimpleCacheClientAsync) -> CacheResponse:
            return await client_async.dictionary_set_field(
                cache_name,
                dictionary_name=dictionary_name,
                field=dictionary_field_str,
                value=dictionary_value_str,
            )

        return _connection_validator

    @fixture
    def dictionary_name_validator(
        client_async: SimpleCacheClientAsync,
        cache_name: TCacheName,
        dictionary_field_str: str,
        dictionary_value_str: str,
    ) -> TDictionaryNameValidator:
        return partial(
            client_async.dictionary_set_field,
            cache_name=cache_name,
            field=dictionary_field_str,
            value=dictionary_value_str,
        )

    @fixture
    def dictionary_setter() -> TDictionarySetter:
        async def _dictionary_setter(
            client_async: SimpleCacheClientAsync,
            cache_name: TCacheName,
            dictionary_name: TDictionaryName,
            field: TDictionaryField,
            value: TDictionaryValue,
            *,
            ttl: CollectionTtl = CollectionTtl.from_cache_ttl(),
        ) -> CacheResponse:
            return await client_async.dictionary_set_field(
                cache_name=cache_name,
                dictionary_name=dictionary_name,
                field=field,
                value=value,
                ttl=ttl,
            )

        return _dictionary_setter

    @fixture
    def dictionary_value_validator(
        client_async: SimpleCacheClientAsync,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
        dictionary_field_str: str,
    ) -> TDictionaryValueValidator:
        return partial(
            client_async.dictionary_set_field,
            cache_name=cache_name,
            dictionary_name=dictionary_name,
            field=dictionary_field_str,
        )


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
        cache_name: TCacheName, dictionary_name: TDictionaryName, dictionary_items: TDictionaryItems
    ) -> TConnectionValidator:
        async def _connection_validator(client_async: SimpleCacheClientAsync) -> CacheResponse:
            return await client_async.dictionary_set_fields(
                cache_name, dictionary_name=dictionary_name, items=dictionary_items
            )

        return _connection_validator

    @fixture
    def dictionary_items_validator(
        client_async: SimpleCacheClientAsync,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
    ) -> TDictionaryItemsValidator:
        return partial(
            client_async.dictionary_set_fields,
            cache_name=cache_name,
            dictionary_name=dictionary_name,
        )

    @fixture
    def dictionary_name_validator(
        client_async: SimpleCacheClientAsync,
        cache_name: TCacheName,
        dictionary_items: TDictionaryItems,
    ) -> TDictionaryNameValidator:
        return partial(client_async.dictionary_set_fields, cache_name=cache_name, items=dictionary_items)

    @fixture
    def dictionary_setter() -> TDictionarySetter:
        async def _dictionary_setter(
            client_async: SimpleCacheClientAsync,
            cache_name: TCacheName,
            dictionary_name: TDictionaryName,
            field: TDictionaryField,
            value: TDictionaryValue,
            *,
            ttl: CollectionTtl = CollectionTtl.from_cache_ttl(),
        ) -> CacheResponse:
            return await client_async.dictionary_set_fields(
                cache_name=cache_name,
                dictionary_name=dictionary_name,
                items={field: value},
                ttl=ttl,
            )

        return _dictionary_setter

    @fixture
    def dictionary_value_validator(
        client_async: SimpleCacheClientAsync,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
        field: TDictionaryField,
    ) -> TDictionaryValueValidator:
        async def _value_validator(value: TDictionaryValue) -> CacheResponse:
            return await client_async.dictionary_set_fields(
                cache_name=cache_name,
                dictionary_name=dictionary_name,
                items={field: value},
            )

        return _value_validator

    async def it_sets_multiple_items(
        client_async: SimpleCacheClientAsync,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
        dictionary_items: TDictionaryItems,
    ) -> None:
        set_response = await client_async.dictionary_set_fields(cache_name, dictionary_name, dictionary_items)
        assert isinstance(set_response, CacheDictionarySetFields.Success)

        fetch_response = await client_async.dictionary_fetch(cache_name, dictionary_name)
        assert isinstance(fetch_response, CacheDictionaryFetch.Hit)
        assert fetch_response.value_dictionary_bytes_bytes == dict(_gen_dictionary_items_as_bytes(dictionary_items))

from datetime import timedelta
from functools import partial
from time import sleep
from typing import Callable

from pytest import fixture
from pytest_describe import behaves_like

from momento import SimpleCacheClient
from momento.auth import CredentialProvider
from momento.config import Configuration
from momento.errors import MomentoErrorCode
from momento.internal._utilities import _dictionary_items_as_bytes
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
    TDictionaryStrStr,
    TDictionaryValue,
)
from tests.utils import uuid_str

from .shared_behaviors import (
    TCacheNameValidator,
    TConnectionValidator,
    a_cache_name_validator,
    a_connection_validator,
)

TDictionaryFieldValidator = Callable[[TDictionaryField], CacheResponse]


def a_dictionary_field_validator() -> None:
    def with_null_field_throws_exception(dictionary_field_validator: TDictionaryFieldValidator) -> None:
        response = dictionary_field_validator(field=None)  # type: ignore
        assert isinstance(response, ErrorResponseMixin)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR

    def with_wrong_container_throws_exception(dictionary_field_validator: TDictionaryFieldValidator) -> None:
        response = dictionary_field_validator(field=Exception())  # type: ignore
        assert isinstance(response, ErrorResponseMixin)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR


TDictionaryFieldsValidator = Callable[[TDictionaryFields], CacheResponse]


def a_dictionary_fields_validator() -> None:
    def with_null_fields_throws_exception(dictionary_fields_validator: TDictionaryFieldsValidator) -> None:
        response = dictionary_fields_validator(fields=None)  # type: ignore
        assert isinstance(response, ErrorResponseMixin)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR

    def with_wrong_container_throws_exception(dictionary_fields_validator: TDictionaryFieldsValidator) -> None:
        response = dictionary_fields_validator(fields=Exception())  # type: ignore
        assert isinstance(response, ErrorResponseMixin)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR


TDictionaryItemsValidator = Callable[[TDictionaryItems], CacheResponse]


def a_dictionary_items_validator() -> None:
    def with_null_items_throws_exception(dictionary_items_validator: TDictionaryItemsValidator) -> None:
        response = dictionary_items_validator(items=None)  # type: ignore
        assert isinstance(response, ErrorResponseMixin)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR

    def with_wrong_container_throws_exception(dictionary_items_validator: TDictionaryItemsValidator) -> None:
        response = dictionary_items_validator(items=[1, 2, 3])  # type: ignore
        assert isinstance(response, ErrorResponseMixin)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR


TDictionaryNameValidator = Callable[[TDictionaryName], CacheResponse]


def a_dictionary_name_validator() -> None:
    def with_null_dictionary_name_it_returns_invalid(dictionary_name_validator: TDictionaryNameValidator) -> None:
        response = dictionary_name_validator(dictionary_name=None)  # type: ignore
        assert isinstance(response, ErrorResponseMixin)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
        assert response.inner_exception.message == "Dictionary name must be a string"

    def with_empty_dictionary_name_it_returns_invalid(
        dictionary_name_validator: TDictionaryNameValidator,
    ) -> None:
        response = dictionary_name_validator(dictionary_name="")
        assert isinstance(response, ErrorResponseMixin)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
        assert response.inner_exception.message == "Dictionary name must not be empty"

    def with_bad_dictionary_name_it_returns_invalid(dictionary_name_validator: TDictionaryNameValidator) -> None:
        response = dictionary_name_validator(dictionary_name=1)  # type: ignore
        assert isinstance(response, ErrorResponseMixin)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
        assert response.inner_exception.message == "Dictionary name must be a string"


TDictionaryGetter = Callable[
    [TDictionaryName, TDictionaryField],
    CacheDictionaryGetFieldResponse,
]


def a_dictionary_getter() -> None:
    def with_bytes_it_succeeds(
        dictionary_getter: TDictionaryGetter,
        client: SimpleCacheClient,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
        dictionary_field_bytes: TDictionaryField,
        dictionary_value_bytes: TDictionaryValue,
    ) -> None:
        set_response = client.dictionary_set_field(
            cache_name, dictionary_name, dictionary_field_bytes, dictionary_value_bytes
        )
        assert isinstance(set_response, CacheDictionarySetField.Success)

        get_response = dictionary_getter(dictionary_name=dictionary_name, field=dictionary_field_bytes)  # type: ignore
        assert isinstance(get_response, CacheDictionaryGetField.Hit)
        assert get_response.field_bytes == dictionary_field_bytes
        assert get_response.value_bytes == dictionary_value_bytes

    def with_string_it_succeeds(
        dictionary_getter: TDictionaryGetter,
        client: SimpleCacheClient,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
        dictionary_field: TDictionaryField,
        dictionary_value: TDictionaryValue,
    ) -> None:
        set_response = client.dictionary_set_field(cache_name, dictionary_name, dictionary_field, dictionary_value)
        assert isinstance(set_response, CacheDictionarySetField.Success)

        get_response = dictionary_getter(dictionary_name=dictionary_name, field=dictionary_field)  # type: ignore
        assert isinstance(get_response, CacheDictionaryGetField.Hit)
        assert get_response.field_string == dictionary_field
        assert get_response.value_string == dictionary_value


TDictionaryRemover = Callable[[SimpleCacheClient, TDictionaryName, TDictionaryField], CacheResponse]


def a_dictionary_remover() -> None:
    def with_string_field_succeeds(
        client: SimpleCacheClient,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
        dictionary_field: TDictionaryField,
        dictionary_value: TDictionaryValue,
        dictionary_remover: TDictionaryRemover,
    ) -> None:
        set_response = client.dictionary_set_field(cache_name, dictionary_name, dictionary_field, dictionary_value)
        assert isinstance(set_response, CacheDictionarySetField.Success)

        remove_response = dictionary_remover(client, dictionary_name, dictionary_field)
        assert not isinstance(remove_response, ErrorResponseMixin)

        fetch_response = client.dictionary_fetch(cache_name, dictionary_name)
        assert isinstance(fetch_response, CacheDictionaryFetch.Miss)

    def with_bytes_field_succeeds(
        client: SimpleCacheClient,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
        dictionary_field_bytes: TDictionaryField,
        dictionary_value: TDictionaryValue,
        dictionary_remover: TDictionaryRemover,
    ) -> None:
        set_response = client.dictionary_set_field(
            cache_name, dictionary_name, dictionary_field_bytes, dictionary_value
        )
        assert isinstance(set_response, CacheDictionarySetField.Success)

        remove_response = dictionary_remover(client, dictionary_name, dictionary_field_bytes)
        assert not isinstance(remove_response, ErrorResponseMixin)

        fetch_response = client.dictionary_fetch(cache_name, dictionary_name)
        assert isinstance(fetch_response, CacheDictionaryFetch.Miss)


TDictionarySetter = Callable[
    [SimpleCacheClient, TDictionaryName, TDictionaryField, TDictionaryValue, CollectionTtl],
    CacheResponse,
]


def a_dictionary_setter() -> None:
    def it_sets_the_ttl(
        configuration: Configuration,
        credential_provider: CredentialProvider,
        dictionary_setter: TDictionarySetter,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
        dictionary_items: TDictionaryItems,
    ) -> None:
        with SimpleCacheClient(configuration, credential_provider, timedelta(hours=1)) as client:
            ttl_seconds = 0.5
            ttl = CollectionTtl(ttl=timedelta(seconds=ttl_seconds), refresh_ttl=False)

            for field, value in dictionary_items.items():
                dictionary_setter(client, dictionary_name, field, value, ttl)

            sleep(ttl_seconds * 2)

            fetch_response = client.dictionary_fetch(cache_name, dictionary_name)
            assert isinstance(fetch_response, CacheDictionaryFetch.Miss)

    def it_refreshes_the_ttl(
        client: SimpleCacheClient,
        dictionary_setter: TDictionarySetter,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
    ) -> None:
        ttl_seconds = 1
        ttl = CollectionTtl.of(timedelta(seconds=ttl_seconds))
        items = dict([("one", "1"), ("two", "2"), ("three", "3"), ("four", "4")])

        for field, value in items.items():
            dictionary_setter(client, dictionary_name, field, value, ttl)
            sleep(ttl_seconds / 2)

        fetch_response = client.dictionary_fetch(cache_name, dictionary_name)
        assert isinstance(fetch_response, CacheDictionaryFetch.Hit)
        assert fetch_response.value_dictionary_string_string == items

    def it_uses_the_default_ttl_when_the_collection_ttl_has_no_ttl(
        configuration: Configuration,
        credential_provider: CredentialProvider,
        dictionary_setter: TDictionarySetter,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
        dictionary_field: TDictionaryField,
        dictionary_value: TDictionaryValue,
    ) -> None:
        ttl_seconds = 1
        with SimpleCacheClient(configuration, credential_provider, timedelta(seconds=ttl_seconds)) as client:
            ttl = CollectionTtl.from_cache_ttl().with_no_refresh_ttl_on_updates()

            dictionary_setter(client, dictionary_name, dictionary_field, dictionary_value, ttl)

            sleep(ttl_seconds / 2)

            fetch_response = client.dictionary_fetch(cache_name, dictionary_name)
            assert isinstance(fetch_response, CacheDictionaryFetch.Hit)
            assert fetch_response.value_dictionary_string_string == {dictionary_field: dictionary_value}

            sleep(ttl_seconds / 2)
            fetch_response = client.dictionary_fetch(cache_name, dictionary_name)
            assert isinstance(fetch_response, CacheDictionaryFetch.Miss)

    def with_bytes_it_succeeds(
        dictionary_setter: TDictionarySetter,
        client: SimpleCacheClient,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
        dictionary_field_bytes: TDictionaryField,
        dictionary_value_bytes: TDictionaryValue,
    ) -> None:
        dictionary_setter(client, dictionary_name, dictionary_field_bytes, dictionary_value_bytes, CollectionTtl())
        fetch_response = client.dictionary_fetch(cache_name, dictionary_name)
        assert isinstance(fetch_response, CacheDictionaryFetch.Hit)
        assert fetch_response.value_dictionary_bytes_bytes == {dictionary_field_bytes: dictionary_value_bytes}

    def with_strings_it_succeeds(
        dictionary_setter: TDictionarySetter,
        client: SimpleCacheClient,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
        dictionary_field: TDictionaryField,
        dictionary_value: TDictionaryValue,
    ) -> None:
        dictionary_setter(client, dictionary_name, dictionary_field, dictionary_value, CollectionTtl())
        fetch_response = client.dictionary_fetch(cache_name, dictionary_name)
        assert isinstance(fetch_response, CacheDictionaryFetch.Hit)
        assert fetch_response.value_dictionary_string_string == {dictionary_field: dictionary_value}

    def with_other_values_type_it_errors(
        dictionary_setter: TDictionarySetter,
        client: SimpleCacheClient,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
    ) -> None:
        for field, value, bad_type in [
            (None, "value", "NoneType"),
            ("field", None, "NoneType"),
            (234, "value", "int"),
            ("field", 234, "int"),
        ]:
            response = dictionary_setter(client, dictionary_name, field, value, CollectionTtl())  # type: ignore
            assert isinstance(response, ErrorResponseMixin)
            assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
            assert (
                response.message
                == f"Invalid argument passed to Momento client: Could not decode bytes to UTF-8<class '{bad_type}'>"
            )


TDictionaryValueValidator = Callable[[TDictionaryValue], CacheResponse]


def a_dictionary_value_validator() -> None:
    def with_null_value_throws_exception(dictionary_value_validator: TDictionaryValueValidator) -> None:
        response = dictionary_value_validator(value=None)  # type: ignore
        assert isinstance(response, ErrorResponseMixin)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR

    def with_wrong_container_throws_exception(dictionary_value_validator: TDictionaryValueValidator) -> None:
        response = dictionary_value_validator(value=Exception())  # type: ignore
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
        client: SimpleCacheClient, dictionary_name: TDictionaryName, dictionary_field: TDictionaryField
    ) -> TCacheNameValidator:
        return partial(client.dictionary_get_field, dictionary_name=dictionary_name, field=dictionary_field)

    @fixture
    def connection_validator(
        dictionary_name: TDictionaryName, dictionary_field: TDictionaryField
    ) -> TConnectionValidator:
        def _connection_validator(client: SimpleCacheClient, cache_name: TCacheName) -> CacheResponse:
            return client.dictionary_get_field(cache_name, dictionary_name=dictionary_name, field=dictionary_field)

        return _connection_validator

    @fixture
    def dictionary_field_validator(
        client: SimpleCacheClient,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
    ) -> TDictionaryNameValidator:
        return partial(client.dictionary_get_field, cache_name=cache_name, dictionary_name=dictionary_name)

    @fixture
    def dictionary_name_validator(
        client: SimpleCacheClient,
        cache_name: TCacheName,
        dictionary_field: TDictionaryField,
    ) -> TDictionaryNameValidator:
        return partial(client.dictionary_get_field, cache_name=cache_name, field=dictionary_field)

    @fixture
    def dictionary_getter(client: SimpleCacheClient, cache_name: TCacheName) -> TDictionaryGetter:
        return partial(client.dictionary_get_field, cache_name=cache_name)

    def misses_when_the_dictionary_does_not_exist(
        client: SimpleCacheClient,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
        dictionary_field: TDictionaryField,
    ) -> None:
        get_response = client.dictionary_get_field(cache_name, dictionary_name, dictionary_field)
        assert isinstance(get_response, CacheDictionaryGetField.Miss)


@behaves_like(a_cache_name_validator)
@behaves_like(a_connection_validator)
@behaves_like(a_dictionary_getter)
@behaves_like(a_dictionary_name_validator)
@behaves_like(a_dictionary_fields_validator)
def describe_dictionary_get_fields() -> None:
    @fixture
    def cache_name_validator(
        client: SimpleCacheClient, dictionary_name: TDictionaryName, dictionary_fields: TDictionaryFields
    ) -> TCacheNameValidator:
        return partial(client.dictionary_get_fields, dictionary_name=dictionary_name, fields=dictionary_fields)

    @fixture
    def connection_validator(
        dictionary_name: TDictionaryName, dictionary_fields: TDictionaryFields
    ) -> TConnectionValidator:
        def _connection_validator(client: SimpleCacheClient, cache_name: TCacheName) -> CacheResponse:
            return client.dictionary_get_fields(cache_name, dictionary_name, dictionary_fields)

        return _connection_validator

    @fixture
    def dictionary_fields_validator(
        client: SimpleCacheClient,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
    ) -> TDictionaryNameValidator:
        return partial(client.dictionary_get_fields, cache_name=cache_name, dictionary_name=dictionary_name)

    @fixture
    def dictionary_name_validator(
        client: SimpleCacheClient,
        cache_name: TCacheName,
        dictionary_field: TDictionaryField,
    ) -> TDictionaryNameValidator:
        return partial(client.dictionary_get_fields, cache_name=cache_name, fields=dictionary_field)

    @fixture
    def dictionary_getter(client: SimpleCacheClient, cache_name: TCacheName) -> TDictionaryGetter:
        def _dictionary_getter(
            dictionary_name: TDictionaryName, field: TDictionaryField
        ) -> CacheDictionaryGetFieldResponse:
            response = client.dictionary_get_fields(cache_name, dictionary_name, [field])
            if isinstance(response, CacheDictionaryGetFields.Error):
                return CacheDictionaryGetField.Error(response.inner_exception)
            elif isinstance(response, CacheDictionaryGetFields.Miss):
                return CacheDictionaryGetField.Miss()
            assert isinstance(response, CacheDictionaryGetFields.Hit)
            return response.responses[0]

        return _dictionary_getter

    def it_misses_when_the_dictionary_does_not_exist(
        client: SimpleCacheClient,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
        dictionary_fields: TDictionaryFields,
    ) -> None:
        get_response = client.dictionary_get_fields(cache_name, dictionary_name, dictionary_fields)
        assert isinstance(get_response, CacheDictionaryGetFields.Miss)

    def it_decodes_responses_to_dictionaries_correctly(
        client: SimpleCacheClient,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
    ) -> None:
        dictionary_items: TDictionaryStrStr = dict([(uuid_str(), uuid_str()), (uuid_str(), uuid_str())])
        set_response = client.dictionary_set_fields(cache_name, dictionary_name, dictionary_items)
        assert isinstance(set_response, CacheDictionarySetFields.Success)

        get_response = client.dictionary_get_fields(cache_name, dictionary_name, dictionary_items.keys())
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

    def it_excludes_misses_from_dictionary(
        client: SimpleCacheClient, cache_name: TCacheName, dictionary_name: TDictionaryName
    ) -> None:
        dictionary_items: TDictionaryStrStr = dict([(uuid_str(), uuid_str()), (uuid_str(), uuid_str())])
        set_response = client.dictionary_set_fields(cache_name, dictionary_name, dictionary_items)
        assert isinstance(set_response, CacheDictionarySetFields.Success)

        missing_key = uuid_str()
        get_response = client.dictionary_get_fields(
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
    def cache_name_validator(client: SimpleCacheClient, dictionary_name: TDictionaryName) -> TCacheNameValidator:
        return partial(client.dictionary_fetch, dictionary_name=dictionary_name)

    @fixture
    def connection_validator(dictionary_name: TDictionaryName) -> TConnectionValidator:
        def _connection_validator(client: SimpleCacheClient, cache_name: TCacheName) -> CacheResponse:
            return client.dictionary_fetch(cache_name, dictionary_name=dictionary_name)

        return _connection_validator

    @fixture
    def dictionary_name_validator(client: SimpleCacheClient, cache_name: TCacheName) -> TDictionaryNameValidator:
        return partial(client.dictionary_fetch, cache_name=cache_name)

    def misses_when_the_dictionary_does_not_exist(
        client: SimpleCacheClient, cache_name: TCacheName, dictionary_name: TDictionaryName
    ) -> None:
        fetch_response = client.dictionary_fetch(cache_name, dictionary_name)
        assert isinstance(fetch_response, CacheDictionaryFetch.Miss)


@behaves_like(a_cache_name_validator)
@behaves_like(a_connection_validator)
@behaves_like(a_dictionary_name_validator)
@behaves_like(a_dictionary_field_validator)
def describe_dictionary_increment() -> None:
    @fixture
    def cache_name_validator(
        client: SimpleCacheClient,
        dictionary_name: TDictionaryName,
        dictionary_field: TDictionaryField,
        increment_amount: int,
    ) -> TCacheNameValidator:
        return partial(
            client.dictionary_increment,
            dictionary_name=dictionary_name,
            field=dictionary_field,
            amount=increment_amount,
            ttl=CollectionTtl(),
        )

    @fixture
    def connection_validator(
        dictionary_name: TDictionaryName,
        dictionary_field: TDictionaryField,
        increment_amount: int,
    ) -> TConnectionValidator:
        def _connection_validator(client: SimpleCacheClient, cache_name: TCacheName) -> CacheResponse:
            return client.dictionary_increment(
                cache_name,
                dictionary_name=dictionary_name,
                field=dictionary_field,
                amount=increment_amount,
                ttl=CollectionTtl(),
            )

        return _connection_validator

    @fixture
    def dictionary_field_validator(
        client: SimpleCacheClient,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
        increment_amount: int,
    ) -> TDictionaryNameValidator:
        return partial(
            client.dictionary_increment,
            cache_name=cache_name,
            dictionary_name=dictionary_name,
            amount=increment_amount,
            ttl=CollectionTtl(),
        )

    @fixture
    def dictionary_name_validator(
        client: SimpleCacheClient,
        cache_name: TCacheName,
        dictionary_field: TDictionaryField,
        increment_amount: int,
    ) -> TDictionaryNameValidator:
        return partial(
            client.dictionary_increment,
            cache_name=cache_name,
            field=dictionary_field,
            amount=increment_amount,
            ttl=CollectionTtl(),
        )

    def it_starts_at_zero(
        client: SimpleCacheClient,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
        dictionary_field: TDictionaryField,
    ) -> None:
        increment_response = client.dictionary_increment(cache_name, dictionary_name, dictionary_field, 0)
        assert isinstance(increment_response, CacheDictionaryIncrement.Success)
        assert increment_response.value == 0

    def it_increments_when_the_field_does_not_exist(
        client: SimpleCacheClient,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
        dictionary_field: TDictionaryField,
        increment_amount: int,
    ) -> None:
        increment_response = client.dictionary_increment(
            cache_name, dictionary_name, dictionary_field, increment_amount
        )
        assert isinstance(increment_response, CacheDictionaryIncrement.Success)
        assert increment_response.value == increment_amount

    def it_increments_an_existing_field(
        client: SimpleCacheClient,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
        dictionary_field: TDictionaryField,
        increment_amount: int,
    ) -> None:
        set_response = client.dictionary_set_field(cache_name, dictionary_name, dictionary_field, str(increment_amount))
        assert isinstance(set_response, CacheDictionarySetField.Success)

        increment_response = client.dictionary_increment(
            cache_name, dictionary_name, dictionary_field, increment_amount
        )
        assert isinstance(increment_response, CacheDictionaryIncrement.Success)
        assert increment_response.value == 2 * increment_amount

    def it_errors_on_bad_initial_values(
        client: SimpleCacheClient,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
        dictionary_field: TDictionaryField,
        increment_amount: int,
    ) -> None:
        set_response = client.dictionary_set_field(cache_name, dictionary_name, dictionary_field, "hello, world!")
        assert isinstance(set_response, CacheDictionarySetField.Success)

        increment_response = client.dictionary_increment(
            cache_name, dictionary_name, dictionary_field, increment_amount
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
        client: SimpleCacheClient, dictionary_name: TDictionaryName, dictionary_field: TDictionaryField
    ) -> TCacheNameValidator:
        return partial(client.dictionary_remove_field, dictionary_name=dictionary_name, field=dictionary_field)

    @fixture
    def connection_validator(
        dictionary_name: TDictionaryName, dictionary_field: TDictionaryField
    ) -> TConnectionValidator:
        def _connection_validator(client: SimpleCacheClient, cache_name: TCacheName) -> CacheResponse:
            return client.dictionary_remove_field(cache_name, dictionary_name=dictionary_name, field=dictionary_field)

        return _connection_validator

    @fixture
    def dictionary_field_validator(
        client: SimpleCacheClient,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
    ) -> TDictionaryNameValidator:
        return partial(client.dictionary_remove_field, cache_name=cache_name, dictionary_name=dictionary_name)

    @fixture
    def dictionary_name_validator(
        client: SimpleCacheClient,
        cache_name: TCacheName,
        dictionary_field: TDictionaryField,
    ) -> TDictionaryNameValidator:
        return partial(client.dictionary_remove_field, cache_name=cache_name, field=dictionary_field)

    @fixture
    def dictionary_remover(cache_name: TCacheName) -> TDictionaryRemover:
        def _dictionary_remover(
            client: SimpleCacheClient,
            dictionary_name: TDictionaryName,
            field: TDictionaryField,
        ) -> CacheResponse:
            return client.dictionary_remove_field(cache_name, dictionary_name, field)

        return _dictionary_remover

    def it_removes_a_field(
        client: SimpleCacheClient,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
        dictionary_field: TDictionaryField,
        dictionary_field_bytes: TDictionaryField,
        dictionary_value: TDictionaryValue,
    ) -> None:
        extra_field, extra_value = uuid_str(), uuid_str()
        set_response = client.dictionary_set_field(cache_name, dictionary_name, extra_field, extra_value)
        assert isinstance(set_response, CacheDictionarySetField.Success)

        for field in [dictionary_field, dictionary_field_bytes]:
            set_response = client.dictionary_set_field(cache_name, dictionary_name, field, dictionary_value)
            assert isinstance(set_response, CacheDictionarySetField.Success)

            remove_response = client.dictionary_remove_field(cache_name, dictionary_name, field)
            assert isinstance(remove_response, CacheDictionaryRemoveField.Success)

            fetch_response = client.dictionary_fetch(cache_name, dictionary_name)
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
        client: SimpleCacheClient,
        dictionary_name: TDictionaryName,
        dictionary_fields: TDictionaryFields,
    ) -> TCacheNameValidator:
        return partial(client.dictionary_remove_fields, dictionary_name=dictionary_name, fields=dictionary_fields)

    @fixture
    def connection_validator(
        dictionary_name: TDictionaryName, dictionary_fields: TDictionaryFields
    ) -> TConnectionValidator:
        def _connection_validator(client: SimpleCacheClient, cache_name: TCacheName) -> CacheResponse:
            return client.dictionary_remove_fields(
                cache_name, dictionary_name=dictionary_name, fields=dictionary_fields
            )

        return _connection_validator

    @fixture
    def dictionary_fields_validator(
        client: SimpleCacheClient,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
    ) -> TDictionaryNameValidator:
        return partial(client.dictionary_remove_fields, cache_name=cache_name, dictionary_name=dictionary_name)

    @fixture
    def dictionary_name_validator(
        client: SimpleCacheClient,
        cache_name: TCacheName,
        dictionary_fields: TDictionaryFields,
    ) -> TDictionaryNameValidator:
        return partial(client.dictionary_remove_fields, cache_name=cache_name, fields=dictionary_fields)

    @fixture
    def dictionary_remover(cache_name: TCacheName) -> TDictionaryRemover:
        def _dictionary_remover(
            client: SimpleCacheClient,
            dictionary_name: TDictionaryName,
            field: TDictionaryField,
        ) -> CacheResponse:
            return client.dictionary_remove_fields(cache_name, dictionary_name, [field])

        return _dictionary_remover

    def it_removes_multiple_items(
        client: SimpleCacheClient,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
        dictionary_items: TDictionaryItems,
    ) -> None:
        set_response = client.dictionary_set_fields(cache_name, dictionary_name, dictionary_items)
        assert isinstance(set_response, CacheDictionarySetFields.Success)

        extra_field, extra_value = uuid_str(), uuid_str()
        unary_set_response = client.dictionary_set_field(cache_name, dictionary_name, extra_field, extra_value)
        assert isinstance(unary_set_response, CacheDictionarySetField.Success)

        fields = [field for field, _ in dictionary_items.items()]
        remove_response = client.dictionary_remove_fields(cache_name, dictionary_name, fields)
        assert isinstance(remove_response, CacheDictionaryRemoveFields.Success)

        fetch_response = client.dictionary_fetch(cache_name, dictionary_name)
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
        client: SimpleCacheClient,
        dictionary_name: TDictionaryName,
        dictionary_field: TDictionaryField,
        dictionary_value: TDictionaryValue,
    ) -> TCacheNameValidator:
        return partial(
            client.dictionary_set_field,
            dictionary_name=dictionary_name,
            field=dictionary_field,
            value=dictionary_value,
        )

    @fixture
    def connection_validator(
        dictionary_name: TDictionaryName, dictionary_field: TDictionaryField, dictionary_value: TDictionaryValue
    ) -> TConnectionValidator:
        def _connection_validator(client: SimpleCacheClient, cache_name: TCacheName) -> CacheResponse:
            return client.dictionary_set_field(
                cache_name,
                dictionary_name=dictionary_name,
                field=dictionary_field,
                value=dictionary_value,
            )

        return _connection_validator

    @fixture
    def dictionary_name_validator(
        client: SimpleCacheClient,
        cache_name: TCacheName,
        dictionary_field: TDictionaryField,
        dictionary_value: TDictionaryValue,
    ) -> TDictionaryNameValidator:
        return partial(
            client.dictionary_set_field,
            cache_name=cache_name,
            field=dictionary_field,
            value=dictionary_value,
            ttl=CollectionTtl(),
        )

    @fixture
    def dictionary_setter(cache_name: TCacheName) -> TDictionarySetter:
        def _dictionary_setter(
            client: SimpleCacheClient,
            dictionary_name: TDictionaryName,
            field: TDictionaryField,
            value: TDictionaryValue,
            ttl: CollectionTtl,
        ) -> CacheResponse:
            return client.dictionary_set_field(cache_name, dictionary_name, field, value, ttl=ttl)

        return _dictionary_setter

    @fixture
    def dictionary_value_validator(
        client: SimpleCacheClient,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
        dictionary_field: TDictionaryField,
    ) -> TDictionaryValueValidator:
        return partial(
            client.dictionary_set_field,
            cache_name=cache_name,
            dictionary_name=dictionary_name,
            field=dictionary_field,
            ttl=CollectionTtl(),
        )


@behaves_like(a_cache_name_validator)
@behaves_like(a_connection_validator)
@behaves_like(a_dictionary_setter)
@behaves_like(a_dictionary_name_validator)
@behaves_like(a_dictionary_items_validator)
def describe_dictionary_set_fields() -> None:
    @fixture
    def cache_name_validator(
        client: SimpleCacheClient,
        dictionary_name: TDictionaryName,
        dictionary_items: TDictionaryItems,
    ) -> TCacheNameValidator:
        return partial(client.dictionary_set_fields, dictionary_name=dictionary_name, items=dictionary_items)

    @fixture
    def connection_validator(
        dictionary_name: TDictionaryName, dictionary_items: TDictionaryItems
    ) -> TConnectionValidator:
        def _connection_validator(client: SimpleCacheClient, cache_name: TCacheName) -> CacheResponse:
            return client.dictionary_set_fields(cache_name, dictionary_name=dictionary_name, items=dictionary_items)

        return _connection_validator

    @fixture
    def dictionary_items_validator(
        client: SimpleCacheClient,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
    ) -> TDictionaryNameValidator:
        return partial(
            client.dictionary_set_fields,
            cache_name=cache_name,
            dictionary_name=dictionary_name,
            ttl=CollectionTtl(),
        )

    @fixture
    def dictionary_name_validator(
        client: SimpleCacheClient,
        cache_name: TCacheName,
        dictionary_items: TDictionaryItems,
    ) -> TDictionaryNameValidator:
        return partial(client.dictionary_set_fields, cache_name=cache_name, items=dictionary_items, ttl=CollectionTtl())

    @fixture
    def dictionary_setter(cache_name: TCacheName) -> TDictionarySetter:
        def _dictionary_setter(
            client: SimpleCacheClient,
            dictionary_name: TDictionaryName,
            field: TDictionaryField,
            value: TDictionaryValue,
            ttl: CollectionTtl,
        ) -> CacheResponse:
            return client.dictionary_set_fields(cache_name, dictionary_name, {field: value}, ttl=ttl)

        return _dictionary_setter

    @fixture
    def dictionary_value_validator(
        client: SimpleCacheClient,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
        dictionary_field: TDictionaryField,
    ) -> TDictionaryNameValidator:
        def _value_validator(dictionary_value: TDictionaryValue) -> CacheResponse:
            return client.dictionary_set_fields(cache_name, dictionary_name, items={dictionary_field: dictionary_value})

        return _value_validator

    def it_sets_multiple_items(
        client: SimpleCacheClient,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
        dictionary_items: TDictionaryItems,
    ) -> None:
        set_response = client.dictionary_set_fields(cache_name, dictionary_name, dictionary_items, CollectionTtl())
        assert isinstance(set_response, CacheDictionarySetFields.Success)

        fetch_response = client.dictionary_fetch(cache_name, dictionary_name)
        assert isinstance(fetch_response, CacheDictionaryFetch.Hit)
        assert fetch_response.value_dictionary_bytes_bytes == _dictionary_items_as_bytes(dictionary_items)

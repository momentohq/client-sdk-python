from datetime import timedelta
from functools import partial
from time import sleep
from typing import Optional

from pytest import fixture
from pytest_describe import behaves_like
from typing_extensions import Protocol

from momento import CacheClient, CredentialProvider
from momento.config import Configuration
from momento.errors import MomentoErrorCode
from momento.requests import CollectionTtl, SortOrder
from momento.responses import (
    CacheResponse,
    CacheSortedSetFetch,
    CacheSortedSetFetchResponse,
    CacheSortedSetGetRank,
    CacheSortedSetGetRankResponse,
    CacheSortedSetGetScore,
    CacheSortedSetGetScoreResponse,
    CacheSortedSetGetScores,
    CacheSortedSetPutElement,
    CacheSortedSetPutElements,
)
from momento.responses.mixins import ErrorResponseMixin
from momento.typing import (
    TCacheName,
    TSortedSetElements,
    TSortedSetName,
    TSortedSetScore,
    TSortedSetValue,
    TSortedSetValues,
)
from tests.utils import uuid_str

from .shared_behaviors import (
    TCacheNameValidator,
    TConnectionValidator,
    a_cache_name_validator,
    a_connection_validator,
)


class TSortedSetNameValidator(Protocol):
    def __call__(self, sorted_set_name: TSortedSetName) -> CacheResponse:
        ...


def a_sorted_set_name_validator() -> None:
    def with_null_sorted_set_name_it_returns_invalid(sorted_set_name_validator: TSortedSetNameValidator) -> None:
        response = sorted_set_name_validator(sorted_set_name=None)  # type: ignore
        assert isinstance(response, ErrorResponseMixin)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
        assert response.inner_exception.message == "Sorted set name must be a string"

    def with_empty_sorted_set_name_it_returns_invalid(
        sorted_set_name_validator: TSortedSetNameValidator,
    ) -> None:
        response = sorted_set_name_validator(sorted_set_name="")
        assert isinstance(response, ErrorResponseMixin)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
        assert response.inner_exception.message == "Sorted set name must not be empty"

    def with_bad_sorted_set_name_it_returns_invalid(sorted_set_name_validator: TSortedSetNameValidator) -> None:
        response = sorted_set_name_validator(sorted_set_name=1)  # type: ignore
        assert isinstance(response, ErrorResponseMixin)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
        assert response.inner_exception.message == "Sorted set name must be a string"


class TSortedSetSetter(Protocol):
    def __call__(
        self,
        client: CacheClient,
        cache_name: TCacheName,
        sorted_set_name: TSortedSetName,
        elements: TSortedSetElements,
        *,
        ttl: CollectionTtl = CollectionTtl.from_cache_ttl(),
    ) -> CacheResponse:
        ...


def a_sorted_set_setter() -> None:
    def it_sets_the_ttl(
        configuration: Configuration,
        credential_provider: CredentialProvider,
        sorted_set_setter: TSortedSetSetter,
        cache_name: TCacheName,
        sorted_set_name: TSortedSetName,
        sorted_set_elements: TSortedSetElements,
    ) -> None:
        with CacheClient(configuration, credential_provider, timedelta(hours=1)) as client:
            ttl_seconds = 0.5
            ttl = CollectionTtl(ttl=timedelta(seconds=ttl_seconds), refresh_ttl=False)

            sorted_set_setter(client, cache_name, sorted_set_name, sorted_set_elements, ttl=ttl)
            sleep(ttl_seconds * 2)

            fetch_response = client.sorted_set_fetch_by_rank(cache_name, sorted_set_name)
            assert isinstance(fetch_response, CacheSortedSetFetch.Miss)

    def it_refreshes_the_ttl(
        client: CacheClient,
        sorted_set_setter: TSortedSetSetter,
        cache_name: TCacheName,
        sorted_set_name: TSortedSetName,
    ) -> None:
        ttl_seconds = 1
        ttl = CollectionTtl.of(timedelta(seconds=ttl_seconds))
        elements = dict([("one", 1.0), ("two", 2.0), ("three", 3.0), ("four", 4.0)])
        elements_list = [(value, score) for value, score in sorted(elements.items(), key=lambda item: item[1])]

        sorted_set_setter(client, cache_name, sorted_set_name, elements, ttl=ttl)

        fetch_response = client.sorted_set_fetch_by_rank(cache_name, sorted_set_name)
        assert isinstance(fetch_response, CacheSortedSetFetch.Hit)
        assert fetch_response.value_list_string == elements_list

    def it_uses_the_default_ttl_when_the_collection_ttl_has_no_ttl(
        configuration: Configuration,
        credential_provider: CredentialProvider,
        sorted_set_setter: TSortedSetSetter,
        cache_name: TCacheName,
        sorted_set_name: TSortedSetName,
        sorted_set_value_str: str,
        sorted_set_score: TSortedSetScore,
    ) -> None:
        ttl_seconds = 1
        with CacheClient(configuration, credential_provider, timedelta(seconds=ttl_seconds)) as client:
            ttl = CollectionTtl.from_cache_ttl().with_no_refresh_ttl_on_updates()

            sorted_set_setter(client, cache_name, sorted_set_name, {sorted_set_value_str: sorted_set_score}, ttl=ttl)

            sleep(ttl_seconds / 2)

            fetch_response = client.sorted_set_fetch_by_rank(cache_name, sorted_set_name)
            assert isinstance(fetch_response, CacheSortedSetFetch.Hit)
            assert fetch_response.value_list_string == [(sorted_set_value_str, sorted_set_score)]

            sleep(ttl_seconds / 2)
            fetch_response = client.sorted_set_fetch_by_rank(cache_name, sorted_set_name)
            assert isinstance(fetch_response, CacheSortedSetFetch.Miss)

    def with_bytes_it_succeeds(
        sorted_set_setter: TSortedSetSetter,
        client: CacheClient,
        cache_name: TCacheName,
        sorted_set_name: TSortedSetName,
        sorted_set_value_bytes: bytes,
        sorted_set_score: TSortedSetScore,
    ) -> None:
        sorted_set_setter(
            client,
            cache_name,
            sorted_set_name,
            {sorted_set_value_bytes: sorted_set_score},
        )
        fetch_response = client.sorted_set_fetch_by_rank(cache_name, sorted_set_name)
        assert isinstance(fetch_response, CacheSortedSetFetch.Hit)
        assert fetch_response.value_list_bytes == [(sorted_set_value_bytes, sorted_set_score)]

    def with_strings_it_succeeds(
        sorted_set_setter: TSortedSetSetter,
        client: CacheClient,
        cache_name: TCacheName,
        sorted_set_name: TSortedSetName,
        sorted_set_value_str: str,
        sorted_set_score: TSortedSetScore,
    ) -> None:
        sorted_set_setter(client, cache_name, sorted_set_name, {sorted_set_value_str: sorted_set_score})
        fetch_response = client.sorted_set_fetch_by_rank(cache_name, sorted_set_name)
        assert isinstance(fetch_response, CacheSortedSetFetch.Hit)
        assert fetch_response.value_list_string == [(sorted_set_value_str, sorted_set_score)]

    def with_other_values_type_it_errors(
        sorted_set_setter: TSortedSetSetter,
        client: CacheClient,
        sorted_set_name: TSortedSetName,
    ) -> None:
        for value, score, bad_type in [
            (None, 1.0, "NoneType"),
            (234, 1.0, "int"),
        ]:
            cache_name = uuid_str()
            response = sorted_set_setter(
                client, cache_name, sorted_set_name, {value: score}, ttl=CollectionTtl()  # type:ignore[arg-type]
            )
            assert isinstance(response, ErrorResponseMixin)
            assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
            assert (
                response.message
                == f"Invalid argument passed to Momento client: Could not convert the given type to bytes: "  # noqa: W503,E501
                f"<class '{bad_type}'>"
            )

    def with_other_scores_type_it_errors(
        sorted_set_setter: TSortedSetSetter,
        client: CacheClient,
        sorted_set_name: TSortedSetName,
    ) -> None:
        for value, score, bad_type in [
            ("field", None, "NoneType"),
            ("field", 234, "int"),
        ]:
            cache_name = uuid_str()
            response = sorted_set_setter(
                client, cache_name, sorted_set_name, {value: score}, ttl=CollectionTtl()  # type:ignore[arg-type]
            )
            assert isinstance(response, ErrorResponseMixin)
            assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
            assert (
                response.message
                == f"Invalid argument passed to Momento client: score must be a float. Given type: "  # noqa: W503,E501
                f"<class '{bad_type}'>"
            )


class TSortedSetScoreGetter(Protocol):
    def __call__(
        self, cache_name: TCacheName, sorted_set_name: TSortedSetName, value: TSortedSetValue
    ) -> CacheSortedSetGetScoreResponse:
        ...


def a_sorted_set_score_getter() -> None:
    def with_bytes_it_succeeds(
        sorted_set_score_getter: TSortedSetScoreGetter,
        client: CacheClient,
        cache_name: TCacheName,
        sorted_set_name: TSortedSetName,
        sorted_set_value_bytes: bytes,
        sorted_set_score: TSortedSetScore,
    ) -> None:
        set_response = client.sorted_set_put_element(
            cache_name, sorted_set_name, sorted_set_value_bytes, sorted_set_score
        )
        assert isinstance(set_response, CacheSortedSetPutElement.Success)

        get_response = sorted_set_score_getter(
            cache_name=cache_name, sorted_set_name=sorted_set_name, value=sorted_set_value_bytes
        )
        assert isinstance(get_response, CacheSortedSetGetScore.Hit)
        assert get_response.value_bytes == sorted_set_value_bytes
        assert get_response.score == sorted_set_score

    def with_strings_it_succeeds(
        sorted_set_score_getter: TSortedSetScoreGetter,
        client: CacheClient,
        cache_name: TCacheName,
        sorted_set_name: TSortedSetName,
        sorted_set_value_str: str,
        sorted_set_score: TSortedSetScore,
    ) -> None:
        set_response = client.sorted_set_put_element(
            cache_name, sorted_set_name, sorted_set_value_str, sorted_set_score
        )
        assert isinstance(set_response, CacheSortedSetPutElement.Success)

        get_response = sorted_set_score_getter(
            cache_name=cache_name, sorted_set_name=sorted_set_name, value=sorted_set_value_str
        )
        assert isinstance(get_response, CacheSortedSetGetScore.Hit)
        assert get_response.value_string == sorted_set_value_str
        assert get_response.score == sorted_set_score


class TSortedSetRankGetter(Protocol):
    def __call__(
        self,
        cache_name: TCacheName,
        sorted_set_name: TSortedSetName,
        value: TSortedSetValue,
        sort_order: SortOrder = SortOrder.ASCENDING,
    ) -> CacheSortedSetGetRankResponse:
        ...


def a_sorted_set_rank_getter() -> None:
    def with_bytes_it_succeeds_ascending(
        sorted_set_rank_getter: TSortedSetRankGetter,
        client: CacheClient,
        cache_name: TCacheName,
        sorted_set_name: TSortedSetName,
        sorted_set_elements: TSortedSetElements,
    ) -> None:
        set_response = client.sorted_set_put_elements(cache_name, sorted_set_name, sorted_set_elements)
        assert isinstance(set_response, CacheSortedSetPutElements.Success)

        sorted_keys = [k for k, v in sorted(sorted_set_elements.items(), key=lambda item: item[1])]
        first_rank_response = sorted_set_rank_getter(
            cache_name=cache_name, sorted_set_name=sorted_set_name, value=sorted_keys[0]
        )
        assert isinstance(first_rank_response, CacheSortedSetGetRank.Hit)
        assert first_rank_response.rank == 0

        second_rank_response = sorted_set_rank_getter(
            cache_name=cache_name, sorted_set_name=sorted_set_name, value=sorted_keys[1]
        )
        assert isinstance(second_rank_response, CacheSortedSetGetRank.Hit)
        assert second_rank_response.rank == 1

    def with_bytes_it_succeeds_descending(
        sorted_set_rank_getter: TSortedSetRankGetter,
        client: CacheClient,
        cache_name: TCacheName,
        sorted_set_name: TSortedSetName,
        sorted_set_elements: TSortedSetElements,
    ) -> None:
        set_response = client.sorted_set_put_elements(cache_name, sorted_set_name, sorted_set_elements)
        assert isinstance(set_response, CacheSortedSetPutElements.Success)

        sorted_keys = [k for k, v in sorted(sorted_set_elements.items(), key=lambda item: item[1])]
        first_rank_response = sorted_set_rank_getter(
            cache_name=cache_name,
            sorted_set_name=sorted_set_name,
            value=sorted_keys[0],
            sort_order=SortOrder.DESCENDING,
        )
        assert isinstance(first_rank_response, CacheSortedSetGetRank.Hit)
        assert first_rank_response.rank == 1

        second_rank_response = sorted_set_rank_getter(
            cache_name=cache_name,
            sorted_set_name=sorted_set_name,
            value=sorted_keys[1],
            sort_order=SortOrder.DESCENDING,
        )
        assert isinstance(second_rank_response, CacheSortedSetGetRank.Hit)
        assert second_rank_response.rank == 0

    def it_succeeds_ascending(
        sorted_set_rank_getter: TSortedSetRankGetter,
        client: CacheClient,
        cache_name: TCacheName,
        sorted_set_name: TSortedSetName,
        sorted_set_elements: TSortedSetElements,
    ) -> None:
        set_response = client.sorted_set_put_elements(cache_name, sorted_set_name, sorted_set_elements)
        assert isinstance(set_response, CacheSortedSetPutElements.Success)

        sorted_keys = [k for k, v in sorted(sorted_set_elements.items(), key=lambda item: item[1])]
        first_rank_response = sorted_set_rank_getter(
            cache_name=cache_name, sorted_set_name=sorted_set_name, value=sorted_keys[0]
        )
        assert isinstance(first_rank_response, CacheSortedSetGetRank.Hit)
        assert first_rank_response.rank == 0

        second_rank_response = sorted_set_rank_getter(
            cache_name=cache_name, sorted_set_name=sorted_set_name, value=sorted_keys[1]
        )
        assert isinstance(second_rank_response, CacheSortedSetGetRank.Hit)
        assert second_rank_response.rank == 1

    def it_succeeds_descending(
        sorted_set_rank_getter: TSortedSetRankGetter,
        client: CacheClient,
        cache_name: TCacheName,
        sorted_set_name: TSortedSetName,
        sorted_set_elements: TSortedSetElements,
    ) -> None:
        set_response = client.sorted_set_put_elements(cache_name, sorted_set_name, sorted_set_elements)
        assert isinstance(set_response, CacheSortedSetPutElements.Success)

        sorted_keys = [k for k, v in sorted(sorted_set_elements.items(), key=lambda item: item[1])]
        first_rank_response = sorted_set_rank_getter(
            cache_name=cache_name,
            sorted_set_name=sorted_set_name,
            value=sorted_keys[0],
            sort_order=SortOrder.DESCENDING,
        )
        assert isinstance(first_rank_response, CacheSortedSetGetRank.Hit)
        assert first_rank_response.rank == 1

        second_rank_response = sorted_set_rank_getter(
            cache_name=cache_name,
            sorted_set_name=sorted_set_name,
            value=sorted_keys[1],
            sort_order=SortOrder.DESCENDING,
        )
        assert isinstance(second_rank_response, CacheSortedSetGetRank.Hit)
        assert second_rank_response.rank == 0


class TSortedSetFetcher(Protocol):
    def __call__(
        self,
        cache_name: TCacheName,
        sorted_set_name: TSortedSetName,
        sort_order: SortOrder,
        minimum: Optional[int] = None,
        maximum: Optional[int] = None,
    ) -> CacheSortedSetFetchResponse:
        ...


def a_sorted_set_fetcher() -> None:
    def with_string_it_succeeds_ascending(
        sorted_set_fetcher: TSortedSetFetcher,
        client: CacheClient,
        cache_name: TCacheName,
        sorted_set_name: TSortedSetName,
    ) -> None:
        elements = {"one": 1.0, "two": 2.0, "three": 3.0}
        set_response = client.sorted_set_put_elements(cache_name, sorted_set_name, elements)
        assert isinstance(set_response, CacheSortedSetPutElements.Success)

        fetch_response = sorted_set_fetcher(
            cache_name=cache_name,
            sorted_set_name=sorted_set_name,
            sort_order=SortOrder.ASCENDING,
        )
        assert isinstance(fetch_response, CacheSortedSetFetch.Hit)
        fetched_values = [element[1] for element in fetch_response.value_list_string]
        assert all(earlier <= later for earlier, later in zip(fetched_values, fetched_values[1:]))

    def with_string_it_succeeds_descending(
        sorted_set_fetcher: TSortedSetFetcher,
        client: CacheClient,
        cache_name: TCacheName,
        sorted_set_name: TSortedSetName,
    ) -> None:
        elements = {"one": 1.0, "two": 2.0, "three": 3.0}
        set_response = client.sorted_set_put_elements(cache_name, sorted_set_name, elements)
        assert isinstance(set_response, CacheSortedSetPutElements.Success)

        fetch_response = sorted_set_fetcher(
            cache_name=cache_name,
            sorted_set_name=sorted_set_name,
            sort_order=SortOrder.DESCENDING,
        )
        assert isinstance(fetch_response, CacheSortedSetFetch.Hit)
        fetched_values = [element[1] for element in fetch_response.value_list_string]
        assert all(earlier >= later for earlier, later in zip(fetched_values, fetched_values[1:]))


class TSortedSetRemover(Protocol):
    def __call__(
        self, cache_name: TCacheName, sorted_set_name: TSortedSetName, value: TSortedSetValue
    ) -> CacheResponse:
        ...


def a_sorted_set_remover() -> None:
    def with_string_field_succeeds(
        client: CacheClient,
        cache_name: TCacheName,
        sorted_set_name: TSortedSetName,
        sorted_set_value_str: str,
        sorted_set_score: float,
        sorted_set_remover: TSortedSetRemover,
    ) -> None:
        set_response = client.sorted_set_put_element(
            cache_name, sorted_set_name, sorted_set_value_str, sorted_set_score
        )
        assert isinstance(set_response, CacheSortedSetPutElement.Success)

        remove_response = sorted_set_remover(
            cache_name=cache_name, sorted_set_name=sorted_set_name, value=sorted_set_value_str
        )
        assert not isinstance(remove_response, ErrorResponseMixin)

        fetch_response = client.sorted_set_fetch_by_rank(cache_name, sorted_set_name)
        assert isinstance(fetch_response, CacheSortedSetFetch.Miss)

    def with_bytes_field_succeeds(
        client: CacheClient,
        cache_name: TCacheName,
        sorted_set_name: TSortedSetName,
        sorted_set_value_str: bytes,
        sorted_set_score: float,
        sorted_set_remover: TSortedSetRemover,
    ) -> None:
        set_response = client.sorted_set_put_element(
            cache_name, sorted_set_name, sorted_set_value_str, sorted_set_score
        )
        assert isinstance(set_response, CacheSortedSetPutElement.Success)

        remove_response = sorted_set_remover(
            cache_name=cache_name, sorted_set_name=sorted_set_name, value=sorted_set_value_str
        )
        assert not isinstance(remove_response, ErrorResponseMixin)

        fetch_response = client.sorted_set_fetch_by_rank(cache_name, sorted_set_name)
        assert isinstance(fetch_response, CacheSortedSetFetch.Miss)


@behaves_like(a_cache_name_validator)
@behaves_like(a_connection_validator)
@behaves_like(a_sorted_set_setter)
@behaves_like(a_sorted_set_name_validator)
def describe_sorted_set_put_field() -> None:
    @fixture
    def cache_name_validator(
        client: CacheClient,
        sorted_set_name: TSortedSetName,
        sorted_set_value_str: str,
        sorted_set_score: TSortedSetScore,
    ) -> TCacheNameValidator:
        return partial(
            client.sorted_set_put_element,
            sorted_set_name=sorted_set_name,
            value=sorted_set_value_str,
            score=sorted_set_score,
        )

    @fixture
    def connection_validator(
        cache_name: TCacheName,
        sorted_set_name: TSortedSetName,
        sorted_set_value_str: str,
        sorted_set_score: TSortedSetScore,
    ) -> TConnectionValidator:
        def _connection_validator(client: CacheClient) -> CacheResponse:
            return client.sorted_set_put_element(
                cache_name,
                sorted_set_name=sorted_set_name,
                value=sorted_set_value_str,
                score=sorted_set_score,
            )

        return _connection_validator

    @fixture
    def sorted_set_name_validator(
        client: CacheClient,
        cache_name: TCacheName,
        sorted_set_value_str: str,
        sorted_set_score: TSortedSetScore,
    ) -> TSortedSetNameValidator:
        return partial(
            client.sorted_set_put_element,
            cache_name=cache_name,
            value=sorted_set_value_str,
            score=sorted_set_score,
        )

    @fixture
    def sorted_set_setter() -> TSortedSetSetter:
        def _sorted_set_setter(
            client: CacheClient,
            cache_name: TCacheName,
            sorted_set_name: TSortedSetName,
            elements: TSortedSetElements,
            *,
            ttl: CollectionTtl = CollectionTtl.from_cache_ttl(),
        ) -> CacheResponse:
            response = None
            for (value, score) in elements.items():
                response = client.sorted_set_put_element(
                    cache_name=cache_name,
                    sorted_set_name=sorted_set_name,
                    value=value,
                    score=score,
                    ttl=ttl,
                )
            assert response is not None
            return response

        return _sorted_set_setter


@behaves_like(a_cache_name_validator)
@behaves_like(a_connection_validator)
@behaves_like(a_sorted_set_setter)
@behaves_like(a_sorted_set_name_validator)
def describe_sorted_set_put_fields() -> None:
    @fixture
    def cache_name_validator(
        client: CacheClient,
        sorted_set_name: TSortedSetName,
        sorted_set_elements: TSortedSetElements,
    ) -> TCacheNameValidator:
        return partial(
            client.sorted_set_put_elements,
            sorted_set_name=sorted_set_name,
            elements=sorted_set_elements,
        )

    @fixture
    def connection_validator(
        cache_name: TCacheName,
        sorted_set_name: TSortedSetName,
        sorted_set_elements: TSortedSetElements,
    ) -> TConnectionValidator:
        def _connection_validator(client: CacheClient) -> CacheResponse:
            return client.sorted_set_put_elements(
                cache_name, sorted_set_name=sorted_set_name, elements=sorted_set_elements
            )

        return _connection_validator

    @fixture
    def sorted_set_name_validator(
        client: CacheClient,
        cache_name: TCacheName,
        sorted_set_elements: TSortedSetElements,
    ) -> TSortedSetNameValidator:
        return partial(
            client.sorted_set_put_elements,
            cache_name=cache_name,
            elements=sorted_set_elements,
        )

    @fixture
    def sorted_set_setter() -> TSortedSetSetter:
        def _sorted_set_setter(
            client: CacheClient,
            cache_name: TCacheName,
            sorted_set_name: TSortedSetName,
            elements: TSortedSetElements,
            *,
            ttl: CollectionTtl = CollectionTtl.from_cache_ttl(),
        ) -> CacheResponse:
            return client.sorted_set_put_elements(
                cache_name=cache_name,
                sorted_set_name=sorted_set_name,
                elements=elements,
                ttl=ttl,
            )

        return _sorted_set_setter


@behaves_like(a_cache_name_validator)
@behaves_like(a_connection_validator)
@behaves_like(a_sorted_set_name_validator)
@behaves_like(a_sorted_set_fetcher)
def describe_sorted_set_fetch_by_rank() -> None:
    @fixture
    def cache_name_validator(
        client: CacheClient,
        sorted_set_name: TSortedSetName,
    ) -> TCacheNameValidator:
        return partial(client.sorted_set_fetch_by_rank, sorted_set_name=sorted_set_name)

    @fixture
    def connection_validator(
        cache_name: TCacheName,
        sorted_set_name: TSortedSetName,
    ) -> TConnectionValidator:
        def _connection_validator(client: CacheClient) -> CacheResponse:
            return client.sorted_set_fetch_by_rank(cache_name, sorted_set_name=sorted_set_name)

        return _connection_validator

    @fixture
    def sorted_set_name_validator(client: CacheClient, cache_name: TCacheName) -> TSortedSetNameValidator:
        return partial(client.sorted_set_fetch_by_rank, cache_name=cache_name)

    @fixture
    def sorted_set_fetcher(
        client: CacheClient,
        cache_name: TCacheName,
        sorted_set_name: TSortedSetName,
    ) -> TSortedSetFetcher:
        return partial(client.sorted_set_fetch_by_rank, cache_name=cache_name, sorted_set_name=sorted_set_name)


@behaves_like(a_cache_name_validator)
@behaves_like(a_connection_validator)
@behaves_like(a_sorted_set_name_validator)
@behaves_like(a_sorted_set_fetcher)
def describe_sorted_set_fetch_by_score() -> None:
    @fixture
    def cache_name_validator(
        client: CacheClient,
        sorted_set_name: TSortedSetName,
    ) -> TCacheNameValidator:
        return partial(client.sorted_set_fetch_by_score, sorted_set_name=sorted_set_name)

    @fixture
    def connection_validator(
        cache_name: TCacheName,
        sorted_set_name: TSortedSetName,
    ) -> TConnectionValidator:
        def _connection_validator(client: CacheClient) -> CacheResponse:
            return client.sorted_set_fetch_by_score(cache_name, sorted_set_name=sorted_set_name)

        return _connection_validator

    @fixture
    def sorted_set_name_validator(client: CacheClient, cache_name: TCacheName) -> TSortedSetNameValidator:
        return partial(client.sorted_set_fetch_by_score, cache_name=cache_name)

    @fixture
    def sorted_set_fetcher(
        client: CacheClient, cache_name: TCacheName, sorted_set_name: TSortedSetName
    ) -> TSortedSetFetcher:
        return partial(client.sorted_set_fetch_by_score, cache_name=cache_name, sorted_set_name=sorted_set_name)


@behaves_like(a_cache_name_validator)
@behaves_like(a_connection_validator)
@behaves_like(a_sorted_set_name_validator)
@behaves_like(a_sorted_set_score_getter)
def describe_sorted_set_get_score() -> None:
    @fixture
    def cache_name_validator(
        client: CacheClient,
        sorted_set_name: TSortedSetName,
        sorted_set_value_str: str,
    ) -> TCacheNameValidator:
        return partial(client.sorted_set_get_score, sorted_set_name=sorted_set_name, value=sorted_set_value_str)

    @fixture
    def connection_validator(
        cache_name: TCacheName,
        sorted_set_name: TSortedSetName,
        sorted_set_value_str: str,
    ) -> TConnectionValidator:
        def _connection_validator(client: CacheClient) -> CacheResponse:
            return client.sorted_set_get_score(cache_name, sorted_set_name=sorted_set_name, value=sorted_set_value_str)

        return _connection_validator

    @fixture
    def sorted_set_name_validator(
        client: CacheClient,
        cache_name: TCacheName,
        sorted_set_value_str: str,
    ) -> TSortedSetNameValidator:
        return partial(client.sorted_set_get_score, cache_name=cache_name, value=sorted_set_value_str)

    @fixture
    def sorted_set_score_getter(client: CacheClient) -> TSortedSetScoreGetter:
        return partial(client.sorted_set_get_score)


@behaves_like(a_cache_name_validator)
@behaves_like(a_connection_validator)
@behaves_like(a_sorted_set_name_validator)
@behaves_like(a_sorted_set_score_getter)
def describe_sorted_set_get_scores() -> None:
    @fixture
    def cache_name_validator(
        client: CacheClient,
        sorted_set_name: TSortedSetName,
        sorted_set_values: TSortedSetValues,
    ) -> TCacheNameValidator:
        return partial(client.sorted_set_get_scores, sorted_set_name=sorted_set_name, values=sorted_set_values)

    @fixture
    def connection_validator(
        cache_name: TCacheName,
        sorted_set_name: TSortedSetName,
        sorted_set_values: TSortedSetValues,
    ) -> TConnectionValidator:
        def _connection_validator(client: CacheClient) -> CacheResponse:
            return client.sorted_set_get_scores(cache_name, sorted_set_name=sorted_set_name, values=sorted_set_values)

        return _connection_validator

    @fixture
    def sorted_set_name_validator(
        client: CacheClient,
        cache_name: TCacheName,
        sorted_set_values: TSortedSetValues,
    ) -> TSortedSetNameValidator:
        return partial(client.sorted_set_get_scores, cache_name=cache_name, values=sorted_set_values)

    @fixture
    def sorted_set_score_getter(client: CacheClient) -> TSortedSetScoreGetter:
        def _sorted_set_score_getter(
            cache_name: TCacheName, sorted_set_name: TSortedSetName, value: TSortedSetValue
        ) -> CacheSortedSetGetScoreResponse:
            response = client.sorted_set_get_scores(cache_name, sorted_set_name, [value])
            if isinstance(response, CacheSortedSetGetScores.Error):
                return CacheSortedSetGetScore.Error(response.inner_exception)
            elif isinstance(response, CacheSortedSetGetScores.Miss):
                bytes_value = value.encode("utf-8") if isinstance(value, str) else value
                return CacheSortedSetGetScore.Miss(bytes_value)
            assert isinstance(response, CacheSortedSetGetScores.Hit)
            return response.responses[0]

        return _sorted_set_score_getter


@behaves_like(a_cache_name_validator)
@behaves_like(a_connection_validator)
@behaves_like(a_sorted_set_name_validator)
@behaves_like(a_sorted_set_rank_getter)
def describe_sorted_set_get_rank() -> None:
    @fixture
    def cache_name_validator(
        client: CacheClient,
        sorted_set_name: TSortedSetName,
        sorted_set_value_str: str,
    ) -> TCacheNameValidator:
        return partial(client.sorted_set_get_rank, sorted_set_name=sorted_set_name, value=sorted_set_value_str)

    @fixture
    def connection_validator(
        cache_name: TCacheName,
        sorted_set_name: TSortedSetName,
        sorted_set_value_str: str,
    ) -> TConnectionValidator:
        def _connection_validator(client: CacheClient) -> CacheResponse:
            return client.sorted_set_get_rank(cache_name, sorted_set_name=sorted_set_name, value=sorted_set_value_str)

        return _connection_validator

    @fixture
    def sorted_set_name_validator(
        client: CacheClient,
        cache_name: TCacheName,
        sorted_set_value_str: str,
    ) -> TSortedSetNameValidator:
        return partial(client.sorted_set_get_rank, cache_name=cache_name, value=sorted_set_value_str)

    @fixture
    def sorted_set_rank_getter(client: CacheClient) -> TSortedSetRankGetter:
        return partial(client.sorted_set_get_rank)


@behaves_like(a_cache_name_validator)
@behaves_like(a_connection_validator)
@behaves_like(a_sorted_set_name_validator)
@behaves_like(a_sorted_set_setter)
def describe_sorted_set_increment_score() -> None:
    @fixture
    def cache_name_validator(
        client: CacheClient,
        sorted_set_name: TSortedSetName,
        sorted_set_value_str: str,
    ) -> TCacheNameValidator:
        return partial(client.sorted_set_increment_score, sorted_set_name=sorted_set_name, value=sorted_set_value_str)

    @fixture
    def connection_validator(
        cache_name: TCacheName,
        sorted_set_name: TSortedSetName,
        sorted_set_value_str: str,
    ) -> TConnectionValidator:
        def _connection_validator(client: CacheClient) -> CacheResponse:
            return client.sorted_set_increment_score(
                cache_name, sorted_set_name=sorted_set_name, value=sorted_set_value_str
            )

        return _connection_validator

    @fixture
    def sorted_set_name_validator(
        client: CacheClient,
        cache_name: TCacheName,
        sorted_set_value_str: str,
    ) -> TSortedSetNameValidator:
        return partial(client.sorted_set_increment_score, cache_name=cache_name, value=sorted_set_value_str)

    @fixture
    def sorted_set_setter() -> TSortedSetSetter:
        def _sorted_set_setter(
            client: CacheClient,
            cache_name: TCacheName,
            sorted_set_name: TSortedSetName,
            elements: TSortedSetElements,
            *,
            ttl: CollectionTtl = CollectionTtl.from_cache_ttl(),
        ) -> CacheResponse:
            response = None
            for (value, score) in elements.items():
                response = client.sorted_set_increment_score(
                    cache_name=cache_name,
                    sorted_set_name=sorted_set_name,
                    value=value,
                    score=score,
                    ttl=ttl,
                )
            assert response is not None
            return response

        return _sorted_set_setter


@behaves_like(a_cache_name_validator)
@behaves_like(a_connection_validator)
@behaves_like(a_sorted_set_name_validator)
@behaves_like(a_sorted_set_remover)
def describe_sorted_set_remove_element() -> None:
    @fixture
    def cache_name_validator(
        client: CacheClient,
        sorted_set_name: TSortedSetName,
        sorted_set_value_str: str,
    ) -> TCacheNameValidator:
        return partial(client.sorted_set_remove_element, sorted_set_name=sorted_set_name, value=sorted_set_value_str)

    @fixture
    def connection_validator(
        cache_name: TCacheName,
        sorted_set_name: TSortedSetName,
        sorted_set_value_str: str,
    ) -> TConnectionValidator:
        def _connection_validator(client: CacheClient) -> CacheResponse:
            return client.sorted_set_remove_element(
                cache_name, sorted_set_name=sorted_set_name, value=sorted_set_value_str
            )

        return _connection_validator

    @fixture
    def sorted_set_name_validator(
        client: CacheClient,
        cache_name: TCacheName,
        sorted_set_value_str: str,
    ) -> TSortedSetNameValidator:
        return partial(client.sorted_set_remove_element, cache_name=cache_name, value=sorted_set_value_str)

    @fixture
    def sorted_set_remover(client: CacheClient) -> TSortedSetRemover:
        return partial(client.sorted_set_remove_element)


@behaves_like(a_cache_name_validator)
@behaves_like(a_connection_validator)
@behaves_like(a_sorted_set_name_validator)
@behaves_like(a_sorted_set_remover)
def describe_sorted_set_remove_elements() -> None:
    @fixture
    def cache_name_validator(
        client: CacheClient,
        sorted_set_name: TSortedSetName,
        sorted_set_values: TSortedSetValues,
    ) -> TCacheNameValidator:
        return partial(client.sorted_set_remove_elements, sorted_set_name=sorted_set_name, values=sorted_set_values)

    @fixture
    def connection_validator(
        cache_name: TCacheName,
        sorted_set_name: TSortedSetName,
        sorted_set_values: TSortedSetValues,
    ) -> TConnectionValidator:
        def _connection_validator(client: CacheClient) -> CacheResponse:
            return client.sorted_set_remove_elements(
                cache_name, sorted_set_name=sorted_set_name, values=sorted_set_values
            )

        return _connection_validator

    @fixture
    def sorted_set_name_validator(
        client: CacheClient,
        cache_name: TCacheName,
        sorted_set_values: TSortedSetValues,
    ) -> TSortedSetNameValidator:
        return partial(client.sorted_set_remove_elements, cache_name=cache_name, values=sorted_set_values)

    @fixture
    def sorted_set_remover(client: CacheClient) -> TSortedSetRemover:
        def _sorted_set_remover(
            cache_name: TCacheName, sorted_set_name: TSortedSetName, value: TSortedSetValue
        ) -> CacheResponse:
            return client.sorted_set_remove_elements(
                cache_name=cache_name, sorted_set_name=sorted_set_name, values=[value]
            )

        return _sorted_set_remover

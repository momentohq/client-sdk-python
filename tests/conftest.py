from __future__ import annotations

import asyncio
import os
import random
from datetime import timedelta
from typing import AsyncIterator, Callable, Iterator, List, Optional, Union, cast

import pytest
import pytest_asyncio
from momento import (
    CacheClient,
    CacheClientAsync,
    Configurations,
    CredentialProvider,
    TopicClient,
    TopicClientAsync,
    TopicConfigurations,
)
from momento.config import Configuration, TopicConfiguration
from momento.typing import (
    TCacheName,
    TDictionaryField,
    TDictionaryFields,
    TDictionaryItems,
    TDictionaryName,
    TListName,
    TListValue,
    TScalarKey,
    TScalarValue,
    TSetElement,
    TSetElementsInput,
    TSetName,
    TSortedSetElements,
    TSortedSetName,
    TSortedSetScore,
    TSortedSetValues,
    TTopicName,
)

from tests.utils import (
    unique_test_cache_name,
    uuid_bytes,
    uuid_str,
)

#######################
# Integration test data
#######################

TEST_CONFIGURATION = Configurations.Laptop.latest()
TEST_TOPIC_CONFIGURATION = TopicConfigurations.Default.latest()


TEST_AUTH_PROVIDER = CredentialProvider.from_environment_variable("TEST_API_KEY")


TEST_CACHE_NAME: Optional[str] = os.getenv("TEST_CACHE_NAME")
if not TEST_CACHE_NAME:
    raise RuntimeError("Integration tests require TEST_CACHE_NAME env var; see README for more details.")

TEST_TOPIC_NAME: Optional[str] = "my-topic"

DEFAULT_TTL_SECONDS: timedelta = timedelta(seconds=60)
BAD_API_KEY: str = "eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJpbnRlZ3JhdGlvbiIsImNwIjoiY29udHJvbC5jZWxsLWFscGhhLWRldi5wcmVwcm9kLmEubW9tZW50b2hxLmNvbSIsImMiOiJjYWNoZS5jZWxsLWFscGhhLWRldi5wcmVwcm9kLmEubW9tZW50b2hxLmNvbSJ9.gdghdjjfjyehhdkkkskskmmls76573jnajhjjjhjdhnndy"  # noqa: E501


#############################################
# Integration test fixtures: data and clients
#############################################


@pytest.fixture(scope="session")
def credential_provider() -> CredentialProvider:
    return TEST_AUTH_PROVIDER


@pytest.fixture(scope="session")
def bad_token_credential_provider() -> CredentialProvider:
    os.environ["BAD_API_KEY"] = BAD_API_KEY
    return CredentialProvider.from_environment_variable("BAD_API_KEY")


@pytest.fixture(scope="session")
def configuration() -> Configuration:
    return TEST_CONFIGURATION


@pytest.fixture(scope="session")
def topic_configuration() -> TopicConfiguration:
    return TEST_TOPIC_CONFIGURATION


@pytest.fixture(scope="session")
def cache_name() -> TCacheName:
    return cast(str, TEST_CACHE_NAME)


@pytest.fixture(scope="session")
def topic_name() -> TTopicName:
    return cast(str, TEST_TOPIC_NAME)


@pytest.fixture
def list_name() -> TListName:
    return uuid_str()


@pytest.fixture
def list_value() -> TListValue:
    return cast(TListValue, random.choice((uuid_bytes(), uuid_str())))


@pytest.fixture
def key() -> TScalarKey:
    return cast(TScalarKey, random.choice((uuid_bytes(), uuid_str())))


@pytest.fixture
def value() -> TScalarValue:
    return cast(TScalarValue, random.choice((uuid_bytes(), uuid_str())))


@pytest.fixture
def values_bytes() -> list[bytes]:
    return [uuid_bytes(), uuid_bytes(), uuid_bytes()]


@pytest.fixture()
def values() -> list[str | bytes]:
    val = random.choice(([uuid_bytes(), uuid_bytes(), uuid_bytes()], [uuid_str(), uuid_str(), uuid_str()]))
    return cast(List[Union[str, bytes]], val)


@pytest.fixture
def values_str() -> list[str]:
    return [uuid_str(), uuid_str(), uuid_str()]


@pytest.fixture
def dictionary_name() -> TDictionaryName:
    return uuid_str()


@pytest.fixture
def dictionary_field() -> TDictionaryField:
    return cast(TDictionaryField, random.choice((uuid_bytes(), uuid_str())))


@pytest.fixture
def dictionary_field_str() -> str:
    return uuid_str()


@pytest.fixture
def dictionary_value_str() -> str:
    return uuid_str()


@pytest.fixture
def dictionary_field_bytes() -> bytes:
    return uuid_bytes()


@pytest.fixture
def dictionary_value_bytes() -> bytes:
    return uuid_bytes()


@pytest.fixture
def dictionary_fields() -> TDictionaryFields:
    return [uuid_str(), uuid_str(), uuid_bytes(), uuid_bytes()]


@pytest.fixture
def dictionary_items() -> TDictionaryItems:
    return cast(TDictionaryItems, {uuid_str(): uuid_str(), uuid_bytes(): uuid_bytes()})


@pytest.fixture
def increment_amount() -> int:
    return random.randint(-42, 42)


@pytest.fixture
def set_name() -> TSetName:
    return uuid_str()


@pytest.fixture
def sorted_set_name() -> TSortedSetName:
    return uuid_str()


@pytest.fixture
def sorted_set_value_str() -> str:
    return uuid_str()


@pytest.fixture
def sorted_set_value_bytes() -> bytes:
    return uuid_bytes()


@pytest.fixture
def sorted_set_values() -> TSortedSetValues:
    return [uuid_str(), uuid_str(), uuid_bytes(), uuid_bytes()]


@pytest.fixture
def sorted_set_score() -> TSortedSetScore:
    return random.random()


@pytest.fixture
def sorted_set_elements() -> TSortedSetElements:
    return cast(TSortedSetElements, {uuid_str(): random.random(), uuid_bytes(): random.random()})


@pytest.fixture
def element() -> TSetElement:
    return cast(TSetElement, random.choice((uuid_bytes(), uuid_str())))


@pytest.fixture
def elements() -> TSetElementsInput:
    return cast(
        TSetElementsInput,
        {
            random.choice((uuid_bytes(), uuid_str())),
            random.choice((uuid_bytes(), uuid_str())),
            random.choice((uuid_bytes(), uuid_str())),
        },
    )


@pytest.fixture
def elements_str() -> set[str]:
    return {uuid_str(), uuid_str(), uuid_str()}


@pytest.fixture
def elements_bytes() -> set[bytes]:
    return {uuid_bytes(), uuid_bytes(), uuid_bytes()}


@pytest.fixture(scope="session")
def default_ttl_seconds() -> timedelta:
    return DEFAULT_TTL_SECONDS


@pytest.fixture(scope="session")
def bad_auth_token() -> str:
    return BAD_API_KEY


@pytest.fixture(scope="session")
def event_loop() -> Iterator[asyncio.AbstractEventLoop]:
    """See also: https://github.com/pytest-dev/pytest-asyncio#event_loop."""
    policy = asyncio.get_event_loop_policy()
    loop = policy.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
def client() -> Iterator[CacheClient]:
    with CacheClient(TEST_CONFIGURATION, TEST_AUTH_PROVIDER, DEFAULT_TTL_SECONDS) as _client:
        # Ensure test cache exists
        _client.create_cache(cast(str, TEST_CACHE_NAME))
        try:
            yield _client
        finally:
            _client.delete_cache(cast(str, TEST_CACHE_NAME))


@pytest.fixture(scope="session")
def client_eager_connection() -> Iterator[CacheClient]:
    with CacheClient.create(
        TEST_CONFIGURATION, TEST_AUTH_PROVIDER, DEFAULT_TTL_SECONDS, eager_connection_timeout=timedelta(seconds=10)
    ) as _client:
        # Ensure test cache exists
        _client.create_cache(cast(str, TEST_CACHE_NAME))
        try:
            yield _client
        finally:
            _client.delete_cache(cast(str, TEST_CACHE_NAME))


@pytest_asyncio.fixture(scope="session")
async def client_async() -> AsyncIterator[CacheClientAsync]:
    async with CacheClientAsync(TEST_CONFIGURATION, TEST_AUTH_PROVIDER, DEFAULT_TTL_SECONDS) as _client:
        # Ensure test cache exists
        # TODO consider deleting cache on when test runner shuts down
        await _client.create_cache(cast(str, TEST_CACHE_NAME))
        try:
            yield _client
        finally:
            await _client.delete_cache(cast(str, TEST_CACHE_NAME))


@pytest_asyncio.fixture(scope="session")
async def client_async_eager_connection() -> AsyncIterator[CacheClientAsync]:
    async with await CacheClientAsync.create(
        TEST_CONFIGURATION, TEST_AUTH_PROVIDER, DEFAULT_TTL_SECONDS, timedelta(seconds=10)
    ) as _client:
        await _client.create_cache(cast(str, TEST_CACHE_NAME))
        try:
            yield _client
        finally:
            await _client.delete_cache(cast(str, TEST_CACHE_NAME))


@pytest.fixture(scope="session")
def topic_client() -> Iterator[TopicClient]:
    with TopicClient(TEST_TOPIC_CONFIGURATION, TEST_AUTH_PROVIDER) as _client:
        yield _client


@pytest.fixture(scope="session")
async def topic_client_async() -> AsyncIterator[TopicClientAsync]:
    async with TopicClientAsync(TEST_TOPIC_CONFIGURATION, TEST_AUTH_PROVIDER) as _topic_client:
        yield _topic_client


TUniqueCacheName = Callable[[CacheClient], str]


@pytest.fixture
def unique_cache_name(client: CacheClient) -> Iterator[Callable[[CacheClient], str]]:
    """Synchronous version of unique_cache_name_async."""
    cache_names = []

    def _unique_cache_name(client: CacheClient) -> str:
        cache_name = unique_test_cache_name()
        cache_names.append(cache_name)
        return cache_name

    try:
        yield _unique_cache_name
    finally:
        for cache_name in cache_names:
            client.delete_cache(cache_name)


TUniqueCacheNameAsync = Callable[[CacheClientAsync], str]


@pytest_asyncio.fixture
async def unique_cache_name_async(
    client_async: CacheClientAsync,
) -> AsyncIterator[Callable[[CacheClientAsync], str]]:
    """Returns unique cache name for testing.

    Also ensures the cache is deleted after the test, even if the test fails.

    It does not create the cache for you.

    Args:
        client_async (CacheClientAsync): The client to use to delete the cache.

    Returns:
        str: the unique cache name.
    """
    cache_names = []

    def _unique_cache_name_async(client: CacheClientAsync) -> str:
        cache_name = unique_test_cache_name()
        cache_names.append(cache_name)
        return cache_name

    try:
        yield _unique_cache_name_async
    finally:
        for cache_name in cache_names:
            await client_async.delete_cache(cache_name)

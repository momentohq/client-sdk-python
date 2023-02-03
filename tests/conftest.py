import asyncio
import os
import random
from datetime import timedelta
from typing import Optional, cast

import pytest
import pytest_asyncio

from momento import SimpleCacheClient, SimpleCacheClientAsync
from momento.auth import CredentialProvider
from momento.config import Configuration, Laptop
from momento.typing import (
    TCacheName,
    TDictionaryField,
    TDictionaryItems,
    TDictionaryName,
    TDictionaryValue,
    TListName,
    TListValue,
    TListValuesInput,
    TListValuesInputBytes,
    TListValuesInputStr,
)
from tests.utils import unique_test_cache_name, uuid_bytes, uuid_str

#######################
# Integration test data
#######################

TEST_CONFIGURATION = Laptop.latest()

TEST_AUTH_PROVIDER = CredentialProvider.from_environment_variable("TEST_AUTH_TOKEN")

TEST_CACHE_NAME: Optional[str] = os.getenv("TEST_CACHE_NAME")
if not TEST_CACHE_NAME:
    raise RuntimeError("Integration tests require TEST_CACHE_NAME env var; see README for more details.")

DEFAULT_TTL_SECONDS: timedelta = timedelta(seconds=60)
BAD_AUTH_TOKEN: str = "eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJpbnRlZ3JhdGlvbiIsImNwIjoiY29udHJvbC5jZWxsLWFscGhhLWRldi5wcmVwcm9kLmEubW9tZW50b2hxLmNvbSIsImMiOiJjYWNoZS5jZWxsLWFscGhhLWRldi5wcmVwcm9kLmEubW9tZW50b2hxLmNvbSJ9.gdghdjjfjyehhdkkkskskmmls76573jnajhjjjhjdhnndy"  # noqa: E501


#############################################
# Integration test fixtures: data and clients
#############################################


@pytest.fixture(scope="session")
def credential_provider() -> CredentialProvider:
    return TEST_AUTH_PROVIDER


@pytest.fixture(scope="session")
def bad_token_credential_provider() -> CredentialProvider:
    os.environ["BAD_AUTH_TOKEN"] = BAD_AUTH_TOKEN
    return CredentialProvider.from_environment_variable("BAD_AUTH_TOKEN")


@pytest.fixture(scope="session")
def configuration() -> Configuration:
    return TEST_CONFIGURATION


@pytest.fixture(scope="session")
def cache_name() -> TCacheName:
    return cast(str, TEST_CACHE_NAME)


@pytest.fixture
def list_name() -> TListName:
    return uuid_str()


@pytest.fixture
def list_value() -> TListValue:
    return random.choice((uuid_bytes(), uuid_str()))


@pytest.fixture
def values_bytes() -> TListValuesInputBytes:
    return [uuid_bytes(), uuid_bytes(), uuid_bytes()]


@pytest.fixture()
def values() -> TListValuesInput:
    return random.choice(([uuid_bytes(), uuid_bytes(), uuid_bytes()], [uuid_str(), uuid_str(), uuid_str()]))


@pytest.fixture
def values_str() -> TListValuesInputStr:
    return [uuid_str(), uuid_str(), uuid_str()]


@pytest.fixture
def dictionary_name() -> TDictionaryName:
    return uuid_str()


@pytest.fixture
def dictionary_field() -> TDictionaryField:
    return uuid_str()


@pytest.fixture
def dictionary_value() -> TDictionaryValue:
    return uuid_str()


@pytest.fixture
def dictionary_items() -> TDictionaryItems:
    return dict([(uuid_str(), uuid_str()), (uuid_bytes(), uuid_bytes())])


@pytest.fixture(scope="session")
def default_ttl_seconds() -> timedelta:
    return DEFAULT_TTL_SECONDS


@pytest.fixture(scope="session")
def bad_auth_token() -> str:
    return BAD_AUTH_TOKEN


@pytest.fixture(scope="session")
def event_loop() -> asyncio.AbstractEventLoop:  # type: ignore
    """cf https://github.com/pytest-dev/pytest-asyncio#event_loop"""
    policy = asyncio.get_event_loop_policy()
    loop = policy.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
def client() -> SimpleCacheClient:
    configuration = Laptop.latest()
    credential_provider = CredentialProvider.from_environment_variable("TEST_AUTH_TOKEN")
    with SimpleCacheClient(configuration, credential_provider, DEFAULT_TTL_SECONDS) as _client:
        # Ensure test cache exists
        _client.create_cache(TEST_CACHE_NAME)
        try:
            yield _client
        finally:
            _client.delete_cache(TEST_CACHE_NAME)


@pytest_asyncio.fixture(scope="session")
async def client_async() -> SimpleCacheClientAsync:
    configuration = Laptop.latest()
    credential_provider = CredentialProvider.from_environment_variable("TEST_AUTH_TOKEN")
    async with SimpleCacheClientAsync(configuration, credential_provider, DEFAULT_TTL_SECONDS) as _client:
        # Ensure test cache exists
        # TODO consider deleting cache on when test runner shuts down
        await _client.create_cache(TEST_CACHE_NAME)
        try:
            yield _client
        finally:
            await _client.delete_cache(TEST_CACHE_NAME)


@pytest.fixture
def unique_cache_name(client: SimpleCacheClient) -> str:
    """Synchronous version of unique_cache_name_async"""

    cache_names = []

    def _unique_cache_name(client: SimpleCacheClient) -> str:
        cache_name = unique_test_cache_name()
        cache_names.append(cache_name)
        return cache_name

    try:
        yield _unique_cache_name
    finally:
        for cache_name in cache_names:
            client.delete_cache(cache_name)


#
@pytest_asyncio.fixture
async def unique_cache_name_async(client_async: SimpleCacheClientAsync) -> str:
    """Returns unique cache name for testing, and ensures the cache is deleted after the test,
    even if the test fails.

    It does not create the cache for you.

    Args:
        client_async (SimpleCacheClientAsync): The client to use to delete the cache.

    Returns:
        str: the unique cache name
    """

    cache_names = []

    def _unique_cache_name_async(client: SimpleCacheClientAsync) -> str:
        cache_name = unique_test_cache_name()
        cache_names.append(cache_name)
        return cache_name

    try:
        yield _unique_cache_name_async
    finally:
        for cache_name in cache_names:
            await client_async.delete_cache(cache_name)

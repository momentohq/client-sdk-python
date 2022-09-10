import asyncio
import os

import pytest

import momento.errors as errors
import momento.simple_cache_client as simple_cache_client
from momento.simple_cache_client import SimpleCacheClient
import momento.aio.simple_cache_client as simple_cache_client_async
from momento.aio.simple_cache_client import (
    SimpleCacheClient as SimpleCacheClientAsync,
)
import momento.incubating.aio.simple_cache_client as incubating_simple_cache_client_async
from momento.incubating.aio.simple_cache_client import (
    SimpleCacheClient as IncubatingSimpleCacheClientAsync,
)
import momento.incubating.simple_cache_client as incubating_simple_cache_client
from momento.incubating.simple_cache_client import (
    SimpleCacheClient as IncubatingSimpleCacheClient,
)


#######################
# Integration test data
#######################


TEST_AUTH_TOKEN: str = os.getenv("TEST_AUTH_TOKEN")
if not TEST_AUTH_TOKEN:
    raise RuntimeError(
        "Integration tests require TEST_AUTH_TOKEN env var; see README for more details."
    )

TEST_CACHE_NAME: str = os.getenv("TEST_CACHE_NAME")
if not TEST_CACHE_NAME:
    raise RuntimeError(
        "Integration tests require TEST_CACHE_NAME env var; see README for more details."
    )

DEFAULT_TTL_SECONDS: int = 60
BAD_AUTH_TOKEN = "eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJpbnRlZ3JhdGlvbiIsImNwIjoiY29udHJvbC5jZWxsLWFscGhhLWRldi5wcmVwcm9kLmEubW9tZW50b2hxLmNvbSIsImMiOiJjYWNoZS5jZWxsLWFscGhhLWRldi5wcmVwcm9kLmEubW9tZW50b2hxLmNvbSJ9.gdghdjjfjyehhdkkkskskmmls76573jnajhjjjhjdhnndy"


#############################################
# Integration test fixtures: data and clients
#############################################


@pytest.fixture(scope="session")
def auth_token() -> str:
    return TEST_AUTH_TOKEN


@pytest.fixture(scope="session")
def cache_name() -> str:
    return TEST_CACHE_NAME


@pytest.fixture(scope="session")
def default_ttl_seconds() -> int:
    return DEFAULT_TTL_SECONDS


@pytest.fixture(scope="session")
def bad_auth_token() -> str:
    return BAD_AUTH_TOKEN


@pytest.fixture(scope="session")
def event_loop():
    return asyncio.get_event_loop()


@pytest.fixture(scope="session")
def client() -> SimpleCacheClient:
    with simple_cache_client.init(TEST_AUTH_TOKEN, DEFAULT_TTL_SECONDS) as _client:
        # Ensure test cache exists
        try:
            _client.create_cache(TEST_CACHE_NAME)
        except errors.AlreadyExistsError:
            pass

        yield _client


@pytest.fixture(scope="session")
async def client_async() -> SimpleCacheClientAsync:
    async with simple_cache_client_async.init(
        TEST_AUTH_TOKEN, DEFAULT_TTL_SECONDS
    ) as _client:
        # Ensure test cache exists
        try:
            await _client.create_cache(TEST_CACHE_NAME)
        except errors.AlreadyExistsError:
            pass

        yield _client


@pytest.fixture(scope="session")
async def incubating_client_async() -> IncubatingSimpleCacheClientAsync:
    async with incubating_simple_cache_client_async.init(
        TEST_AUTH_TOKEN, DEFAULT_TTL_SECONDS
    ) as client:
        # Ensure test cache exists
        try:
            await client.create_cache(TEST_CACHE_NAME)
        except errors.AlreadyExistsError:
            pass

        yield client


@pytest.fixture(scope="session")
def incubating_client() -> IncubatingSimpleCacheClient:
    with incubating_simple_cache_client.init(
        TEST_AUTH_TOKEN, DEFAULT_TTL_SECONDS
    ) as client:
        # Ensure test cache exists
        try:
            client.create_cache(TEST_CACHE_NAME)
        except errors.AlreadyExistsError:
            pass

        yield client

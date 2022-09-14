import asyncio
import os
from typing import cast, Optional

import pytest
import pytest_asyncio

import momento.errors as errors
from momento.simple_cache_client import SimpleCacheClient
from momento.aio.simple_cache_client import (
    SimpleCacheClient as SimpleCacheClientAsync,
)
from momento.incubating.aio.simple_cache_client import (
    SimpleCacheClientIncubating as IncubatingSimpleCacheClientAsync,
)
from momento.incubating.simple_cache_client import (
    SimpleCacheClientIncubating as IncubatingSimpleCacheClient,
)


#######################
# Integration test data
#######################


TEST_AUTH_TOKEN: Optional[str] = os.getenv("TEST_AUTH_TOKEN")
if not TEST_AUTH_TOKEN:
    raise RuntimeError(
        "Integration tests require TEST_AUTH_TOKEN env var; see README for more details."
    )

TEST_CACHE_NAME: Optional[str] = os.getenv("TEST_CACHE_NAME")
if not TEST_CACHE_NAME:
    raise RuntimeError(
        "Integration tests require TEST_CACHE_NAME env var; see README for more details."
    )

DEFAULT_TTL_SECONDS: int = 60
BAD_AUTH_TOKEN: str = "eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJpbnRlZ3JhdGlvbiIsImNwIjoiY29udHJvbC5jZWxsLWFscGhhLWRldi5wcmVwcm9kLmEubW9tZW50b2hxLmNvbSIsImMiOiJjYWNoZS5jZWxsLWFscGhhLWRldi5wcmVwcm9kLmEubW9tZW50b2hxLmNvbSJ9.gdghdjjfjyehhdkkkskskmmls76573jnajhjjjhjdhnndy"  # noqa: E501


#############################################
# Integration test fixtures: data and clients
#############################################


@pytest.fixture(scope="session")
def auth_token() -> str:
    return cast(str, TEST_AUTH_TOKEN)


@pytest.fixture(scope="session")
def cache_name() -> str:
    return cast(str, TEST_CACHE_NAME)


@pytest.fixture(scope="session")
def default_ttl_seconds() -> int:
    return DEFAULT_TTL_SECONDS


@pytest.fixture(scope="session")
def bad_auth_token() -> str:
    return BAD_AUTH_TOKEN


@pytest.fixture(scope="session")
def event_loop() -> asyncio.AbstractEventLoop:
    """cf https://github.com/pytest-dev/pytest-asyncio#event_loop"""
    policy = asyncio.get_event_loop_policy()
    loop = policy.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
def client() -> SimpleCacheClient:
    with SimpleCacheClient(TEST_AUTH_TOKEN, DEFAULT_TTL_SECONDS) as _client:
        # Ensure test cache exists
        try:
            _client.create_cache(TEST_CACHE_NAME)
        except errors.AlreadyExistsError:
            pass

        yield _client


@pytest_asyncio.fixture(scope="session")
async def client_async() -> SimpleCacheClientAsync:
    async with SimpleCacheClientAsync(TEST_AUTH_TOKEN, DEFAULT_TTL_SECONDS) as _client:
        # Ensure test cache exists
        try:
            await _client.create_cache(TEST_CACHE_NAME)
        except errors.AlreadyExistsError:
            pass

        yield _client


@pytest.fixture(scope="module")
def incubating_client() -> IncubatingSimpleCacheClient:
    with IncubatingSimpleCacheClient(TEST_AUTH_TOKEN, DEFAULT_TTL_SECONDS) as client:
        # Ensure test cache exists
        try:
            client.create_cache(TEST_CACHE_NAME)
        except errors.AlreadyExistsError:
            pass

        yield client


@pytest_asyncio.fixture(scope="session")
async def incubating_client_async() -> IncubatingSimpleCacheClientAsync:
    async with IncubatingSimpleCacheClientAsync(
        TEST_AUTH_TOKEN, DEFAULT_TTL_SECONDS
    ) as client:
        # Ensure test cache exists
        try:
            await client.create_cache(TEST_CACHE_NAME)
        except errors.AlreadyExistsError:
            pass

        yield client

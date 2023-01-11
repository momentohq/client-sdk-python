import asyncio
from datetime import timedelta
import os
from typing import Optional, cast

import pytest
import pytest_asyncio

from momento.auth.credential_provider import EnvMomentoTokenProvider
from momento.config.configuration import Configuration
from momento.config.configurations import Laptop
import momento.errors as errors
from momento.aio.simple_cache_client import SimpleCacheClient as SimpleCacheClientAsync
from momento.simple_cache_client import SimpleCacheClient

#######################
# Integration test data
#######################

TEST_CONFIGURATION = Laptop.latest()

TEST_AUTH_PROVIDER = EnvMomentoTokenProvider("TEST_AUTH_TOKEN")

TEST_AUTH_TOKEN: Optional[str] = os.getenv("TEST_AUTH_TOKEN")
if not TEST_AUTH_TOKEN:
    raise RuntimeError("Integration tests require TEST_AUTH_TOKEN env var; see README for more details.")

TEST_CACHE_NAME: Optional[str] = os.getenv("TEST_CACHE_NAME")
if not TEST_CACHE_NAME:
    raise RuntimeError("Integration tests require TEST_CACHE_NAME env var; see README for more details.")

DEFAULT_TTL_SECONDS: timedelta = timedelta(seconds=60)
BAD_AUTH_TOKEN: str = "eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJpbnRlZ3JhdGlvbiIsImNwIjoiY29udHJvbC5jZWxsLWFscGhhLWRldi5wcmVwcm9kLmEubW9tZW50b2hxLmNvbSIsImMiOiJjYWNoZS5jZWxsLWFscGhhLWRldi5wcmVwcm9kLmEubW9tZW50b2hxLmNvbSJ9.gdghdjjfjyehhdkkkskskmmls76573jnajhjjjhjdhnndy"  # noqa: E501


#############################################
# Integration test fixtures: data and clients
#############################################


@pytest.fixture(scope="session")
def auth_provider() -> EnvMomentoTokenProvider:
    return TEST_AUTH_PROVIDER


@pytest.fixture(scope="session")
def bad_token_auth_provider() -> EnvMomentoTokenProvider:
    os.environ["BAD_AUTH_TOKEN"] = BAD_AUTH_TOKEN
    return EnvMomentoTokenProvider("BAD_AUTH_TOKEN")


@pytest.fixture(scope="session")
def auth_token() -> str:
    return cast(str, TEST_AUTH_TOKEN)


@pytest.fixture(scope="session")
def configuration() -> Configuration:
    return TEST_CONFIGURATION


@pytest.fixture(scope="session")
def cache_name() -> str:
    return cast(str, TEST_CACHE_NAME)


@pytest.fixture(scope="session")
def default_ttl_seconds() -> timedelta:
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
    configuration = Laptop.latest()
    auth_provider = EnvMomentoTokenProvider("TEST_AUTH_TOKEN")
    with SimpleCacheClient(configuration, auth_provider, DEFAULT_TTL_SECONDS) as _client:
        # Ensure test cache exists
        try:
            _client.create_cache(TEST_CACHE_NAME)
        except errors.AlreadyExistsError:
            pass

        yield _client


@pytest_asyncio.fixture(scope="session")
async def client_async() -> SimpleCacheClientAsync:
    configuration = Laptop.latest()
    auth_provider = EnvMomentoTokenProvider("TEST_AUTH_TOKEN")
    async with SimpleCacheClientAsync(configuration, auth_provider, DEFAULT_TTL_SECONDS) as _client:
        # Ensure test cache exists
        try:
            # TODO consider deleting cache on when test runner shuts down
            await _client.create_cache(TEST_CACHE_NAME)
        except errors.AlreadyExistsError:
            pass

        yield _client

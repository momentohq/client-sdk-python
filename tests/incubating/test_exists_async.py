import os
import unittest

import pytest

import momento.errors as errors
import momento.incubating.aio.simple_cache_client as simple_cache_client
from momento.incubating.aio.simple_cache_client import SimpleCacheClient
from tests.utils import uuid_str

_AUTH_TOKEN = os.getenv("TEST_AUTH_TOKEN")
_TEST_CACHE_NAME = os.getenv("TEST_CACHE_NAME")
_DEFAULT_TTL_SECONDS = 60


@pytest.fixture
async def client() -> SimpleCacheClient:
    if not _AUTH_TOKEN:
        raise RuntimeError(
            "Integration tests require TEST_AUTH_TOKEN env var; see README for more details."
        )
    if not _TEST_CACHE_NAME:
        raise RuntimeError(
            "Integration tests require TEST_CACHE_NAME env var; see README for more details."
        )

    async with simple_cache_client.init(_AUTH_TOKEN, _DEFAULT_TTL_SECONDS) as _client:
        # Ensure test cache exists
        try:
            await _client.create_cache(_TEST_CACHE_NAME)
        except errors.AlreadyExistsError:
            pass

        yield _client


async def test_exists_unary_missing(client: SimpleCacheClient):
    key = uuid_str()
    response = await client.exists(_TEST_CACHE_NAME, key)
    assert not response
    assert response.num_exists() == 0
    assert response.results() == [False]
    assert response.missing_keys() == [key]
    assert response.present_keys() == []
    assert list(response.zip_keys_and_results()) == [(key, False)]


async def test_exists_unary_exists(client: SimpleCacheClient):
    key, value = uuid_str(), uuid_str()
    await client.set(_TEST_CACHE_NAME, key, value)

    response = await client.exists(_TEST_CACHE_NAME, key)

    assert response
    assert response.num_exists() == 1
    assert response.results() == [True]
    assert response.missing_keys() == []
    assert response.present_keys() == [key]
    assert list(response.zip_keys_and_results()) == [(key, True)]


async def test_exists_multi(client: SimpleCacheClient):
    keys = []
    for i in range(3):
        key = uuid_str()
        await client.set(_TEST_CACHE_NAME, key, uuid_str())
        keys.append(key)

    response = await client.exists(_TEST_CACHE_NAME, *keys)
    assert response
    assert response.all()
    assert response.num_exists() == 3
    assert response.results() == [True] * 3
    assert response.missing_keys() == []
    assert response.present_keys() == keys
    assert list(response.zip_keys_and_results()) == list(zip(keys, [True] * 3))

    missing1, missing2 = uuid_str(), uuid_str()
    more_keys = [missing1] + keys + [missing2]
    response = await client.exists(_TEST_CACHE_NAME, *more_keys)
    assert not response
    assert not response.all()
    assert response.num_exists() == 3

    mask = [False] + [True] * 3 + [False]
    assert response.results() == mask

    assert response.missing_keys() == [missing1, missing2]
    assert response.present_keys() == keys
    assert list(response.zip_keys_and_results()) == list(zip(more_keys, mask))

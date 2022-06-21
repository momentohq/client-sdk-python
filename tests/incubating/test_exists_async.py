import os
import unittest

import momento.incubating.aio.simple_cache_client as simple_cache_client
from momento.vendor.python.unittest.async_case import IsolatedAsyncioTestCase

_AUTH_TOKEN = os.getenv("TEST_AUTH_TOKEN")
_TEST_CACHE_NAME = os.getenv("TEST_CACHE_NAME")
_DEFAULT_TTL_SECONDS = 60


class TestMomentoAsync(IsolatedAsyncioTestCase):
    async def test_exists_unary_missing(self):
        async with simple_cache_client.init(
            _AUTH_TOKEN, _DEFAULT_TTL_SECONDS
        ) as simple_cache:
            response = await simple_cache.exists(_TEST_CACHE_NAME, "exists-key")

            self.assertFalse(response)
            self.assertEqual(0, response.num_exists())
            self.assertEqual([False], response.results())

    async def test_exists_unary_exists(self):
        async with simple_cache_client.init(
            _AUTH_TOKEN, _DEFAULT_TTL_SECONDS
        ) as simple_cache:
            await simple_cache.set(_TEST_CACHE_NAME, "exists-key1", "my-value")

            response = await simple_cache.exists(_TEST_CACHE_NAME, "exists-key1")

            self.assertTrue(response)
            self.assertEqual(1, response.num_exists())
            self.assertEqual([True], response.results())

    async def test_exists_multi(self):
        async with simple_cache_client.init(
            _AUTH_TOKEN, _DEFAULT_TTL_SECONDS
        ) as simple_cache:
            keys = []
            for i in range(2, 5):
                key = f"exists-key{i}"
                await simple_cache.set(_TEST_CACHE_NAME, key, f"my-value-{i}")
                keys.append(key)

            response = await simple_cache.exists(_TEST_CACHE_NAME, *keys)
            self.assertTrue(response)
            self.assertTrue(response.all())
            self.assertEqual(3, response.num_exists())
            self.assertEqual([True] * 3, response.results())

            more_keys = ["I'm not here"] + keys + ["Neither am I"]
            response = await simple_cache.exists(_TEST_CACHE_NAME, *more_keys)
            self.assertFalse(response)
            self.assertFalse(response.all())
            self.assertEqual(3, response.num_exists())
            self.assertEqual([False] + [True] * 3 + [False], response.results())


if __name__ == "__main__":
    unittest.main()

import asyncio
import unittest
import os
from typing import List

import momento.aio.simple_cache_client as aio
import momento.errors as errors
from momento.tools.cached_async import cached_async

from momento.vendor.python.unittest.async_case import IsolatedAsyncioTestCase

_AUTH_TOKEN = os.getenv('TEST_AUTH_TOKEN')
_TEST_CACHE_NAME = os.getenv('TEST_CACHE_NAME')
_DEFAULT_TTL_SECONDS = 60


class TestCachedAsync(IsolatedAsyncioTestCase):
    client: aio.SimpleCacheClient = None
    invocations: List[str] = None

    @classmethod
    async def asyncSetUp(self) -> None:
        self.invocations = []
        if not _AUTH_TOKEN:
            raise RuntimeError(
                "Integration tests require TEST_AUTH_TOKEN env var; see README for more details."
            )
        if not _TEST_CACHE_NAME:
            raise RuntimeError(
                "Integration tests require TEST_CACHE_NAME env var; see README for more details."
            )

        # default client for use in tests
        self.client = aio.init(_AUTH_TOKEN, _DEFAULT_TTL_SECONDS)
        await self.client.__aenter__()

        try:
            await self.client.delete_cache(_TEST_CACHE_NAME)
        except errors.SdkError as e:
            print("error deleting cache")
            print(e)
        # ensure test cache exists
        try:
            await self.client.create_cache(_TEST_CACHE_NAME)
        except errors.AlreadyExistsError:
            # do nothing, cache already exists
            pass

        # Dynamically late-apply the decorator with a dynamically instantiated client instead of a static client
        # set fail_open to false so the test will fail if the client fails.
        @cached_async(client=self.client, cache=_TEST_CACHE_NAME, fail_open=False, is_class_method=True)
        async def slowly_double_method(self, string):
            # Load from a database or whatever you need to do.
            # Just doing a yield here to pretend like it's taking time.
            await asyncio.sleep(0.01)
            self.invocations.append(string)
            return string + string

        self.slowly_double = slowly_double_method

        TestCachedAsync.slowly_double_fn_cached = cached_async(client=self.client, cache=_TEST_CACHE_NAME, fail_open=False, is_class_method=False)(TestCachedAsync.slowly_double_fn)

    @classmethod
    async def asyncTearDown(self) -> None:
        # close client. Usually you'd expect people to
        # async with simple_cache_client.init(auth, ttl) as simple_cache:
        # and allow the scope manager to close the client.
        await self.client.__aexit__(None, None, None)

    @staticmethod
    async def slowly_double_fn(string):
        """Gets overridden during asyncSetUp()"""
        await asyncio.sleep(0.1)
        return string + string

    async def test_cached_method(self):
        v1 = await self.slowly_double("hello0")
        v2 = await self.slowly_double("hello0")
        v3 = await self.slowly_double("hello1")

        for i in range(10):
            # These will all be cached instead of loading slowly
            await self.slowly_double("hello" + str(i % 2))

        self.assertEqual("hello0hello0", v1, "it was supposed to double")
        self.assertEqual("hello0hello0", v2, "it was supposed to double")
        self.assertEqual("hello1hello1", v3, "it was supposed to double")

        self.assertEqual(self.invocations, ["hello0", "hello1"])

    async def test_cached_function(self):
        v1 = await TestCachedAsync.slowly_double_fn_cached("hello0")
        v2 = await TestCachedAsync.slowly_double_fn_cached("hello0")
        v3 = await TestCachedAsync.slowly_double_fn_cached("hello1")

        for i in range(10):
            await TestCachedAsync.slowly_double_fn_cached("hello" + str(i % 2))

        self.assertEqual("hello0hello0", v1, "it was supposed to double")
        self.assertEqual("hello0hello0", v2, "it was supposed to double")
        self.assertEqual("hello1hello1", v3, "it was supposed to double")


if __name__ == '__main__':
    unittest.main()

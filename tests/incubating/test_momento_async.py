import itertools
import os
import unittest
import warnings

from momento.cache_operation_types import CacheGetStatus
import momento.incubating.aio.simple_cache_client as simple_cache_client
from momento.incubating.aio.utils import convert_dict_items_to_bytes
from momento.vendor.python.unittest.async_case import IsolatedAsyncioTestCase

_AUTH_TOKEN = os.getenv('TEST_AUTH_TOKEN')
_TEST_CACHE_NAME = os.getenv('TEST_CACHE_NAME')
_DEFAULT_TTL_SECONDS = 60


class TestMomentoAsync(IsolatedAsyncioTestCase):
    async def test_incubating_warning(self):
        with self.assertWarns(UserWarning):
            warnings.simplefilter("always")
            async with simple_cache_client.init(_AUTH_TOKEN, _DEFAULT_TTL_SECONDS):
                pass

    async def test_get_dictionary_miss(self):
        async with simple_cache_client.init(_AUTH_TOKEN, _DEFAULT_TTL_SECONDS) as simple_cache:
            get_response = await simple_cache.dictionary_get(
                cache_name=_TEST_CACHE_NAME, dictionary_name="hello world", key="key")
            self.assertEqual(CacheGetStatus.MISS, get_response.status())

    async def test_dictionary_set_response(self):
        async with simple_cache_client.init(_AUTH_TOKEN, _DEFAULT_TTL_SECONDS) as simple_cache:
            # Test with key as string
            dictionary = {"key1": "value1"}
            set_response = await simple_cache.dictionary_set(
                cache_name=_TEST_CACHE_NAME, dictionary_name="myhash", dictionary=dictionary, refresh_ttl=False)
            self.assertEqual("myhash", set_response.key())
            self.assertEqual(dictionary, set_response.value())

            # Test key as bytes
            dictionary = dictionary={b"key1": "value1"}
            set_response = await simple_cache.dictionary_set(
                cache_name=_TEST_CACHE_NAME, dictionary_name="myhash2", dictionary=dictionary, refresh_ttl=False)
            self.assertEqual("myhash2", set_response.key())
            self.assertEqual(dictionary, set_response.value())

    async def test_dictionary_set_and_dictionary_get_missing_key(self):
        async with simple_cache_client.init(_AUTH_TOKEN, _DEFAULT_TTL_SECONDS) as simple_cache:
            await simple_cache.dictionary_set(
                cache_name=_TEST_CACHE_NAME, dictionary_name="myhash3", dictionary={"key1": "value1"}, refresh_ttl=False)
            get_response = await simple_cache.dictionary_get(
                cache_name=_TEST_CACHE_NAME, dictionary_name="myhash3", key="key2")
            self.assertEqual(CacheGetStatus.MISS, get_response.status())

    async def test_dictionary_get_hit(self):
        async with simple_cache_client.init(_AUTH_TOKEN, _DEFAULT_TTL_SECONDS) as simple_cache:
            # Test all combinations of type(key) in {str, bytes} and type(value) in {str, bytes}
            for i, (key_is_str, value_is_str) in enumerate(itertools.product((True, False), (True, False))):
                key, value = "key1", "value1"
                if not key_is_str:
                    key = key.encode()
                if not value_is_str:
                    value = value.encode()
                dictionary = {key: value}
                # Use distinct hash names to avoid collisions with already finished tests
                dictionary_name = f"myhash4-{i}"

                await simple_cache.dictionary_set(
                    cache_name=_TEST_CACHE_NAME, dictionary_name=dictionary_name, dictionary=dictionary, refresh_ttl=False)
                get_response = await simple_cache.dictionary_get(
                    cache_name=_TEST_CACHE_NAME, dictionary_name=dictionary_name, key=key)
                self.assertEqual(CacheGetStatus.HIT, get_response.status())
                self.assertEqual(value, get_response.value() if value_is_str else get_response.value_as_bytes())

    async def test_dictionary_get_all_miss(self):
        async with simple_cache_client.init(_AUTH_TOKEN, _DEFAULT_TTL_SECONDS) as simple_cache:
            get_response = await simple_cache.dictionary_get_all(
                cache_name=_TEST_CACHE_NAME, dictionary_name="myhash5")
            self.assertEqual(CacheGetStatus.MISS, get_response.status())

    async def test_dictionary_get_all_hit(self):
        async with simple_cache_client.init(_AUTH_TOKEN, _DEFAULT_TTL_SECONDS) as simple_cache:
            dictionary = {"key1": "value1", "key2": "value2"}
            await simple_cache.dictionary_set(
                cache_name=_TEST_CACHE_NAME, dictionary_name="myhash6", dictionary=dictionary, refresh_ttl=False
            )
            get_all_response = await simple_cache.dictionary_get_all(
                cache_name=_TEST_CACHE_NAME, dictionary_name="myhash6")
            self.assertEqual(CacheGetStatus.HIT, get_all_response.status())

            expected = convert_dict_items_to_bytes(dictionary)
            self.assertEqual(expected, get_all_response.value_as_bytes())

            expected = dictionary
            self.assertEqual(expected, get_all_response.value())



if __name__ == '__main__':
    unittest.main()

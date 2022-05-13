import itertools
import unittest
import os
import warnings

from momento.incubating.aio.utils import (
    convert_dict_values_to_bytes,
    dict_to_stored_hash)
import momento.incubating.simple_cache_client as simple_cache_client

from momento.cache_operation_types import CacheGetStatus
from momento.incubating.cache_operation_types import CacheDictionaryValue

_AUTH_TOKEN = os.getenv('TEST_AUTH_TOKEN')
_TEST_CACHE_NAME = os.getenv('TEST_CACHE_NAME')
_DEFAULT_TTL_SECONDS = 60


class TestMomento(unittest.TestCase):
    def test_incubating_warning(self):
        with self.assertWarns(UserWarning):
            warnings.simplefilter("always")
            with simple_cache_client.init(_AUTH_TOKEN, _DEFAULT_TTL_SECONDS):
                pass

    def test_get_dictionary_miss(self):
        with simple_cache_client.init(_AUTH_TOKEN, _DEFAULT_TTL_SECONDS) as simple_cache:
            get_response = simple_cache.dictionary_get(
                cache_name=_TEST_CACHE_NAME, dictionary_name="hello world", key="key")
            self.assertEqual(CacheGetStatus.MISS, get_response.status())

    def test_dictionary_set_response(self):
        with simple_cache_client.init(_AUTH_TOKEN, _DEFAULT_TTL_SECONDS) as simple_cache:
            # Test with key as string
            set_response = simple_cache.dictionary_set(
                cache_name=_TEST_CACHE_NAME, dictionary_name="myhash", dictionary={"key1": "value1"})
            self.assertEqual("myhash", set_response.key())
            self.assertEqual({"key1": CacheDictionaryValue(value=b"value1")}, set_response.value())

            # Test key as bytes
            set_response = simple_cache.dictionary_set(
                cache_name=_TEST_CACHE_NAME, dictionary_name="myhash2", dictionary={b"key1": "value1"})
            self.assertEqual("myhash2", set_response.key())
            self.assertEqual({b"key1": CacheDictionaryValue(value=b"value1")}, set_response.value())

    def test_dictionary_set_and_dictionary_get_missing_key(self):
        with simple_cache_client.init(_AUTH_TOKEN, _DEFAULT_TTL_SECONDS) as simple_cache:
            simple_cache.dictionary_set(
                cache_name=_TEST_CACHE_NAME, dictionary_name="myhash3", dictionary={"key1": "value1"})
            get_response = simple_cache.dictionary_get(
                cache_name=_TEST_CACHE_NAME, dictionary_name="myhash3", key="key2")
            self.assertEqual(CacheGetStatus.MISS, get_response.status())

    def test_dictionary_get_hit(self):
        with simple_cache_client.init(_AUTH_TOKEN, _DEFAULT_TTL_SECONDS) as simple_cache:
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

                simple_cache.dictionary_set(
                    cache_name=_TEST_CACHE_NAME, dictionary_name=dictionary_name, dictionary=dictionary)
                get_response = simple_cache.dictionary_get(
                    cache_name=_TEST_CACHE_NAME, dictionary_name=dictionary_name, key=key)
                self.assertEqual(CacheGetStatus.HIT, get_response.status())
                self.assertEqual(value, get_response.value() if value_is_str else get_response.value_as_bytes())

    def test_dictionary_get_all_miss(self):
        with simple_cache_client.init(_AUTH_TOKEN, _DEFAULT_TTL_SECONDS) as simple_cache:
            get_response = simple_cache.dictionary_get_all(
                cache_name=_TEST_CACHE_NAME, dictionary_name="myhash5")
            self.assertEqual(CacheGetStatus.MISS, get_response.status())

    def test_dictionary_get_all_hit(self):
        with simple_cache_client.init(_AUTH_TOKEN, _DEFAULT_TTL_SECONDS) as simple_cache:
            dictionary = {"key1": "value1", "key2": "value2"}
            simple_cache.dictionary_set(
                cache_name=_TEST_CACHE_NAME, dictionary_name="myhash6", dictionary=dictionary
            )
            get_all_response = simple_cache.dictionary_get_all(
                cache_name=_TEST_CACHE_NAME, dictionary_name="myhash6")
            self.assertEqual(CacheGetStatus.HIT, get_all_response.status())

            expected = dict_to_stored_hash(
                convert_dict_values_to_bytes(dictionary))
            self.assertEqual(expected, get_all_response.value())


if __name__ == '__main__':
    unittest.main()

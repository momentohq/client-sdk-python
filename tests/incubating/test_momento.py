import itertools
import unittest
import os
import warnings

from momento.incubating.aio.utils import convert_dict_items_to_bytes
import momento.incubating.simple_cache_client as simple_cache_client

from momento.cache_operation_types import CacheGetStatus
from momento.incubating.cache_operation_types import CacheDictionaryGetUnaryResponse

_AUTH_TOKEN = os.getenv("TEST_AUTH_TOKEN")
_TEST_CACHE_NAME = os.getenv("TEST_CACHE_NAME")
_DEFAULT_TTL_SECONDS = 60


class TestMomento(unittest.TestCase):
    def test_incubating_warning(self):
        with self.assertWarns(UserWarning):
            warnings.simplefilter("always")
            with simple_cache_client.init(_AUTH_TOKEN, _DEFAULT_TTL_SECONDS):
                pass

    def test_dictionary_get_miss(self):
        with simple_cache_client.init(
            _AUTH_TOKEN, _DEFAULT_TTL_SECONDS
        ) as simple_cache:
            get_response = simple_cache.dictionary_get(
                _TEST_CACHE_NAME, "hello world", "key"
            )
            self.assertEqual(CacheGetStatus.MISS, get_response.status())

    def test_dictionary_get_multi_miss(self):
        with simple_cache_client.init(
            _AUTH_TOKEN, _DEFAULT_TTL_SECONDS
        ) as simple_cache:
            get_response = simple_cache.dictionary_get(
                _TEST_CACHE_NAME, "hello world", "key1", "key2", "key3"
            )
            self.assertEqual(3, len(get_response.to_list()))
            self.assertTrue(
                all(result == CacheGetStatus.MISS for result in get_response.status())
            )
            self.assertEqual(3, len(get_response.values()))
            self.assertTrue(all(value is None for value in get_response.values()))
            self.assertEqual(3, len(get_response.values_as_bytes()))
            self.assertTrue(
                all(value is None for value in get_response.values_as_bytes())
            )

    def test_dictionary_set_response(self):
        with simple_cache_client.init(
            _AUTH_TOKEN, _DEFAULT_TTL_SECONDS
        ) as simple_cache:
            # Test with key as string
            dictionary = {"key1": "value1"}
            set_response = simple_cache.dictionary_multi_set(
                cache_name=_TEST_CACHE_NAME,
                dictionary_name="mydict",
                dictionary=dictionary,
                refresh_ttl=False,
            )
            self.assertEqual("mydict", set_response.dictionary_name())
            self.assertEqual(dictionary, set_response.dictionary())

            # Test as bytes
            dictionary = {b"key1": b"value1"}
            set_response = simple_cache.dictionary_multi_set(
                cache_name=_TEST_CACHE_NAME,
                dictionary_name="mydict2",
                dictionary=dictionary,
                refresh_ttl=False,
            )
            self.assertEqual("mydict2", set_response.dictionary_name())
            self.assertEqual(dictionary, set_response.dictionary_as_bytes())

    def test_dictionary_set_unary(self):
        with simple_cache_client.init(
            _AUTH_TOKEN, _DEFAULT_TTL_SECONDS
        ) as simple_cache:
            dictionary_name = "mydict20"
            key, value = "my-key", "my-value"
            set_response = simple_cache.dictionary_set(
                cache_name=_TEST_CACHE_NAME,
                dictionary_name=dictionary_name,
                key=key,
                value=value,
                refresh_ttl=False,
            )

            self.assertEqual(dictionary_name, set_response.dictionary_name())
            self.assertEqual(key, set_response.key())
            self.assertEqual(value, set_response.value())

            get_response = simple_cache.dictionary_get(
                _TEST_CACHE_NAME, dictionary_name, key
            )
            self.assertEqual(value, get_response.value())

    def test_dictionary_set_and_dictionary_get_missing_key(self):
        with simple_cache_client.init(
            _AUTH_TOKEN, _DEFAULT_TTL_SECONDS
        ) as simple_cache:
            simple_cache.dictionary_multi_set(
                cache_name=_TEST_CACHE_NAME,
                dictionary_name="mydict3",
                dictionary={"key1": "value1"},
                refresh_ttl=False,
            )
            get_response = simple_cache.dictionary_get(
                _TEST_CACHE_NAME, "mydict3", "key2"
            )
            self.assertEqual(CacheGetStatus.MISS, get_response.status())

    def test_dictionary_get_zero_length_keys(self):
        with simple_cache_client.init(
            _AUTH_TOKEN, _DEFAULT_TTL_SECONDS
        ) as simple_cache:
            with self.assertRaises(ValueError):
                simple_cache.dictionary_get(_TEST_CACHE_NAME, "my-dictionary", *[])

    def test_dictionary_get_hit(self):
        with simple_cache_client.init(
            _AUTH_TOKEN, _DEFAULT_TTL_SECONDS
        ) as simple_cache:
            # Test all combinations of type(key) in {str, bytes} and type(value) in {str, bytes}
            for i, (key_is_str, value_is_str) in enumerate(
                itertools.product((True, False), (True, False))
            ):
                key, value = "key1", "value1"
                if not key_is_str:
                    key = key.encode()
                if not value_is_str:
                    value = value.encode()
                dictionary = {key: value}
                # Use distinct hash names to avoid collisions with already finished tests
                dictionary_name = f"mydict4-{i}"

                simple_cache.dictionary_multi_set(
                    cache_name=_TEST_CACHE_NAME,
                    dictionary_name=dictionary_name,
                    dictionary=dictionary,
                    refresh_ttl=False,
                )
                get_response = simple_cache.dictionary_get(
                    _TEST_CACHE_NAME, dictionary_name, key
                )
                self.assertEqual(CacheGetStatus.HIT, get_response.status())
                self.assertEqual(
                    value,
                    get_response.value()
                    if value_is_str
                    else get_response.value_as_bytes(),
                )

    def test_dictionary_get_multi_hit(self):
        with simple_cache_client.init(
            _AUTH_TOKEN, _DEFAULT_TTL_SECONDS
        ) as simple_cache:
            dictionary_name = "mydict10"
            dictionary = {"key1": "value1", "key2": "value2", "key3": "value3"}

            simple_cache.dictionary_multi_set(
                cache_name=_TEST_CACHE_NAME,
                dictionary_name=dictionary_name,
                dictionary=dictionary,
                refresh_ttl=False,
            )
            get_response = simple_cache.dictionary_get(
                _TEST_CACHE_NAME, dictionary_name, "key1", "key2", "key3"
            )

            values = ["value1", "value2", "value3"]
            self.assertEqual(get_response.values(), values)
            self.assertEqual(
                get_response.values_as_bytes(), [b"value1", b"value2", b"value3"]
            )

            results = [CacheGetStatus.HIT] * 3
            self.assertTrue(get_response.status(), results)

            individual_responses = [
                CacheDictionaryGetUnaryResponse(value.encode("utf-8"), result)
                for value, result in zip(values, results)
            ]
            self.assertEqual(get_response.to_list(), individual_responses)

            get_response = simple_cache.dictionary_get(
                _TEST_CACHE_NAME, dictionary_name, "key1", "key2", "key5"
            )
            self.assertTrue(
                get_response.status(),
                [CacheGetStatus.HIT, CacheGetStatus.HIT, CacheGetStatus.MISS],
            )
            self.assertTrue(get_response.values(), ["value1", "value2", None])
            self.assertTrue(
                get_response.values_as_bytes(), [b"value1", b"value2", None]
            )

    def test_dictionary_get_all_miss(self):
        with simple_cache_client.init(
            _AUTH_TOKEN, _DEFAULT_TTL_SECONDS
        ) as simple_cache:
            get_response = simple_cache.dictionary_get_all(
                cache_name=_TEST_CACHE_NAME, dictionary_name="mydict5"
            )
            self.assertEqual(CacheGetStatus.MISS, get_response.status())

    def test_dictionary_get_all_hit(self):
        with simple_cache_client.init(
            _AUTH_TOKEN, _DEFAULT_TTL_SECONDS
        ) as simple_cache:
            dictionary = {"key1": "value1", "key2": "value2"}
            simple_cache.dictionary_multi_set(
                cache_name=_TEST_CACHE_NAME,
                dictionary_name="mydict6",
                dictionary=dictionary,
                refresh_ttl=False,
            )
            get_all_response = simple_cache.dictionary_get_all(
                cache_name=_TEST_CACHE_NAME, dictionary_name="mydict6"
            )
            self.assertEqual(CacheGetStatus.HIT, get_all_response.status())

            expected = convert_dict_items_to_bytes(dictionary)
            self.assertEqual(expected, get_all_response.value_as_bytes())

            expected = dictionary
            self.assertEqual(expected, get_all_response.value())


if __name__ == "__main__":
    unittest.main()

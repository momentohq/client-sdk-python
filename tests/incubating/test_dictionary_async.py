import itertools
import os
import unittest
import warnings

from momento.cache_operation_types import CacheGetStatus
from momento.incubating.cache_operation_types import CacheDictionaryGetUnaryResponse
import momento.incubating.aio.simple_cache_client as simple_cache_client
from momento.incubating.aio.utils import convert_dict_items_to_bytes
from momento.vendor.python.unittest.async_case import IsolatedAsyncioTestCase
from tests.utils import uuid_str, uuid_bytes, str_to_bytes

_AUTH_TOKEN = os.getenv("TEST_AUTH_TOKEN")
_TEST_CACHE_NAME = os.getenv("TEST_CACHE_NAME")
_DEFAULT_TTL_SECONDS = 60


class TestMomentoAsync(IsolatedAsyncioTestCase):
    async def test_incubating_warning(self):
        with self.assertWarns(UserWarning):
            warnings.simplefilter("always")
            async with simple_cache_client.init(_AUTH_TOKEN, _DEFAULT_TTL_SECONDS):
                pass

    async def test_dictionary_get_miss(self):
        async with simple_cache_client.init(
            _AUTH_TOKEN, _DEFAULT_TTL_SECONDS
        ) as simple_cache:
            get_response = await simple_cache.dictionary_get(
                cache_name=_TEST_CACHE_NAME, dictionary_name=uuid_str(), key=uuid_str()
            )
            self.assertEqual(CacheGetStatus.MISS, get_response.status())

    async def test_dictionary_get_multi_miss(self):
        async with simple_cache_client.init(
            _AUTH_TOKEN, _DEFAULT_TTL_SECONDS
        ) as simple_cache:
            get_response = await simple_cache.dictionary_get_multi(
                _TEST_CACHE_NAME, uuid_str(), uuid_str(), uuid_str(), uuid_str()
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

    async def test_dictionary_set_response(self):
        async with simple_cache_client.init(
            _AUTH_TOKEN, _DEFAULT_TTL_SECONDS
        ) as simple_cache:
            # Test with key as string
            dictionary = {uuid_str(): uuid_str()}
            dictionary_name = uuid_str()
            set_response = await simple_cache.dictionary_set_multi(
                cache_name=_TEST_CACHE_NAME,
                dictionary_name=dictionary_name,
                dictionary=dictionary,
                refresh_ttl=False,
            )
            self.assertEqual(dictionary_name, set_response.dictionary_name())
            self.assertEqual(dictionary, set_response.dictionary())

            # Test as bytes
            dictionary = {uuid_bytes(): uuid_bytes()}
            dictionary_name = uuid_str()
            set_response = await simple_cache.dictionary_set_multi(
                cache_name=_TEST_CACHE_NAME,
                dictionary_name=dictionary_name,
                dictionary=dictionary,
                refresh_ttl=False,
            )
            self.assertEqual(dictionary_name, set_response.dictionary_name())
            self.assertEqual(dictionary, set_response.dictionary_as_bytes())

    async def test_dictionary_set_unary(self):
        async with simple_cache_client.init(
            _AUTH_TOKEN, _DEFAULT_TTL_SECONDS
        ) as simple_cache:
            dictionary_name = uuid_str()
            key, value = uuid_str(), uuid_str()
            set_response = await simple_cache.dictionary_set(
                cache_name=_TEST_CACHE_NAME,
                dictionary_name=dictionary_name,
                key=key,
                value=value,
                refresh_ttl=False,
            )

            self.assertEqual(dictionary_name, set_response.dictionary_name())
            self.assertEqual(key, set_response.key())
            self.assertEqual(value, set_response.value())

            get_response = await simple_cache.dictionary_get(
                cache_name=_TEST_CACHE_NAME, dictionary_name=dictionary_name, key=key
            )
            self.assertEqual(value, get_response.value())

    async def test_dictionary_set_and_dictionary_get_missing_key(self):
        async with simple_cache_client.init(
            _AUTH_TOKEN, _DEFAULT_TTL_SECONDS
        ) as simple_cache:
            dictionary_name = uuid_str()
            await simple_cache.dictionary_set(
                cache_name=_TEST_CACHE_NAME,
                dictionary_name=dictionary_name,
                key=uuid_str(),
                value=uuid_str(),
                refresh_ttl=False,
            )
            get_response = await simple_cache.dictionary_get(
                cache_name=_TEST_CACHE_NAME,
                dictionary_name=dictionary_name,
                key=uuid_str(),
            )
            self.assertEqual(CacheGetStatus.MISS, get_response.status())

    async def test_dictionary_get_zero_length_keys(self):
        async with simple_cache_client.init(
            _AUTH_TOKEN, _DEFAULT_TTL_SECONDS
        ) as simple_cache:
            with self.assertRaises(ValueError):
                await simple_cache.dictionary_get_multi(
                    _TEST_CACHE_NAME, uuid_str(), *[]
                )

    async def test_dictionary_get_hit(self):
        async with simple_cache_client.init(
            _AUTH_TOKEN, _DEFAULT_TTL_SECONDS
        ) as simple_cache:
            # Test all combinations of type(key) in {str, bytes} and type(value) in {str, bytes}
            for i, (key_is_str, value_is_str) in enumerate(
                itertools.product((True, False), (True, False))
            ):
                key, value = uuid_str(), uuid_str()
                if not key_is_str:
                    key = key.encode()
                if not value_is_str:
                    value = value.encode()
                dictionary = {key: value}
                dictionary_name = uuid_str()
                await simple_cache.dictionary_set_multi(
                    cache_name=_TEST_CACHE_NAME,
                    dictionary_name=dictionary_name,
                    dictionary=dictionary,
                    refresh_ttl=False,
                )
                get_response = await simple_cache.dictionary_get(
                    cache_name=_TEST_CACHE_NAME,
                    dictionary_name=dictionary_name,
                    key=key,
                )
                self.assertEqual(CacheGetStatus.HIT, get_response.status())
                self.assertEqual(
                    value,
                    get_response.value()
                    if value_is_str
                    else get_response.value_as_bytes(),
                )

    async def test_dictionary_get_multi_hit(self):
        async with simple_cache_client.init(
            _AUTH_TOKEN, _DEFAULT_TTL_SECONDS
        ) as simple_cache:
            dictionary_name = uuid_str()
            keys = [uuid_str() for _ in range(3)]
            values = [uuid_str() for _ in range(3)]
            dictionary = {k: v for k, v in zip(keys, values)}

            await simple_cache.dictionary_set_multi(
                cache_name=_TEST_CACHE_NAME,
                dictionary_name=dictionary_name,
                dictionary=dictionary,
                refresh_ttl=False,
            )
            get_response = await simple_cache.dictionary_get_multi(
                _TEST_CACHE_NAME, dictionary_name, *keys
            )

            self.assertEqual(get_response.values(), values)
            self.assertEqual(
                get_response.values_as_bytes(), [str_to_bytes(i) for i in values]
            )

            results = [CacheGetStatus.HIT] * 3
            self.assertTrue(get_response.status(), results)

            individual_responses = [
                CacheDictionaryGetUnaryResponse(value.encode("utf-8"), result)
                for value, result in zip(values, results)
            ]
            self.assertEqual(get_response.to_list(), individual_responses)

            get_response = await simple_cache.dictionary_get_multi(
                _TEST_CACHE_NAME, dictionary_name, keys[0], keys[1], uuid_str()
            )
            self.assertTrue(
                get_response.status(),
                [CacheGetStatus.HIT, CacheGetStatus.HIT, CacheGetStatus.MISS],
            )
            self.assertTrue(get_response.values(), [values[0], values[1], None])
            self.assertTrue(
                get_response.values_as_bytes(),
                [str_to_bytes(values[0]), str_to_bytes(values[1]), None],
            )

    async def test_dictionary_get_all_miss(self):
        async with simple_cache_client.init(
            _AUTH_TOKEN, _DEFAULT_TTL_SECONDS
        ) as simple_cache:
            get_response = await simple_cache.dictionary_get_all(
                cache_name=_TEST_CACHE_NAME, dictionary_name=uuid_str()
            )
            self.assertEqual(CacheGetStatus.MISS, get_response.status())

    async def test_dictionary_get_all_hit(self):
        async with simple_cache_client.init(
            _AUTH_TOKEN, _DEFAULT_TTL_SECONDS
        ) as simple_cache:
            dictionary_name = uuid_str()
            dictionary = {uuid_str(): uuid_str(), uuid_str(): uuid_str()}
            await simple_cache.dictionary_set_multi(
                cache_name=_TEST_CACHE_NAME,
                dictionary_name=dictionary_name,
                dictionary=dictionary,
                refresh_ttl=False,
            )
            get_all_response = await simple_cache.dictionary_get_all(
                cache_name=_TEST_CACHE_NAME, dictionary_name=dictionary_name
            )
            self.assertEqual(CacheGetStatus.HIT, get_all_response.status())

            expected = convert_dict_items_to_bytes(dictionary)
            self.assertEqual(expected, get_all_response.value_as_bytes())

            expected = dictionary
            self.assertEqual(expected, get_all_response.value())


if __name__ == "__main__":
    unittest.main()

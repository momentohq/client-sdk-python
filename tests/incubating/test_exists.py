import os
import unittest

import momento.incubating.simple_cache_client as simple_cache_client
from tests.utils import uuid_str

_AUTH_TOKEN = os.getenv("TEST_AUTH_TOKEN")
_TEST_CACHE_NAME = os.getenv("TEST_CACHE_NAME")
_DEFAULT_TTL_SECONDS = 60


class TestMomento(unittest.TestCase):
    def test_exists_unary_missing(self):
        with simple_cache_client.init(
            _AUTH_TOKEN, _DEFAULT_TTL_SECONDS
        ) as simple_cache:
            key = uuid_str()
            response = simple_cache.exists(_TEST_CACHE_NAME, key)

            self.assertFalse(response)
            self.assertEqual(0, response.num_exists())
            self.assertEqual([False], response.results())
            self.assertEqual([key], response.missing_keys())
            self.assertEqual([], response.present_keys())
            self.assertEqual(
                [(key, False)], list(response.zip_keys_and_results())
            )

    def test_exists_unary_exists(self):
        with simple_cache_client.init(
            _AUTH_TOKEN, _DEFAULT_TTL_SECONDS
        ) as simple_cache:
            key, value = uuid_str(), uuid_str()
            simple_cache.set(_TEST_CACHE_NAME, key, value)

            response = simple_cache.exists(_TEST_CACHE_NAME, key)

            self.assertTrue(response)
            self.assertEqual(1, response.num_exists())
            self.assertEqual([True], response.results())
            self.assertEqual([], response.missing_keys())
            self.assertEqual([key], response.present_keys())
            self.assertEqual(
                [(key, True)], list(response.zip_keys_and_results())
            )

    def test_exists_multi(self):
        with simple_cache_client.init(
            _AUTH_TOKEN, _DEFAULT_TTL_SECONDS
        ) as simple_cache:
            keys = []
            for i in range(3):
                key = uuid_str()
                simple_cache.set(_TEST_CACHE_NAME, key, uuid_str())
                keys.append(key)

            response = simple_cache.exists(_TEST_CACHE_NAME, *keys)
            self.assertTrue(response)
            self.assertTrue(response.all())
            self.assertEqual(3, response.num_exists())
            self.assertEqual([True] * 3, response.results())
            self.assertEqual([], response.missing_keys())
            self.assertEqual(keys, response.present_keys())
            self.assertEqual(
                list(zip(keys, [True] * 3)), list(response.zip_keys_and_results())
            )

            missing1, missing2 = uuid_str(), uuid_str()
            more_keys = [missing1] + keys + [missing2]
            response = simple_cache.exists(_TEST_CACHE_NAME, *more_keys)
            self.assertFalse(response)
            self.assertFalse(response.all())
            self.assertEqual(3, response.num_exists())
            mask = [False] + [True] * 3 + [False]
            self.assertEqual(mask, response.results())

            self.assertEqual([missing1, missing2], response.missing_keys())
            self.assertEqual(keys, response.present_keys())
            self.assertEqual(
                list(zip(more_keys, mask)), list(response.zip_keys_and_results())
            )


if __name__ == "__main__":
    unittest.main()

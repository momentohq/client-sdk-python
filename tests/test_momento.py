import unittest
import os
import uuid

import momento.simple_cache_client as simple_cache_client
from momento.cache_operation_responses import CacheResult

_AUTH_TOKEN = os.getenv('TEST_AUTH_TOKEN')
_TEST_CACHE_NAME = os.getenv('TEST_CACHE_NAME')
_DEFAULT_TTL_SECONDS = 60


class TestMomento(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not _AUTH_TOKEN:
            raise RuntimeError(
                "Integration tests require TEST_AUTH_TOKEN env var; see README for more details."
            )
        if not _TEST_CACHE_NAME:
            raise RuntimeError(
                "Integration tests require TEST_CACHE_NAME env var; see README for more details."
            )

    def test_happy_path(self):
        key = str(uuid.uuid4())
        value = str(uuid.uuid4())

        with simple_cache_client.get(_AUTH_TOKEN,
                                     _DEFAULT_TTL_SECONDS) as simple_cache:
            simple_cache.set(_TEST_CACHE_NAME, key, value)
            get_resp = simple_cache.get(_TEST_CACHE_NAME, key)

            self.assertEqual(get_resp.result(), CacheResult.HIT)
            self.assertEqual(get_resp.str_utf8(), value)


if __name__ == '__main__':
    unittest.main()

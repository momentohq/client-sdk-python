import unittest
import os
import uuid

import momento.momento as momento
from momento.cache_operation_responses import CacheResult


_AUTH_TOKEN = os.getenv('TEST_AUTH_TOKEN')
_TEST_CACHE_NAME = os.getenv('TEST_CACHE_NAME')
_DEFAULT_TTL_SECONDS = 60

class TestMomento(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        if not _AUTH_TOKEN:
            raise RuntimeError("Integration tests require TEST_AUTH_TOKEN env var; see README for more details.")
        if not _TEST_CACHE_NAME:
            raise RuntimeError("Integration tests require TEST_CACHE_NAME env var; see README for more details.")

    def test_happy_path(self):
        key = str(uuid.uuid4())
        value = str(uuid.uuid4())

        with momento.init(_AUTH_TOKEN) as momento_client:
            with momento_client.get_cache(_TEST_CACHE_NAME,
                    ttl_seconds=_DEFAULT_TTL_SECONDS, create_if_absent=True) as cache_client:

               cache_client.set(key, value)

               get_resp = cache_client.get(key)

               self.assertEqual(get_resp.result(), CacheResult.HIT)
               self.assertEqual(get_resp.str_utf8(), value)


if __name__ == '__main__':
    unittest.main()

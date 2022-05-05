import itertools
import os
import time
import unittest
import uuid

import momento.aio.simple_cache_client as simple_cache_client
import momento.errors as errors
from momento.cache_operation_types import (
    CacheGetStatus,
    CacheMultiSetOperation,
    CacheMultiGetOperation,
    CacheHashGetStatus,
    CacheHashValue)
from momento.vendor.python.unittest.async_case import IsolatedAsyncioTestCase

_AUTH_TOKEN = os.getenv('TEST_AUTH_TOKEN')
_TEST_CACHE_NAME = os.getenv('TEST_CACHE_NAME')
_BAD_AUTH_TOKEN = "eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJpbnRlZ3JhdGlvbiIsImNwIjoiY29udHJvbC5jZWxsLWFscGhhLWRldi5wcmVwcm9kLmEubW9tZW50b2hxLmNvbSIsImMiOiJjYWNoZS5jZWxsLWFscGhhLWRldi5wcmVwcm9kLmEubW9tZW50b2hxLmNvbSJ9.gdghdjjfjyehhdkkkskskmmls76573jnajhjjjhjdhnndy"
_DEFAULT_TTL_SECONDS = 60


class TestMomentoAsync(IsolatedAsyncioTestCase):
    @classmethod
    async def asyncSetUp(self) -> None:
        if not _AUTH_TOKEN:
            raise RuntimeError(
                "Integration tests require TEST_AUTH_TOKEN env var; see README for more details."
            )
        if not _TEST_CACHE_NAME:
            raise RuntimeError(
                "Integration tests require TEST_CACHE_NAME env var; see README for more details."
            )

        # default client for use in tests
        self.client = simple_cache_client.init(_AUTH_TOKEN, _DEFAULT_TTL_SECONDS)
        await self.client.__aenter__()

        # ensure test cache exists
        try:
            await self.client.create_cache(_TEST_CACHE_NAME)
        except errors.AlreadyExistsError:
            # do nothing, cache already exists
            pass

    @classmethod
    async def asyncTearDown(self) -> None:
        # close client. Usually you'd expect people to
        # async with simple_cache_client.init(auth, ttl) as simple_cache:
        # and allow the scope manager to close the client.
        await self.client.__aexit__(None, None, None)

    # basic happy path test
    async def test_create_cache_get_set_values_and_delete_cache(self):
        cache_name = str(uuid.uuid4())
        key = str(uuid.uuid4())
        value = str(uuid.uuid4())

        await self.client.create_cache(cache_name)

        set_resp = await self.client.set(cache_name, key, value)
        self.assertEqual(set_resp.value(), value)

        get_resp = await self.client.get(cache_name, key)
        self.assertEqual(get_resp.status(), CacheGetStatus.HIT)
        self.assertEqual(get_resp.value(), value)

        get_for_key_in_some_other_cache = await self.client.get(_TEST_CACHE_NAME, key)
        self.assertEqual(get_for_key_in_some_other_cache.status(), CacheGetStatus.MISS)

        await self.client.delete_cache(cache_name)

    # init

    async def test_init_throws_exception_when_client_uses_negative_default_ttl(self):
        with self.assertRaises(errors.InvalidArgumentError) as cm:
            simple_cache_client.init(_AUTH_TOKEN, -1)
        self.assertEqual('{}'.format(cm.exception), "TTL Seconds must be a non-negative integer")

    async def test_init_throws_exception_for_non_jwt_token(self):
        with self.assertRaises(errors.InvalidArgumentError) as cm:
            simple_cache_client.init("notanauthtoken", _DEFAULT_TTL_SECONDS)
        self.assertEqual('{}'.format(cm.exception), "Invalid Auth token.")

    async def test_init_throws_exception_when_client_uses_negative_request_timeout_ms(self):
        with self.assertRaises(errors.InvalidArgumentError) as cm:
            simple_cache_client.init(_AUTH_TOKEN, _DEFAULT_TTL_SECONDS, -1)
        self.assertEqual('{}'.format(cm.exception), "Request timeout must be greater than zero.")

    async def test_init_throws_exception_when_client_uses_zero_request_timeout_ms(self):
        with self.assertRaises(errors.InvalidArgumentError) as cm:
            simple_cache_client.init(_AUTH_TOKEN, _DEFAULT_TTL_SECONDS, 0)
        self.assertEqual('{}'.format(cm.exception), "Request timeout must be greater than zero.")

    # create_cache

    async def test_create_cache_throws_already_exists_when_creating_existing_cache(self):
        with self.assertRaises(errors.AlreadyExistsError):
            await self.client.create_cache(_TEST_CACHE_NAME)

    async def test_create_cache_throws_exception_for_empty_cache_name(self):
        with self.assertRaises(errors.BadRequestError):
            await self.client.create_cache("")

    async def test_create_cache_throws_validation_exception_for_null_cache_name(self):
        with self.assertRaises(errors.InvalidArgumentError) as cm:
            await self.client.create_cache(None)
        self.assertEqual('{}'.format(cm.exception), "Cache name must be a non-empty string")

    async def test_create_cache_with_bad_cache_name_throws_exception(self):
        with self.assertRaises(errors.InvalidArgumentError) as cm:
            await self.client.create_cache(1)
        self.assertEqual('{}'.format(cm.exception),
                         "Cache name must be a non-empty string")

    async def test_create_cache_throws_authentication_exception_for_bad_token(self):
        async with simple_cache_client.init(_BAD_AUTH_TOKEN, _DEFAULT_TTL_SECONDS) as simple_cache:
            with self.assertRaises(errors.AuthenticationError):
                await simple_cache.create_cache(str(uuid.uuid4()))

    # delete_cache
    async def test_delete_cache_succeeds(self):
        cache_name = str(uuid.uuid4())

        await self.client.create_cache(cache_name)
        with self.assertRaises(errors.AlreadyExistsError):
            await self.client.create_cache(cache_name)
        await self.client.delete_cache(cache_name)
        with self.assertRaises(errors.NotFoundError):
            await self.client.delete_cache(cache_name)

    async def test_delete_cache_throws_not_found_when_deleting_unknown_cache(self):
        cache_name = str(uuid.uuid4())
        with self.assertRaises(errors.NotFoundError):
            await self.client.delete_cache(cache_name)

    async def test_delete_cache_throws_invalid_input_for_null_cache_name(self):
        with self.assertRaises(errors.InvalidArgumentError):
            await self.client.delete_cache(None)

    async def test_delete_cache_throws_exception_for_empty_cache_name(self):
        with self.assertRaises(errors.BadRequestError):
            await self.client.delete_cache("")

    async def test_delete_with_bad_cache_name_throws_exception(self):
        with self.assertRaises(errors.InvalidArgumentError) as cm:
            await self.client.delete_cache(1)
        self.assertEqual('{}'.format(cm.exception), "Cache name must be a non-empty string")

    async def test_delete_cache_throws_authentication_exception_for_bad_token(self):
        async with simple_cache_client.init(_BAD_AUTH_TOKEN, _DEFAULT_TTL_SECONDS) as simple_cache:
            with self.assertRaises(errors.AuthenticationError):
                await simple_cache.create_cache(str(uuid.uuid4()))

    # list_caches

    async def test_list_caches_succeeds(self):
        cache_name = str(uuid.uuid4())

        caches = (await self.client.list_caches()).caches()
        cache_names = [cache.name() for cache in caches]
        self.assertNotIn(cache_name, cache_names)

        try:
            await self.client.create_cache(cache_name)

            list_cache_resp = await self.client.list_caches()
            caches = list_cache_resp.caches()
            cache_names = [cache.name() for cache in caches]
            self.assertIn(cache_name, cache_names)
            self.assertIsNone(list_cache_resp.next_token())
        finally:
            await self.client.delete_cache(cache_name)

    async def test_list_caches_throws_authentication_exception_for_bad_token(self):
        async with simple_cache_client.init(_BAD_AUTH_TOKEN, _DEFAULT_TTL_SECONDS) as simple_cache:
            with self.assertRaises(errors.AuthenticationError):
                await simple_cache.list_caches()

    async def test_list_caches_with_next_token_works(self):
        # skip until pagination is actually implemented, see
        # https://github.com/momentohq/control-plane-service/issues/83
        self.skipTest("pagination not yet implemented")
    
    # signing keys
    async def test_create_list_revoke_signing_keys(self):
        create_resp = await self.client.create_signing_key(30)
        list_resp = await self.client.list_signing_keys()
        self.assertTrue(create_resp.key_id() in [signing_key.key_id() for signing_key in list_resp.signing_keys()])
        await self.client.revoke_signing_key(create_resp.key_id())
        list_resp = await self.client.list_signing_keys()
        self.assertFalse(create_resp.key_id() in [signing_key.key_id() for signing_key in list_resp.signing_keys()])

    # setting and getting

    async def test_set_and_get_with_hit(self):
        key = str(uuid.uuid4())
        value = str(uuid.uuid4())

        set_resp = await self.client.set(_TEST_CACHE_NAME, key, value)
        self.assertEqual(set_resp.value(), value)
        self.assertEqual(set_resp.value_as_bytes(), bytes(value, 'utf-8'))

        get_resp = await self.client.get(_TEST_CACHE_NAME, key)
        self.assertEqual(get_resp.status(), CacheGetStatus.HIT)
        self.assertEqual(get_resp.value(), value)
        self.assertEqual(get_resp.value_as_bytes(), bytes(value, 'utf-8'))

    async def test_set_and_get_with_byte_key_values(self):
        key = uuid.uuid4().bytes
        value = uuid.uuid4().bytes

        set_resp = await self.client.set(_TEST_CACHE_NAME, key, value)
        self.assertEqual(set_resp.value_as_bytes(), value)

        get_resp = await self.client.get(_TEST_CACHE_NAME, key)
        self.assertEqual(get_resp.status(), CacheGetStatus.HIT)
        self.assertEqual(get_resp.value_as_bytes(), value)

    async def test_get_returns_miss(self):
        key = str(uuid.uuid4())

        get_resp = await self.client.get(_TEST_CACHE_NAME, key)
        self.assertEqual(get_resp.status(), CacheGetStatus.MISS)
        self.assertEqual(get_resp.value_as_bytes(), None)
        self.assertEqual(get_resp.value(), None)

    async def test_expires_items_after_ttl(self):
        key = str(uuid.uuid4())
        val = str(uuid.uuid4())
        async with simple_cache_client.init(_AUTH_TOKEN, 2) as simple_cache:
            await simple_cache.set(_TEST_CACHE_NAME, key, val)

            self.assertEqual((await simple_cache.get(_TEST_CACHE_NAME, key)).status(), CacheGetStatus.HIT)

            time.sleep(4)
            self.assertEqual((await simple_cache.get(_TEST_CACHE_NAME, key)).status(), CacheGetStatus.MISS)

    async def test_set_with_different_ttl(self):
        key1 = str(uuid.uuid4())
        key2 = str(uuid.uuid4())

        await self.client.set(_TEST_CACHE_NAME, key1, "1", 2)
        await self.client.set(_TEST_CACHE_NAME, key2, "2")

        self.assertEqual((await self.client.get(_TEST_CACHE_NAME, key1)).status(), CacheGetStatus.HIT)
        self.assertEqual((await self.client.get(_TEST_CACHE_NAME, key2)).status(), CacheGetStatus.HIT)

        time.sleep(4)
        self.assertEqual((await self.client.get(_TEST_CACHE_NAME, key1)).status(), CacheGetStatus.MISS)
        self.assertEqual((await self.client.get(_TEST_CACHE_NAME, key2)).status(), CacheGetStatus.HIT)

    # set

    async def test_set_with_non_existent_cache_name_throws_not_found(self):
        cache_name = str(uuid.uuid4())
        with self.assertRaises(errors.NotFoundError):
            await self.client.set(cache_name, "foo", "bar")

    async def test_set_with_null_cache_name_throws_exception(self):
        cache_name = str(uuid.uuid4())
        with self.assertRaises(errors.InvalidArgumentError) as cm:
            await self.client.set(None, "foo", "bar")
        self.assertEqual('{}'.format(cm.exception), "Cache name must be a non-empty string")

    async def test_set_with_empty_cache_name_throws_exception(self):
        cache_name = str(uuid.uuid4())
        with self.assertRaises(errors.BadRequestError) as cm:
            await self.client.set("", "foo", "bar")
        self.assertEqual('{}'.format(cm.exception), "Cache header is empty")

    async def test_set_with_null_key_throws_exception(self):
        with self.assertRaises(errors.InvalidArgumentError):
            await self.client.set(_TEST_CACHE_NAME, None, "bar")

    async def test_set_with_null_value_throws_exception(self):
        with self.assertRaises(errors.InvalidArgumentError):
            await self.client.set(_TEST_CACHE_NAME, "foo", None)

    async def test_set_negative_ttl_throws_exception(self):
        with self.assertRaises(errors.InvalidArgumentError) as cm:
            await self.client.set(_TEST_CACHE_NAME, "foo", "bar", -1)
        self.assertEqual('{}'.format(cm.exception), "TTL Seconds must be a non-negative integer")

    async def test_set_with_bad_cache_name_throws_exception(self):
        with self.assertRaises(errors.InvalidArgumentError) as cm:
            await self.client.set(1, "foo", "bar")
        self.assertEqual('{}'.format(cm.exception), "Cache name must be a non-empty string")

    async def test_set_with_bad_key_throws_exception(self):
        with self.assertRaises(errors.InvalidArgumentError) as cm:
            await self.client.set(_TEST_CACHE_NAME, 1, "bar")
        self.assertEqual('{}'.format(cm.exception), "Unsupported type for key: <class 'int'>")

    async def test_set_with_bad_value_throws_exception(self):
        with self.assertRaises(errors.InvalidArgumentError) as cm:
            await self.client.set(_TEST_CACHE_NAME, "foo", 1)
        self.assertEqual('{}'.format(cm.exception), "Unsupported type for value: <class 'int'>")

    async def test_set_throws_authentication_exception_for_bad_token(self):
        async with simple_cache_client.init(_BAD_AUTH_TOKEN, _DEFAULT_TTL_SECONDS) as simple_cache:
            with self.assertRaises(errors.AuthenticationError):
                await simple_cache.set(_TEST_CACHE_NAME, "foo", "bar")

    async def test_set_throws_timeout_error_for_short_request_timeout(self):
        async with simple_cache_client.init(_AUTH_TOKEN, _DEFAULT_TTL_SECONDS, request_timeout_ms=1) as simple_cache:
            with self.assertRaises(errors.TimeoutError):
                await simple_cache.set(_TEST_CACHE_NAME, "foo", "bar")

    # get

    async def test_get_with_non_existent_cache_name_throws_not_found(self):
        cache_name = str(uuid.uuid4())
        with self.assertRaises(errors.NotFoundError):
            await self.client.get(cache_name, "foo")

    async def test_get_with_null_cache_name_throws_exception(self):
        cache_name = str(uuid.uuid4())
        with self.assertRaises(errors.InvalidArgumentError) as cm:
            await self.client.get(None, "foo")
        self.assertEqual('{}'.format(cm.exception), "Cache name must be a non-empty string")

    async def test_get_with_empty_cache_name_throws_exception(self):
        cache_name = str(uuid.uuid4())
        with self.assertRaises(errors.BadRequestError) as cm:
            await self.client.get("", "foo")
        self.assertEqual('{}'.format(cm.exception), "Cache header is empty")

    async def test_get_with_null_key_throws_exception(self):
        with self.assertRaises(errors.InvalidArgumentError):
            await self.client.get(_TEST_CACHE_NAME, None)

    async def test_get_with_bad_cache_name_throws_exception(self):
        with self.assertRaises(errors.InvalidArgumentError) as cm:
            await self.client.get(1, "foo")
        self.assertEqual('{}'.format(cm.exception), "Cache name must be a non-empty string")

    async def test_get_with_bad_key_throws_exception(self):
        with self.assertRaises(errors.InvalidArgumentError) as cm:
            await self.client.get(_TEST_CACHE_NAME, 1)
        self.assertEqual('{}'.format(cm.exception), "Unsupported type for key: <class 'int'>")

    async def test_get_throws_authentication_exception_for_bad_token(self):
        async with simple_cache_client.init(_BAD_AUTH_TOKEN, _DEFAULT_TTL_SECONDS) as simple_cache:
            with self.assertRaises(errors.AuthenticationError):
                await simple_cache.get(_TEST_CACHE_NAME, "foo")

    async def test_get_throws_timeout_error_for_short_request_timeout(self):
        async with simple_cache_client.init(_AUTH_TOKEN, _DEFAULT_TTL_SECONDS, request_timeout_ms=1) as simple_cache:
            with self.assertRaises(errors.TimeoutError):
                await simple_cache.get(_TEST_CACHE_NAME, "foo")

    # Multi op tests
    async def test_multi_get_and_set(self):
        set_resp = await self.client.multi_set(
            cache_name=_TEST_CACHE_NAME,
            ops=[
                CacheMultiSetOperation(key="foo1", value="bar1"),
                CacheMultiSetOperation(key="foo2", value="bar2"),
                CacheMultiSetOperation(key="foo3", value="bar3"),
                CacheMultiSetOperation(key="foo4", value="bar4"),
                CacheMultiSetOperation(key="foo5", value="bar5"),
            ]
        )
        self.assertEqual(0, len(set_resp.get_failed_responses()))
        self.assertEqual(5, len(set_resp.get_successful_responses()))
        get_resp = await self.client.multi_get(
            cache_name=_TEST_CACHE_NAME,
            ops=[
                CacheMultiGetOperation(key="foo5"),
                CacheMultiGetOperation(key="foo1"),
                CacheMultiGetOperation(key="foo2"),
                CacheMultiGetOperation(key="foo3")
            ]
        )
        self.assertEqual("bar5", get_resp.values()[0])
        self.assertEqual("bar1", get_resp.values()[1])
        self.assertEqual("bar2", get_resp.values()[2])
        self.assertEqual("bar3", get_resp.values()[3])

    # Multi op failure retry test
    async def test_multi_set_failure_retry(self):

        # Start with a cache client with impossibly small request timeout to force failures
        async with simple_cache_client.init(_AUTH_TOKEN, _DEFAULT_TTL_SECONDS, request_timeout_ms=1) as simple_cache:
            set_resp = await simple_cache.multi_set(
                cache_name=_TEST_CACHE_NAME,
                ops=[
                    CacheMultiSetOperation(key="fizz1", value="buzz1"),
                    CacheMultiSetOperation(key="fizz2", value="buzz2"),
                    CacheMultiSetOperation(key="fizz3", value="buzz3"),
                    CacheMultiSetOperation(key="fizz4", value="buzz4"),
                    CacheMultiSetOperation(key="fizz5", value="buzz5"),
                ]
            )
            get_resp = await simple_cache.multi_get(
                cache_name=_TEST_CACHE_NAME,
                ops=[
                    CacheMultiGetOperation(key="fizz4"),
                    CacheMultiGetOperation(key="fizz5")
                ]
            )

            self.assertEqual(0, len(set_resp.get_successful_responses()))
            self.assertEqual(5, len(set_resp.get_failed_responses()))
            self.assertEqual(0, len(get_resp.get_successful_responses()))
            self.assertEqual(2, len(get_resp.get_failed_responses()))

            # Now switch over to normal test client and re-drive failed transactions make sure it works
            set_resp = await self.client.multi_set(cache_name=_TEST_CACHE_NAME, ops=set_resp.get_failed_responses())
            get_resp = await self.client.multi_get(cache_name=_TEST_CACHE_NAME, ops=get_resp.get_failed_responses())

            # we should only have success now and no errors after re-driving all the failed ops
            self.assertEqual(5, len(set_resp.get_successful_responses()))
            self.assertEqual(2, len(get_resp.get_successful_responses()))
            self.assertEqual(0, len(set_resp.get_failed_responses()))
            self.assertEqual(0, len(get_resp.get_failed_responses()))

            # Make sure were getting values we expect back
            self.assertEqual("buzz5", get_resp.values()[1])

    # Test hget hash miss
    async def test_get_hash_miss(self):
        async with simple_cache_client.init(_AUTH_TOKEN, _DEFAULT_TTL_SECONDS) as simple_cache:
            get_response = await simple_cache.hget(
                cache_name=_TEST_CACHE_NAME, hash_name="hello world", key="key")
            self.assertEquals(CacheHashGetStatus.HASH_MISS, get_response.status())

    async def test_hset_response(self):
        async with simple_cache_client.init(_AUTH_TOKEN, _DEFAULT_TTL_SECONDS) as simple_cache:
            # Test with key as string
            set_response = await simple_cache.hset(
                cache_name=_TEST_CACHE_NAME, hash_name="myhash", map={"key1": "value1"})
            self.assertEquals("myhash", set_response.key())
            self.assertEquals({"key1": CacheHashValue(value=b"value1")}, set_response.value())

            # Test key as bytes
            set_response = await simple_cache.hset(
                cache_name=_TEST_CACHE_NAME, hash_name="myhash2", map={b"key1": "value1"})
            self.assertEquals("myhash2", set_response.key())
            self.assertEquals({b"key1": CacheHashValue(value=b"value1")}, set_response.value())            

    async def test_hget_and_hset_missing_key(self):
        async with simple_cache_client.init(_AUTH_TOKEN, _DEFAULT_TTL_SECONDS) as simple_cache:
            await simple_cache.hset(
                cache_name=_TEST_CACHE_NAME, hash_name="myhash3", map={"key1": "value1"})
            get_response = await simple_cache.hget(
                cache_name=_TEST_CACHE_NAME, hash_name="myhash3", key="key2")
            self.assertEquals(CacheHashGetStatus.HASH_KEY_MISS, get_response.status())

    async def test_hget_hit(self):
        async with simple_cache_client.init(_AUTH_TOKEN, _DEFAULT_TTL_SECONDS) as simple_cache:
            # Test all combinations of type(key) in {str, bytes} and type(value) in {str, bytes}
            for i, (key_is_str, value_is_str) in enumerate(itertools.product((True, False), (True, False))):
                key, value = "key1", "value1"
                if not key_is_str:
                    key = key.encode()
                if not value_is_str:
                    value = value.encode()
                mapping = {key: value}
                # Use distinct hash names to avoid collisions with already finished tests
                hash_name = f"myhash4-{i}"

                await simple_cache.hset(
                    cache_name=_TEST_CACHE_NAME, hash_name=hash_name, map=mapping)
                get_response = await simple_cache.hget(
                    cache_name=_TEST_CACHE_NAME, hash_name=hash_name, key=key)
                self.assertEquals(CacheHashGetStatus.HIT, get_response.status())
                self.assertEquals(value, get_response.value() if value_is_str else get_response.value_as_bytes())

    def test_hgetall(self):
        pass


if __name__ == '__main__':
    unittest.main()

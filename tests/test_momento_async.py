import unittest
import os
import uuid
import time

import momento.aio.simple_cache_client as simple_cache_client
import momento.errors as errors
from momento.cache_operation_responses import CacheGetStatus

_AUTH_TOKEN = os.getenv('TEST_AUTH_TOKEN')
_TEST_CACHE_NAME = os.getenv('TEST_CACHE_NAME')
_BAD_AUTH_TOKEN = "eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJpbnRlZ3JhdGlvbiIsImNwIjoiY29udHJvbC5jZWxsLWFscGhhLWRldi5wcmVwcm9kLmEubW9tZW50b2hxLmNvbSIsImMiOiJjYWNoZS5jZWxsLWFscGhhLWRldi5wcmVwcm9kLmEubW9tZW50b2hxLmNvbSJ9.gdghdjjfjyehhdkkkskskmmls76573jnajhjjjhjdhnndy"
_DEFAULT_TTL_SECONDS = 60


class TestMomentoAsync(unittest.IsolatedAsyncioTestCase):
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

    async def test_create_cache_throws_permission_exception_for_bad_token(self):
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

    async def test_delete_cache_throws_permission_exception_for_bad_token(self):
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

    async def test_list_caches_throws_permission_exception_for_bad_token(self):
        async with simple_cache_client.init(_BAD_AUTH_TOKEN, _DEFAULT_TTL_SECONDS) as simple_cache:
            with self.assertRaises(errors.AuthenticationError):
                await simple_cache.list_caches()

    async def test_list_caches_with_next_token_works(self):
        # skip until pagination is actually implemented, see
        # https://github.com/momentohq/control-plane-service/issues/83
        self.skipTest("pagination not yet implemented")

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
        async with simple_cache_client.init(_AUTH_TOKEN, 1) as simple_cache:
            await simple_cache.set(_TEST_CACHE_NAME, key, val)

            self.assertEqual((await simple_cache.get(_TEST_CACHE_NAME, key)).status(), CacheGetStatus.HIT)

            time.sleep(1.5)
            self.assertEqual((await simple_cache.get(_TEST_CACHE_NAME, key)).status(), CacheGetStatus.MISS)

    async def test_set_with_different_ttl(self):
        key1 = str(uuid.uuid4())
        key2 = str(uuid.uuid4())

        await self.client.set(_TEST_CACHE_NAME, key1, "1", 1)
        await self.client.set(_TEST_CACHE_NAME, key2, "2")

        self.assertEqual((await self.client.get(_TEST_CACHE_NAME, key1)).status(), CacheGetStatus.HIT)
        self.assertEqual((await self.client.get(_TEST_CACHE_NAME, key2)).status(), CacheGetStatus.HIT)

        time.sleep(1.5)
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

    async def test_set_throws_permission_exception_for_bad_token(self):
        async with simple_cache_client.init(_BAD_AUTH_TOKEN, _DEFAULT_TTL_SECONDS) as simple_cache:
            with self.assertRaises(errors.AuthenticationError):
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

    async def test_get_throws_permission_exception_for_bad_token(self):
        async with simple_cache_client.init(_BAD_AUTH_TOKEN, _DEFAULT_TTL_SECONDS) as simple_cache:
            with self.assertRaises(errors.AuthenticationError):
                await simple_cache.get(_TEST_CACHE_NAME, "foo")


if __name__ == '__main__':
    unittest.main()

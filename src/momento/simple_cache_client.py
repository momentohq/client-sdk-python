import asyncio
from types import TracebackType
from typing import Optional, Mapping, Union, Type

from .aio import simple_cache_client as aio
from ._async_utils import wait_for_coroutine
from .cache_operation_types import (
    CacheGetResponse,
    CacheSetResponse,
    CacheDeleteResponse,
    CreateCacheResponse,
    CreateSigningKeyResponse,
    DeleteCacheResponse,
    ListCachesResponse,
    CacheGetMultiResponse,
    CacheSetMultiResponse,
    ListSigningKeysResponse,
    RevokeSigningKeyResponse,
)
from ._utilities._data_validation import _validate_request_timeout


class SimpleCacheClient:
    def __init__(
        self,
        auth_token: str,
        default_ttl_seconds: int,
        data_client_operation_timeout_ms: Optional[int],
    ):
        self._init_loop()
        self._momento_async_client = aio.SimpleCacheClient(
            auth_token=auth_token,
            default_ttl_seconds=default_ttl_seconds,
            data_client_operation_timeout_ms=data_client_operation_timeout_ms,
        )

    def _init_loop(self) -> None:
        try:
            # If the synchronous client is used inside an async application,
            # use the event loop it's running within.
            loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()
        except RuntimeError:
            # Currently, we rely on asyncio's module-wide event loop due to the
            # way the grpc stubs we've got are hiding the _loop parameter.
            # If a separate loop is required, e.g., so you can run Simple Cache
            # on a background thread, you'll want to open an issue.
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        self._loop = loop

    def __enter__(self) -> "SimpleCacheClient":
        wait_for_coroutine(self._loop, self._momento_async_client.__aenter__())
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        wait_for_coroutine(
            self._loop,
            self._momento_async_client.__aexit__(exc_type, exc_value, traceback),
        )

    def create_cache(self, cache_name: str) -> CreateCacheResponse:
        """Creates a new cache in your Momento account.

        Args:
            cache_name: String used to create cache.

        Returns:
            CreateCacheResponse

        Raises:
            InvalidArgumentError: If provided cache_name is None.
            BadRequestError: If the cache name provided doesn't follow the naming conventions
            AlreadyExistsError: If cache with the given name already exists.
            AuthenticationError: If the provided Momento Auth Token is invalid.
            ClientSdkError: For any SDK checks that fail.
        """
        coroutine = self._momento_async_client.create_cache(cache_name)
        return wait_for_coroutine(self._loop, coroutine)

    def delete_cache(self, cache_name: str) -> DeleteCacheResponse:
        """Deletes a cache and all the items within it.

        Args:
            cache_name: String cache name to delete.

        Returns:
            DeleteCacheResponse

        Raises:
            InvalidArgumentError: If provided cache_name is None.
            BadRequestError: If the cache name provided doesn't follow the naming conventions
            NotFoundError: If an attempt is made to delete a MomentoCache that doesn't exist.
            AuthenticationError: If the provided Momento Auth Token is invalid.
            ClientSdkError: For any SDK checks that fail.
        """
        coroutine = self._momento_async_client.delete_cache(cache_name)
        return wait_for_coroutine(self._loop, coroutine)

    def list_caches(self, next_token: Optional[str] = None) -> ListCachesResponse:
        """Lists all caches.

        Args:
            next_token: Token to continue paginating through the list. It's used to handle large paginated lists.

        Returns:
            ListCachesResponse

        Raises:
            AuthenticationError: If the provided Momento Auth Token is invalid.
        """
        coroutine = self._momento_async_client.list_caches(next_token)
        return wait_for_coroutine(self._loop, coroutine)

    def create_signing_key(self, ttl_minutes: int) -> CreateSigningKeyResponse:
        """Creates a Momento signing key

        Args:
            ttl_minutes: The key's time-to-live in minutes

        Returns:
            CreateSigningKeyResponse

        Raises:
            InvalidArgumentError: If provided ttl minutes is negative.
            BadRequestError: If the ttl provided is not accepted
            AuthenticationError: If the provided Momento Auth Token is invalid.
            ClientSdkError: For any SDK checks that fail.
        """
        coroutine = self._momento_async_client.create_signing_key(ttl_minutes)
        return wait_for_coroutine(self._loop, coroutine)

    def revoke_signing_key(self, key_id: str) -> RevokeSigningKeyResponse:
        """Revokes a Momento signing key, all tokens signed by which will be invalid

        Args:
            key_id: The id of the Momento signing key to revoke

        Returns:
            RevokeSigningKeyResponse

        Raises:
            AuthenticationError: If the provided Momento Auth Token is invalid.
            ClientSdkError: For any SDK checks that fail.
        """
        coroutine = self._momento_async_client.revoke_signing_key(key_id)
        return wait_for_coroutine(self._loop, coroutine)

    def list_signing_keys(
        self, next_token: Optional[str] = None
    ) -> ListSigningKeysResponse:
        """Lists all Momento signing keys for the provided auth token.

        Args:
            next_token: Token to continue paginating through the list. It's used to handle large paginated lists.

        Returns:
            ListSigningKeysResponse

        Raises:
            AuthenticationError: If the provided Momento Auth Token is invalid.
            ClientSdkError: For any SDK checks that fail.
        """
        coroutine = self._momento_async_client.list_signing_keys(next_token)
        return wait_for_coroutine(self._loop, coroutine)

    def set(
        self,
        cache_name: str,
        key: str,
        value: Union[str, bytes],
        ttl_seconds: Optional[int] = None,
    ) -> CacheSetResponse:
        """Stores an item in cache

        Args:
            cache_name: Name of the cache to store the item in.
            key (string or bytes): The key to be used to store item.
            value (string or bytes): The value to be stored.
            ttl_seconds (Optional): Time to live in cache in seconds. If not provided, then default TTL for the cache
                client instance is used.

        Returns:
            CacheSetResponse

        Raises:
            InvalidArgumentError: If validation fails for the provided method arguments.
            BadRequestError: If the provided inputs are rejected by server because they are invalid
            NotFoundError: If the cache with the given name doesn't exist.
            AuthenticationError: If the provided Momento Auth Token is invalid.
            InternalServerError: If server encountered an unknown error while trying to store the item.
        """
        coroutine = self._momento_async_client.set(cache_name, key, value, ttl_seconds)
        return wait_for_coroutine(self._loop, coroutine)

    def set_multi(
        self,
        cache_name: str,
        items: Union[Mapping[str, str], Mapping[bytes, bytes]],
        ttl_seconds: Optional[int] = None,
    ) -> CacheSetMultiResponse:
        """Store items in the cache.

        Args:
            cache_name: Name of the cache to store the item in.
            items: (Union[Mapping[str, str], Mapping[bytes, bytes]]): The items to store.
            ttl_seconds: (Optional[int]): The TTL to apply to each item. Defaults to None.

        Returns:
            CacheSetMultiResponse

        Raises:
            InvalidArgumentError: If validation fails for the provided method arguments.
            BadRequestError: If the provided inputs are rejected by server because they are invalid
            NotFoundError: If the cache with the given name doesn't exist.
            AuthenticationError: If the provided Momento Auth Token is invalid.
            InternalServerError: If server encountered an unknown error while trying to retrieve the item.
        """
        coroutine = self._momento_async_client.set_multi(cache_name, items, ttl_seconds)
        return wait_for_coroutine(self._loop, coroutine)

    def get(self, cache_name: str, key: str) -> CacheGetResponse:
        """Retrieve an item from the cache

        Args:
            cache_name: Name of the cache to get the item from
            key (string or bytes): The key to be used to retrieve the item.

        Returns:
            CacheGetResponse

        Raises:
            InvalidArgumentError: If validation fails for the provided method arguments.
            BadRequestError: If the provided inputs are rejected by server because they are invalid
            NotFoundError: If the cache with the given name doesn't exist.
            AuthenticationError: If the provided Momento Auth Token is invalid.
            InternalServerError: If server encountered an unknown error while trying to retrieve the item.
        """
        coroutine = self._momento_async_client.get(cache_name, key)
        return wait_for_coroutine(self._loop, coroutine)

    def get_multi(
        self, cache_name: str, *keys: Union[str, bytes]
    ) -> CacheGetMultiResponse:
        """Retrieve multiple items from the cache.

        Args:
            cache_name (str): Name of the cache to get the item from.
            keys: (Union[str, bytes]): The keys used to retrieve the items.

        Returns:
            CacheGetMultiResponse

        Raises:
            InvalidArgumentError: If validation fails for the provided method arguments.
            BadRequestError: If the provided inputs are rejected by server because they are invalid
            NotFoundError: If the cache with the given name doesn't exist.
            AuthenticationError: If the provided Momento Auth Token is invalid.
            InternalServerError: If server encountered an unknown error while trying to retrieve the item.
        """
        coroutine = self._momento_async_client.get_multi(cache_name, *keys)
        return wait_for_coroutine(self._loop, coroutine)

    def delete(self, cache_name: str, key: str) -> CacheDeleteResponse:
        """Delete an item from the cache.

        Performs a no-op if the item is not in the cache.

        Args:
            cache_name: Name of the cache to delete the item from.
            key (string or bytes): The key to delete.

        Returns:
            CacheDeleteResponse

        Raises:
            InvalidArgumentError: If validation fails for provided method arguments.
            BadRequestError: If the provided inputs are rejected by server because they are invalid
            NotFoundError: If the cache with the given name doesn't exist.
            AuthenticationError: If the provided Momento Auth Token is invalid.
            InternalServerError: If server encountered an unknown error while trying to delete the item.
        """
        coroutine = self._momento_async_client.delete(cache_name, key)
        return wait_for_coroutine(self._loop, coroutine)


def init(
    auth_token: str,
    item_default_ttl_seconds: int,
    request_timeout_ms: Optional[int] = None,
) -> SimpleCacheClient:
    """Creates a SimpleCacheClient

    Args:
        auth_token: Momento Token to authenticate the requests with Simple Cache Service
        item_default_ttl_seconds: A default Time To Live in seconds for cache objects created by this client. It is
            possible to override this setting when calling the set method.
        request_timeout_ms: An optional timeout in milliseconds to allow for Get and Set operations to complete.
            Defaults to 5 seconds. The request will be terminated if it takes longer than this value and will result
            in TimeoutError.
    Returns:
        SimpleCacheClient
    Raises:
        IllegalArgumentError: If method arguments fail validations
    """
    _validate_request_timeout(request_timeout_ms)
    return SimpleCacheClient(auth_token, item_default_ttl_seconds, request_timeout_ms)

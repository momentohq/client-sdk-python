from ._scs_control_client import _ScsControlClient
from ._scs_data_client import _ScsDataClient

from .. import _momento_endpoint_resolver
from ..cache_operation_responses import CreateCacheResponse, DeleteCacheResponse, ListCachesResponse, CacheSetResponse, \
    CacheGetResponse


class SimpleCacheClient:
    """async Simple Cache client"""
    def __init__(self, auth_token, default_ttl_seconds):
        endpoints = _momento_endpoint_resolver.resolve(auth_token)
        self._control_client = _ScsControlClient(auth_token,
                                                 endpoints.control_endpoint)
        self._data_client = _ScsDataClient(auth_token,
                                           endpoints.cache_endpoint,
                                           default_ttl_seconds)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, exc_traceback):
        await self._control_client.close()
        await self._data_client.close()

    async def create_cache(self, cache_name) -> CreateCacheResponse:
        """Creates a new cache in your Momento account.

        Args:
            cache_name: String used to create cache.

        Returns:
            CreateCacheResponse

        Raises:
            InvalidInputError: If cache name is None.
            ClientSdkError: For any SDK checks that fail.
            CacheValueError: If provided cache_name is empty.
            CacheExistsError: If cache with the given name already exists.
            PermissionError: If the provided Momento Auth Token is invalid to perform the requested operation.
        """
        return await self._control_client.create_cache(cache_name)

    async def delete_cache(self, cache_name) -> DeleteCacheResponse:
        """Deletes a cache and all of the items within it.

        Args:
            cache_name: String cache name to delete.

        Returns:
            DeleteCacheResponse

        Raises:
            CacheNotFoundError: If an attempt is made to delete a MomentoCache that doesn't exits.
            InvalidInputError: If cache name is None.
            ClientSdkError: For any SDK checks that fail.
            CacheValueError: If provided cache name is empty
            PermissionError: If the provided Momento Auth Token is invalid to perform the requested operation.
        """
        return await self._control_client.delete_cache(cache_name)

    async def list_caches(self, next_token=None) -> ListCachesResponse:
        """Lists all caches.

        Args:
            next_token: Token to continue paginating through the list. It's used to handle large paginated lists.

        Returns:
            ListCachesResponse

        Raises:
            Exception to notify either sdk, grpc, or operation error.
            PermissionError: If the provided Momento Auth Token is invalid to perform the requested operation.
        """
        return await self._control_client.list_caches(next_token)

    async def set(self, cache_name, key, value, ttl_seconds=None) -> CacheSetResponse:
        """Stores an item in cache

        Args:
            cache_name: Name of the cache to store the item in.
            key (string or bytes): The key to be used to store item.
            value (string or bytes): The value to be stored.
            ttl_seconds (Optional): Time to live in cache in seconds. If not provided, then default TTL for the cache client instance is used.

        Returns:
            CacheSetResponse

        Raises:
            InvalidInputError: If service validation fails for provided values.
            ClientSdkError: If cache name is invalid type.
            CacheNotFoundError: If an attempt is made to store an item in a cache that doesn't exist.
            PermissionError: If the provided Momento Auth Token is invalid to perform the requested operation.
            InternalServerError: If server encountered an unknown error while trying to store the item.
        """
        return await self._data_client.set(cache_name, key, value, ttl_seconds)

    async def get(self, cache_name, key) -> CacheGetResponse:
        """Retrieve an item from the cache

        Args:
            cache_name: Name of the cache to get the item from
            key (string or bytes): The key to be used to retrieve the item.

        Returns:
            CacheGetResponse

        Raises:
            InvalidInputError: If service validation fails for provided values.
            ClientSdkError: If cache name is invalid type.
            CacheNotFoundError: If an attempt is made to retrieve an item in a cache that doesn't exist.
            PermissionError: If the provided Momento Auth Token is invalid to perform the requested operation.
            InternalServerError: If server encountered an unknown error while trying to retrieve the item.
        """
        return await self._data_client.get(cache_name, key)


def init(auth_token, item_default_ttl_seconds) -> SimpleCacheClient:
    """ Creates an async SimpleCacheClient

    Args:
        auth_token: Momento Token to authenticate the requests with Simple Cache Service
        item_default_ttl_seconds: A default Time To Live in seconds for cache objects created by this client. It is possible to override this setting when calling the set method.
    Returns:
        SimpleCacheClient
    Raises:
        InvalidInputError: If service validation fails for provided values
        InternalServerError: If server encountered an unknown error.
    """
    return SimpleCacheClient(auth_token, item_default_ttl_seconds)
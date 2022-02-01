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
            InvalidArgumentError: If provided cache_name is empty or None.
            BadRequestError: If the cache name provided doesn't follow the naming conventions
            ExistsError: If cache with the given name already exists.
            AuthenticationError: If the provided Momento Auth Token is invalid.
            ClientSdkError: For any SDK checks that fail.
        """
        return await self._control_client.create_cache(cache_name)

    async def delete_cache(self, cache_name) -> DeleteCacheResponse:
        """Deletes a cache and all of the items within it.

        Args:
            cache_name: String cache name to delete.

        Returns:
            DeleteCacheResponse

        Raises:
            InvalidArgumentError: If provided cache_name is empty or None.
            BadRequestError: If the cache name provided doesn't follow the naming conventions
            NotFoundError: If an attempt is made to delete a MomentoCache that doesn't exits.
            AuthenticationError: If the provided Momento Auth Token is invalid.
            ClientSdkError: For any SDK checks that fail.
        """
        return await self._control_client.delete_cache(cache_name)

    async def list_caches(self, next_token=None) -> ListCachesResponse:
        """Lists all caches.

        Args:
            next_token: Token to continue paginating through the list. It's used to handle large paginated lists.

        Returns:
            ListCachesResponse

        Raises:
            AuthenticationError: If the provided Momento Auth Token is invalid.
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
            InvalidArgumentError: If validation fails for provided values.
            BadRequestError: If the provided inputs are rejected by server because they are invalid
            NotFoundError: If the cache with the given name doesn't exist.
            AuthenticationError: If the provided Momento Auth Token is invalid.
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
            InvalidArgumentError: If validation fails for provided values.
            BadRequestError: If the provided inputs are rejected by server because they are invalid
            NotFoundError: If the cache with the given name doesn't exist.
            AuthenticationError: If the provided Momento Auth Token is invalid.
            InternalServerError: If server encountered an unknown error while trying to store the item.
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
        IllegalArgumentError: If the provided auth token and/or item_default_ttl_seconds is invalid
    """
    return SimpleCacheClient(auth_token, item_default_ttl_seconds)

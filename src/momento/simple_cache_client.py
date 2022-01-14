from ._scs_control_client import _ScsControlClient
from ._scs_data_client import _ScsDataClient

from . import _momento_endpoint_resolver

class SimpleCacheClient:
    def __init__(self, auth_token, default_ttl_seconds):
        endpoints = _momento_endpoint_resolver.resolve(auth_token)
        self._control_client = _ScsControlClient(auth_token, endpoints.control_endpoint)
        self._data_client = _ScsDataClient(auth_token, endpoints.cache_endpoint, default_ttl_seconds)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self._control_client.close()
        self._data_client.close()

    def create_cache(self, cache_name):
        """Creates a new cache in your Momento account.

        Args:
            cache_name: String used to create cache.

        Returns:
            CreateCacheResponse

        Raises:
            ClientSdkError: If an attempt is made to pass anything other than string as cache_name.
        """
        return self._control_client.create_cache(cache_name)

    def delete_cache(self, cache_name):
        """Deletes a cache and all of the items within it.

        Args:
            cache_name: String cache name to delete.

        Returns:
            DeleteCacheResponse

        Raises:
            CacheNotFoundError: If an attempt is made to delete a MomentoCache that doesn't exits.
            ClientSdkError: If an attempt is made to pass anything other than string as cache_name.
        """
        return self._control_client.delete_cache(cache_name)

    def list_caches(self, next_token=None):
        """Lists all caches.

        Args:
            next_token: Token to continue paginating through the list. It's used to handle large paginated lists.

        Returns:
            ListCachesResponse

        Raises:
            Exception to notify either sdk, grpc, or operation error.
        """
        return self._control_client.list_caches(next_token)

    def set(self, cache_name, key, value, ttl_seconds=None):
        """Stores an item in cache

        Args:
            cache_name: Name of the cache to stor the item in.
            key (string or bytes): The key to be used to store item.
            value (string or bytes): The value to be stored.
            ttl_second (Optional): Time to live in cache in seconds. If not provided default TTL provided while creating the cache client instance is used.

        Returns:
            CacheSetResponse

        Raises:
            CacheValueError: If service validation fails for provided values.
            CacheNotFoundError: If an attempt is made to store an item in a cache that doesn't exist.
            PermissionError: If the provided Momento Auth Token is invalid to perform the requested operation.
            InternalServerError: If server encountered an unknown error while trying to store the item.
        """
        return self._data_client.set(cache_name, key, value, ttl_seconds)

    def get(self, cache_name, key):
        """Retrieve an item from the cache

        Args:
            cache_name: Name of the cache to get the item from
            key (string or bytes): The key to be used to retrieve the item.

        Returns:
            CacheGetResponse

        Raises:
            CacheValueError: If service validation fails for provided values.
            CacheNotFoundError: If an attempt is made to retrieve an item in a cache that doesn't exist.
            PermissionError: If the provided Momento Auth Token is invalid to perform the requested operation.
            InternalServerError: If server encountered an unknown error while trying to retrieve the item.
        """
        return self._data_client.get(cache_name, key)


def get(auth_token, item_default_ttl_seconds):
    """ Creates a SimpleCacheClient

    Args:
        auth_token: Momento Token to authenticate the requests with Simple Cache Service
        item_default_ttl_seconds: A default Time To Live in seconds for cache objects created by this client. 0It is possible to override this setting when calling the set method.
    Returns:
        SimpleCacheClient
    """
    return SimpleCacheClient(auth_token, item_default_ttl_seconds)

from types import TracebackType
from typing import Optional, Union, Type, List

try:
    from ._scs_control_client import _ScsControlClient
    from ._scs_data_client import _ScsDataClient
    from .._utilities._data_validation import _validate_request_timeout
except ImportError as e:
    if e.name == "cygrpc":
        import sys

        print(
            "There is an issue on M1 macs between GRPC native packaging and Python wheel tags. See https://github.com/grpc/grpc/issues/28387",
            file=sys.stderr,
        )
        print("-".join("" for _ in range(99)), file=sys.stderr)
        print("    TO WORK AROUND:", file=sys.stderr)
        print("    * Install Rosetta 2", file=sys.stderr)
        print(
            "    * Install Python from python.org (you might need to do this if you're using an arm-only build)",
            file=sys.stderr,
        )
        print("    * re-run with:", file=sys.stderr)
        print("arch -x86_64 {} {}".format(sys.executable, *sys.argv), file=sys.stderr)
        print("-".join("" for _ in range(99)), file=sys.stderr)
    raise e

from .. import _momento_endpoint_resolver
from ..cache_operation_types import (
    CreateCacheResponse,
    DeleteCacheResponse,
    ListCachesResponse,
    CacheSetResponse,
    CacheGetResponse,
    CacheMultiSetOperation,
    CacheMultiGetOperation,
    CacheMultiSetFailureResponse,
    CacheMultiSetResponse,
    CacheMultiGetResponse,
    CacheMultiGetFailureResponse,
)


class SimpleCacheClient:
    """async Simple Cache client"""

    def __init__(
        self,
        auth_token: str,
        default_ttl_seconds: int,
        data_client_operation_timeout_ms: Optional[int],
    ):
        endpoints = _momento_endpoint_resolver.resolve(auth_token)
        self._control_client = _ScsControlClient(auth_token, endpoints.control_endpoint)
        self._data_client = _ScsDataClient(
            auth_token,
            endpoints.cache_endpoint,
            default_ttl_seconds,
            data_client_operation_timeout_ms,
        )

    async def __aenter__(self) -> "SimpleCacheClient":
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        await self._control_client.close()
        await self._data_client.close()

    async def create_cache(self, cache_name: str) -> CreateCacheResponse:
        """Creates a new cache in your Momento account.

        Args:
            cache_name: String used to create cache.

        Returns:
            CreateCacheResponse

        Raises:
            InvalidArgumentError: If provided cache_name None.
            BadRequestError: If the cache name provided doesn't follow the naming conventions
            ExistsError: If cache with the given name already exists.
            AuthenticationError: If the provided Momento Auth Token is invalid.
            ClientSdkError: For any SDK checks that fail.
        """
        return await self._control_client.create_cache(cache_name)

    async def delete_cache(self, cache_name: str) -> DeleteCacheResponse:
        """Deletes a cache and all of the items within it.

        Args:
            cache_name: String cache name to delete.

        Returns:
            DeleteCacheResponse

        Raises:
            InvalidArgumentError: If provided cache_name is None.
            BadRequestError: If the cache name provided doesn't follow the naming conventions
            NotFoundError: If an attempt is made to delete a MomentoCache that doesn't exits.
            AuthenticationError: If the provided Momento Auth Token is invalid.
            ClientSdkError: For any SDK checks that fail.
        """
        return await self._control_client.delete_cache(cache_name)

    async def list_caches(self, next_token: Optional[str] = None) -> ListCachesResponse:
        """Lists all caches.

        Args:
            next_token: Token to continue paginating through the list. It's used to handle large paginated lists.

        Returns:
            ListCachesResponse

        Raises:
            AuthenticationError: If the provided Momento Auth Token is invalid.
        """
        return await self._control_client.list_caches(next_token)

    async def multi_set(
        self,
        cache_name: str,
        ops: Union[List[CacheMultiSetOperation], List[CacheMultiSetFailureResponse]],
    ) -> CacheMultiSetResponse:
        """Executes a list of passed Set operations in parallel.

        Args:
            cache_name: Name of the cache to store the item in.
            ops: (Union[List[CacheMultiSetOperation], List[CacheMultiSetFailureResponse]]): List of set operations to
                execute.

        Returns:
            CacheMultiGetResponse

        Raises:
            InvalidArgumentError: If validation fails for the provided method arguments.
            BadRequestError: If the provided inputs are rejected by server because they are invalid
            NotFoundError: If the cache with the given name doesn't exist.
            AuthenticationError: If the provided Momento Auth Token is invalid.
            InternalServerError: If server encountered an unknown error while trying to retrieve the item.
        """
        return await self._data_client.multi_set(cache_name, ops)

    async def set(
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
            InvalidArgumentError: If validation fails for provided method arguments.
            BadRequestError: If the provided inputs are rejected by server because they are invalid
            NotFoundError: If the cache with the given name doesn't exist.
            AuthenticationError: If the provided Momento Auth Token is invalid.
            InternalServerError: If server encountered an unknown error while trying to store the item.
        """
        return await self._data_client.set(cache_name, key, value, ttl_seconds)

    async def multi_get(
        self,
        cache_name: str,
        ops: Union[List[CacheMultiGetOperation], List[CacheMultiGetFailureResponse]],
    ) -> CacheMultiGetResponse:
        """Executes a list of passed Get operations in parallel.

        Args:
            cache_name: Name of the cache to get the item from.
            ops: (Union[List[CacheMultiGetOperation], List[CacheMultiGetFailureResponse]]): List of get operations to
                execute.

        Returns:
            CacheMultiGetResponse

        Raises:
            InvalidArgumentError: If validation fails for the provided method arguments.
            BadRequestError: If the provided inputs are rejected by server because they are invalid
            NotFoundError: If the cache with the given name doesn't exist.
            AuthenticationError: If the provided Momento Auth Token is invalid.
            InternalServerError: If server encountered an unknown error while trying to retrieve the item.
        """
        return await self._data_client.multi_get(cache_name, ops)

    async def get(self, cache_name: str, key: str) -> CacheGetResponse:
        """Retrieve an item from the cache

        Args:
            cache_name: Name of the cache to get the item from
            key (string or bytes): The key to be used to retrieve the item.

        Returns:
            CacheGetResponse

        Raises:
            InvalidArgumentError: If validation fails for provided method arguments.
            BadRequestError: If the provided inputs are rejected by server because they are invalid
            NotFoundError: If the cache with the given name doesn't exist.
            AuthenticationError: If the provided Momento Auth Token is invalid.
            InternalServerError: If server encountered an unknown error while trying to retrieve the item.
        """
        return await self._data_client.get(cache_name, key)


def init(
    auth_token: str,
    item_default_ttl_seconds: int,
    request_timeout_ms: Optional[int] = None,
) -> SimpleCacheClient:
    """Creates an async SimpleCacheClient

    Args:
        auth_token: Momento Token to authenticate the requests with Simple Cache Service
        item_default_ttl_seconds: A default Time To Live in seconds for cache objects created by this client. It is
            possible to override this setting when calling the set method.
        request_timeout_ms: An optional timeout in milliseconds to allow for Get and Set operations to complete.
            Defaults to 5 seconds. The request will be terminated if it takes longer than this value and will result in
            TimeoutError.
    Returns:
        SimpleCacheClient
    Raises:
        IllegalArgumentError: If method arguments fail validations.
    """
    _validate_request_timeout(request_timeout_ms)
    return SimpleCacheClient(auth_token, item_default_ttl_seconds, request_timeout_ms)

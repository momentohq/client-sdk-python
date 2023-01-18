from types import TracebackType
from typing import Optional, Type, Union

from momento import logs

try:
    from momento._utilities._data_validation import _validate_request_timeout
    from momento.internal.synchronous._scs_control_client import _ScsControlClient
    from momento.internal.synchronous._scs_data_client import _ScsDataClient
except ImportError as e:
    if e.name == "cygrpc":
        import sys

        print(
            "There is an issue on M1 macs between GRPC native packaging and Python wheel tags. "
            "See https://github.com/grpc/grpc/issues/28387",
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

import momento._momento_endpoint_resolver as endpoint_resolver
from momento.cache_operation_types import (
    CreateSigningKeyResponse,
    ListSigningKeysResponse,
    RevokeSigningKeyResponse,
)
from momento.responses import (
    CacheDeleteResponseBase,
    CacheGetResponseBase,
    CacheSetResponseBase,
    CreateCacheResponseBase,
    DeleteCacheResponseBase,
    ListCachesResponseBase,
)


class SimpleCacheClient:
    """Synchronous Simple Cache Client"""

    def __init__(
        self,
        auth_token: str,
        default_ttl_seconds: int,
        request_timeout_ms: Optional[int] = None,
    ):
        """Creates an async SimpleCacheClient

        Args:
            auth_token (str): Momento Token to authenticate the requests with Simple Cache Service
            default_ttl_seconds (int): A default Time To Live in seconds for cache objects created by this client. It is
                possible to override this setting when calling the set method.
            request_timeout_ms (Optional[int], optional): An optional timeout in milliseconds to allow for Get and Set
                operations to complete. The request will be terminated if it takes longer than this value and will
                result in TimeoutError. Defaults to None, in which case a 5 second timeout is used.
        Raises:
            IllegalArgumentError: If method arguments fail validations.
        """
        _validate_request_timeout(request_timeout_ms)
        self._logger = logs.logger
        self._next_client_index = 0
        endpoints = endpoint_resolver.resolve(auth_token)
        self._control_client = _ScsControlClient(auth_token, endpoints.control_endpoint)
        self._data_client = _ScsDataClient(
            auth_token,
            endpoints.cache_endpoint,
            default_ttl_seconds,
            request_timeout_ms,
        )

    def __enter__(self) -> "SimpleCacheClient":
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        self._control_client.close()
        self._data_client.close()

    def create_cache(self, cache_name: str) -> CreateCacheResponseBase:
        """Creates a new cache in your Momento account.

        Args:
            cache_name: String used to create cache.

        Returns:
            CreateCacheResponseBase

        Raises:
            InvalidArgumentError: If provided cache_name None.
            BadRequestError: If the cache name provided doesn't follow the naming conventions
            ExistsError: If cache with the given name already exists.
            AuthenticationError: If the provided Momento Auth Token is invalid.
            ClientSdkError: For any SDK checks that fail.
        """
        return self._control_client.create_cache(cache_name)

    def delete_cache(self, cache_name: str) -> DeleteCacheResponseBase:
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
        return self._control_client.delete_cache(cache_name)

    def list_caches(self, next_token: Optional[str] = None) -> ListCachesResponseBase:
        """Lists all caches.

        Args:
            next_token: Token to continue paginating through the list. It's used to handle large paginated lists.

        Returns:
            ListCachesResponse

        Raises:
            AuthenticationError: If the provided Momento Auth Token is invalid.
        """
        return self._control_client.list_caches(next_token)

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
        return self._control_client.create_signing_key(ttl_minutes, self._data_client.endpoint)

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
        return self._control_client.revoke_signing_key(key_id)

    def list_signing_keys(self, next_token: Optional[str] = None) -> ListSigningKeysResponse:
        """Lists all Momento signing keys for the provided auth token.

        Args:
            next_token: Token to continue paginating through the list. It's used to handle large paginated lists.

        Returns:
            ListSigningKeysResponse

        Raises:
            AuthenticationError: If the provided Momento Auth Token is invalid.
            ClientSdkError: For any SDK checks that fail.
        """
        return self._control_client.list_signing_keys(self._data_client.endpoint, next_token)

    def set(
        self,
        cache_name: str,
        key: str,
        value: Union[str, bytes],
        ttl_seconds: Optional[int] = None,
    ) -> CacheSetResponseBase:
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
        return self._data_client.set(cache_name, key, value, ttl_seconds)

    def get(self, cache_name: str, key: str) -> CacheGetResponseBase:
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
        return self._data_client.get(cache_name, key)

    def delete(self, cache_name: str, key: str) -> CacheDeleteResponseBase:
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
        return self._data_client.delete(cache_name, key)

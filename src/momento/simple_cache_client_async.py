from datetime import timedelta
from types import TracebackType
from typing import Optional, Type, Union

from momento import logs
from momento.auth.credential_provider import CredentialProvider
from momento.config.configuration import Configuration

try:
    from momento._utilities._data_validation import _validate_request_timeout
    from momento.internal.aio._scs_control_client import _ScsControlClient
    from momento.internal.aio._scs_data_client import _ScsDataClient
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

from momento.responses import (
    CacheDeleteResponseBase,
    CacheGetResponseBase,
    CacheSetResponseBase,
    CreateCacheResponseBase,
    CreateSigningKeyResponse,
    DeleteCacheResponseBase,
    ListCachesResponseBase,
    ListSigningKeysResponse,
    RevokeSigningKeyResponse,
)


class SimpleCacheClientAsync:
    """Async Simple Cache Client"""

    # For high load, we might get better performance with multiple clients, because the server is
    # configured to allow a max of 100 streams per connection.  In the javascript SDK, multiple
    # clients resulted in an obvious performance improvement.  However, in the python SDK I have
    # not yet been able to observe a clear benefit.  So for now, we are putting the plumbing in
    # place so that we can more easily test performance with multiple connections in the future,
    # but we are leaving the default value set to 1.
    #
    # We are hard-coding the value for now, because we haven't yet designed the API for
    # users to use to configure tunables:
    # https://github.com/momentohq/dev-eco-issue-tracker/issues/85
    _NUM_CLIENTS = 1

    def __init__(self, configuration: Configuration, credential_provider: CredentialProvider, default_ttl: timedelta):
        """Creates an async SimpleCacheClient

        Args:
            configuration (Configuration): An object holding configuration settings for communication with the server.
            credential_provider (CredentialProvider): An object holding the auth token and endpoint information.
            default_ttl (timedelta): A default Time To Live timedelta for cache objects created by this client.
                It is possible to override this setting when calling the set method.
        Raises:
            IllegalArgumentException: If method arguments fail validations.
        """
        _validate_request_timeout(configuration.get_transport_strategy().get_grpc_configuration().get_deadline())
        self._logger = logs.logger
        self._next_client_index = 0
        self._control_client = _ScsControlClient(credential_provider)
        self._data_clients = [
            _ScsDataClient(configuration, credential_provider, default_ttl)
            for _ in range(SimpleCacheClientAsync._NUM_CLIENTS)
        ]

    async def __aenter__(self) -> "SimpleCacheClientAsync":
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        await self._control_client.close()
        for data_client in self._data_clients:
            await data_client.close()

    async def create_cache(self, cache_name: str) -> CreateCacheResponseBase:
        """Creates a cache if it doesn't exist.

        Args:
            cache_name (str): Name of the cache to be created.

        Returns:
            CreateCacheResponseBase: result of the create cache operation. This result
            is resolved to a type-safe object of one of the following subtypes:

            - `CreateCacheResponse.Success`
            - `CreateCacheResponse.AlreadyExists`
            - `CreateCacheResponse.Error`

        Pattern matching can be used to operate on the appropriate subtype.
        For example, in python 3.10+:
        ```
        match response:
            case CreateCacheResponse.Success():
                ...
            case CreateCacheResponse.CacheAlreadyExists():
                ...
            case CreateCacheResponse.Error():
                ...
            case _:
                # Shouldn't happen
        ```
        or equivalently in earlier versions of python:
        ```
        if isinstance(response, CreateCacheResponse.Success):
            ...
        elif isinstance(response, CreateCacheResponse.AlreadyExists):
            ...
        elif isinstance(response, CreateCacheResponse.Error):
            ...
        else:
            # Shouldn't happen
        ```
        """
        return await self._control_client.create_cache(cache_name)

    async def delete_cache(self, cache_name: str) -> DeleteCacheResponseBase:
        """Deletes a cache and all of the items within it.

        Args:
            cache_name (str): Name of the cache to be deleted.

        Returns:
            DeleteCacheResponseBase: result of the delete cache operation. This result
            is resolved to a type-safe object of one of the following subtypes:

            - `DeleteCacheResponse.Success`
            - `DeleteCacheResponse.Error`

        Pattern matching can be used to operate on the appropriate subtype.
        For example, in python 3.10+:
        ```
        match response:
            case DeleteCacheResponse.Success():
                ...
            case DeleteCacheResponse.Error():
                ...
            case _:
                # Shouldn't happen
        ```
        or equivalently in earlier versions of python:
        ```
        if isinstance(response, DeleteCacheResponse.Success):
            ...
        elif isinstance(response, DeleteCacheResponse.Error):
            ...
        else:
            # Shouldn't happen
        ```
        """
        return await self._control_client.delete_cache(cache_name)

    async def list_caches(self, next_token: Optional[str] = None) -> ListCachesResponseBase:
        """Lists all caches.

        Args:
            next_token: A token to specify where to start paginating. This is the NextToken from a previous response.

        Returns:
            ListCachesResponseBase: result of the delete cache operation. This result
            is resolved to a type-safe object of one of the following subtypes:

            - `ListCachesResponse.Success`
            - `ListCachesResponse.Error`

        Pattern matching can be used to operate on the appropriate subtype.
        For example, in python 3.10+:
        ```
        match response:
            case ListCachesResponse.Success():
                ...
            case ListCachesResponse.Error():
                ...
            case _:
                # Shouldn't happen
        ```
        or equivalently in earlier versions of python:
        ```
        if isinstance(response, ListCachesResponse.Success):
            ...
        elif isinstance(response, ListCachesResponse.Error):
            ...
        else:
            # Shouldn't happen
        ```
        """
        return await self._control_client.list_caches(next_token)

    async def create_signing_key(self, ttl: timedelta) -> CreateSigningKeyResponse:
        """Creates a Momento signing key

        Args:
            ttl: The key's time-to-live represented as a timedelta

        Returns:
            CreateSigningKeyResponse

        Raises:
            SdkException: validation, server-side, or other runtime error
        """
        return await self._control_client.create_signing_key(ttl, self._get_next_client().endpoint)

    async def revoke_signing_key(self, key_id: str) -> RevokeSigningKeyResponse:
        """Revokes a Momento signing key, all tokens signed by which will be invalid

        Args:
            key_id: The id of the Momento signing key to revoke

        Returns:
            RevokeSigningKeyResponse

        Raises:
            SdkException: validation, server-side, or other runtime error
        """
        return await self._control_client.revoke_signing_key(key_id)

    async def list_signing_keys(self, next_token: Optional[str] = None) -> ListSigningKeysResponse:
        """Lists all Momento signing keys for the provided auth token.

        Args:
            next_token: Token to continue paginating through the list. It's used to handle large paginated lists.

        Returns:
            ListSigningKeysResponse

        Raises:
            SdkException: validation, server-side, or other runtime error
        """
        return await self._control_client.list_signing_keys(self._get_next_client().endpoint, next_token)

    async def set(
        self,
        cache_name: str,
        key: Union[str, bytes],
        value: Union[str, bytes],
        ttl: Optional[timedelta] = None,
    ) -> CacheSetResponseBase:
        """Set the value in cache with a given time to live (TTL) seconds.

        Args:
            cache_name (str): Name of the cache to store the item in.
            key (Union[str, bytes]): The key to set.
            value (Union[str, bytes]): The value to be stored.
            ttl (Optional[timedelta], optional): TTL for the item in cache.
            This TTL takes precedence over the TTL used when initializing a cache client.
            Defaults to client TTL. If specified must be strictly positive.

        Returns:
            CacheSetResponseBase: result of the set operation. This result
            is resolved to a type-safe object of one of the following subtypes:

            - `CacheSetResponse.Success`
            - `CacheSetResponse.Error`

        Pattern matching can be used to operate on the appropriate subtype.
        For example, in python 3.10+:
        ```
        match response:
            case CacheSetResponse.Success():
                ...
            case CacheSetResponse.Error():
                ...
            case _:
                # Shouldn't happen
        ```
        or equivalently in earlier versions of python:
        ```
        if isinstance(response, CacheSetResponse.Success):
            ...
        elif isinstance(response, CacheSetResponse.Error):
            ...
        else:
            # Shouldn't happen
        ```
        """
        return await self._get_next_client().set(cache_name, key, value, ttl)

    async def get(self, cache_name: str, key: Union[str, bytes]) -> CacheGetResponseBase:
        """Get the cache value stored for the given key.

        Args:
            cache_name (str): Name of the cache to perform the lookup in.
            key (Union[str, bytes]): The key to lookup.

        Returns:
            CacheGetResponseBase: the status of the get operation and the associated value. This result
            is resolved to a type-safe object of one of the following subtypes:

            - `CacheGetResponse.Hit`
            - `CacheGetResponse.Miss`
            - `CacheGetResponse.Error`

        Pattern matching can be used to operate on the appropriate subtype.
        For example, in python 3.10+:
        ```
        match response:
            case CacheGetResponse.Hit() as hit:
                return hit.value_string
            case CacheGetResponse.Miss():
                ... # Handle miss
            case CacheGetResponse.Error():
                ...
        ```
        or equivalently in earlier versions of python:
        ```
        if isinstance(response, CacheGetResponse.Hit):
            ...
        elif isinstance(response, CacheGetResponse.Miss):
            ...
        elif isinstance(response, CacheGetResponse.Error):
            ...
        else:
            # Shouldn't happen
        ```
        """
        return await self._get_next_client().get(cache_name, key)

    async def delete(self, cache_name: str, key: Union[str, bytes]) -> CacheDeleteResponseBase:
        """Remove the key from the cache.

        Args:
            cache_name (str): Name of the cache to delete the key from.
            key (Union[str, bytes]): The key to delete.

        Returns:
            CacheDeleteResponseBase: result of the delete operation. This result
            is resolved to a type-safe object of one of the following subtypes:

            - `CacheDeleteResponse.Success`
            - `CacheDeleteResponse.Error`

        Pattern matching can be used to operate on the appropriate subtype.
        For example, in python 3.10+:
        ```
        match response:
            case CacheDeleteResponse.Success():
                ...
            case CacheDeleteResponse.Error():
                ...
            case _:
                # Shouldn't happen
        ```
        or equivalently in earlier versions of python:
        ```
        if isinstance(response, CacheDeleteResponse.Success):
            ...
        elif isinstance(response, CacheDeleteResponse.Error):
            ...
        else:
            # Shouldn't happen
        ```
        """
        return await self._get_next_client().delete(cache_name, key)

    def _get_next_client(self) -> _ScsDataClient:
        client = self._data_clients[self._next_client_index]
        self._next_client_index = (self._next_client_index + 1) % len(self._data_clients)
        return client

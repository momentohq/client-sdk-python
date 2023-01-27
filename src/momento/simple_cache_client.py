from datetime import timedelta
from types import TracebackType
from typing import Optional, Type, Union

from momento import logs
from momento.auth import CredentialProvider
from momento.config import Configuration

try:
    from momento.internal._utilities import _validate_request_timeout
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

from momento.responses import (
    CacheDeleteResponse,
    CacheGetResponse,
    CacheSetResponse,
    CreateCacheResponse,
    DeleteCacheResponse,
    ListCachesResponse,
)


class SimpleCacheClient:
    """Synchronous Simple Cache Client"""

    def __init__(
        self,
        configuration: Configuration,
        credential_provider: CredentialProvider,
        default_ttl: timedelta,
    ):
        """Creates a synchronous SimpleCacheClient

        Args:
            configuration (Configuration): An object holding configuration settings for communication with the server.
            credential_provider (CredentialProvider): An object holding the auth token and endpoint information.
            default_ttl (timedelta): A default Time To Live timedelta for cache objects created by this client.
                It is possible to override this setting when calling the set method.
        Raises:
            IllegalArgumentException: If method arguments fail validations.
        """
        self._logger = logs.logger
        _validate_request_timeout(configuration.get_transport_strategy().get_grpc_configuration().get_deadline())
        self._configuration = configuration
        self._control_client = _ScsControlClient(credential_provider)
        self._data_client = _ScsDataClient(
            configuration,
            credential_provider,
            default_ttl,
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

    def create_cache(self, cache_name: str) -> CreateCacheResponse:
        """Creates a cache if it doesn't exist.

        Args:
            cache_name (str): Name of the cache to be created.

        Returns:
            CreateCacheResponse: result of the create cache operation. This result
            is resolved to a type-safe object of one of the following subtypes:

            - `CreateCache.Success`
            - `CreateCache.AlreadyExists`
            - `CreateCache.Error`

            Pattern matching can be used to operate on the appropriate subtype.
            For example, in python 3.10+:

                match response:
                    case CreateCache.Success():
                        ...
                    case CreateCache.CacheAlreadyExists():
                        ...
                    case CreateCache.Error():
                        ...
                    case _:
                        # Shouldn't happen

            or equivalently in earlier versions of python:

                if isinstance(response, CreateCache.Success):
                    ...
                elif isinstance(response, CreateCache.AlreadyExists):
                    ...
                elif isinstance(response, CreateCache.Error):
                    ...
                else:
                    # Shouldn't happen
        """
        return self._control_client.create_cache(cache_name)

    def delete_cache(self, cache_name: str) -> DeleteCacheResponse:
        """Deletes a cache and all of the items within it.

        Args:
            cache_name (str): Name of the cache to be deleted.

        Returns:
            DeleteCacheResponse: result of the delete cache operation. This result
            is resolved to a type-safe object of one of the following subtypes:

            - `DeleteCache.Success`
            - `DeleteCache.Error`

            Pattern matching can be used to operate on the appropriate subtype.
            For example, in python 3.10+:

                match response:
                    case DeleteCache.Success():
                        ...
                    case DeleteCache.Error():
                        ...
                    case _:
                        # Shouldn't happen

            or equivalently in earlier versions of python:

                if isinstance(response, DeleteCache.Success):
                    ...
                elif isinstance(response, DeleteCache.Error):
                    ...
                else:
                    # Shouldn't happen
        """
        return self._control_client.delete_cache(cache_name)

    def list_caches(self, next_token: Optional[str] = None) -> ListCachesResponse:
        """Lists all caches.

        Args:
            next_token: A token to specify where to start paginating. This is the NextToken from a previous response.

        Returns:
            ListCachesResponse: result of the delete cache operation. This result
            is resolved to a type-safe object of one of the following subtypes:

            - `ListCaches.Success`
            - `ListCaches.Error`

            Pattern matching can be used to operate on the appropriate subtype.
            For example, in python 3.10+:

                match response:
                    case ListCaches.Success():
                        ...
                    case ListCaches.Error():
                        ...
                    case _:
                        # Shouldn't happen

            or equivalently in earlier versions of python:

                if isinstance(response, ListCaches.Success):
                    ...
                elif isinstance(response, ListCaches.Error):
                    ...
                else:
                    # Shouldn't happen
        """
        return self._control_client.list_caches(next_token)

    def set(
        self,
        cache_name: str,
        key: Union[str, bytes],
        value: Union[str, bytes],
        ttl: Optional[timedelta] = None,
    ) -> CacheSetResponse:
        """Set the value in cache with a given time to live (TTL) seconds.

        Args:
            cache_name (str): Name of the cache to store the item in.
            key (Union[str, bytes]): The key to set.
            value (Union[str, bytes]): The value to be stored.
            ttl (Optional[timedelta], optional): TTL for the item in cache.
            This TTL takes precedence over the TTL used when initializing a cache client.
            Defaults to client TTL. If specified must be strictly positive.

        Returns:
            CacheSetResponse: result of the set operation. This result
            is resolved to a type-safe object of one of the following subtypes:

            - `CacheSet.Success`
            - `CacheSet.Error`

            Pattern matching can be used to operate on the appropriate subtype.
            For example, in python 3.10+:

                match response:
                    case CacheSet.Success():
                        ...
                    case CacheSet.Error():
                        ...
                    case _:
                        # Shouldn't happen

            or equivalently in earlier versions of python:

                if isinstance(response, CacheSet.Success):
                    ...
                elif isinstance(response, CacheSet.Error):
                    ...
                else:
                    # Shouldn't happen
        """
        return self._data_client.set(cache_name, key, value, ttl)

    def get(self, cache_name: str, key: Union[str, bytes]) -> CacheGetResponse:
        """Get the cache value stored for the given key.

        Args:
            cache_name (str): Name of the cache to perform the lookup in.
            key (Union[str, bytes]): The key to lookup.

        Returns:
            CacheGetResponse: the status of the get operation and the associated value. This result
            is resolved to a type-safe object of one of the following subtypes:

            - `CacheGet.Hit`
            - `CacheGet.Miss`
            - `CacheGet.Error`

            Pattern matching can be used to operate on the appropriate subtype.
            For example, in python 3.10+:

                match response:
                    case CacheGet.Hit() as hit:
                        return hit.value_string
                    case CacheGet.Miss():
                        ... # Handle miss
                    case CacheGet.Error():
                        ...

            or equivalently in earlier versions of python:

                if isinstance(response, CacheGet.Hit):
                    ...
                elif isinstance(response, CacheGet.Miss):
                    ...
                elif isinstance(response, CacheGet.Error):
                    ...
                else:
                    # Shouldn't happen
        """
        return self._data_client.get(cache_name, key)

    def delete(self, cache_name: str, key: Union[str, bytes]) -> CacheDeleteResponse:
        """Remove the key from the cache.

        Args:
            cache_name (str): Name of the cache to delete the key from.
            key (Union[str, bytes]): The key to delete.

        Returns:
            CacheDeleteResponse: result of the delete operation. This result
            is resolved to a type-safe object of one of the following subtypes:

            - `CacheDelete.Success`
            - `CacheDelete.Error`

            Pattern matching can be used to operate on the appropriate subtype.
            For example, in python 3.10+:

                match response:
                    case CacheDelete.Success():
                        ...
                    case CacheDelete.Error():
                        ...
                    case _:
                        # Shouldn't happen

            or equivalently in earlier versions of python:

                if isinstance(response, CacheDelete.Success):
                    ...
                elif isinstance(response, CacheDelete.Error):
                    ...
                else:
                    # Shouldn't happen
        """
        return self._data_client.delete(cache_name, key)

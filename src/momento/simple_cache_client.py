from __future__ import annotations

from datetime import timedelta
from types import TracebackType
from typing import Optional, Type

from momento import logs
from momento.auth import CredentialProvider
from momento.config import Configuration
from momento.requests import CollectionTtl

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
    CacheDictionaryFetchResponse,
    CacheDictionaryGetFieldResponse,
    CacheDictionaryGetFieldsResponse,
    CacheDictionaryIncrementResponse,
    CacheDictionaryRemoveFieldResponse,
    CacheDictionaryRemoveFieldsResponse,
    CacheDictionarySetFieldResponse,
    CacheDictionarySetFieldsResponse,
    CacheGetResponse,
    CacheListConcatenateBackResponse,
    CacheListConcatenateFrontResponse,
    CacheListFetchResponse,
    CacheListLengthResponse,
    CacheListPopBackResponse,
    CacheListPopFrontResponse,
    CacheListPushBackResponse,
    CacheListPushFrontResponse,
    CacheListRemoveValueResponse,
    CacheSetResponse,
    CreateCacheResponse,
    CreateSigningKeyResponse,
    DeleteCacheResponse,
    ListCachesResponse,
    ListSigningKeysResponse,
    RevokeSigningKeyResponse,
)
from momento.typing import (
    TCacheName,
    TDictionaryField,
    TDictionaryFields,
    TDictionaryItems,
    TDictionaryName,
    TDictionaryValue,
    TListName,
    TListValue,
    TListValuesInput,
    TScalarKey,
    TScalarValue,
)


class SimpleCacheClient:
    """Synchronous Simple Cache Client

    Cache and control methods return a response object unique to each request.
    The response object is resolved to a type-safe object of one of several
    sub-types. See the documentation for each response type for details.

    Pattern matching can be used to operate on the appropriate subtype.
    For example, in python 3.10+ if you're deleting a key:

        response = client.delete(cache_name, key)
        match response:
            case CacheDelete.Success():
                ...they key was deleted or not found...
            case CacheDelete.Error():
                ...there was an error trying to delete the key...

    or equivalently in earlier versions of python:

        response = client.delete(cache_name, key)
        if isinstance(response, CacheDelete.Success):
            ...
        elif isinstance(response, CacheDelete.Error):
            ...
        else:
            raise Exception("This should never happen")
    """

    _NUM_CLIENTS = 1
    """(async client only) For high load, we might get better performance with multiple clients,
    because the server is configured to allow a max of 100 streams per connection.
`
    In the javascript SDK, multiple clients resulted in an obvious performance improvement.
    However, in the python SDK I have not yet been able to observe a clear benefit.
    So for now, we are putting the plumbing in place so that we can more easily test performance
    with multiple connections in the future, but we are leaving the default value set to 1.

    We are hard-coding the value for now, because we haven't yet designed the API for
    users to use to configure tunables:
    https://github.com/momentohq/dev-eco-issue-tracker/issues/85
    """

    def __init__(self, configuration: Configuration, credential_provider: CredentialProvider, default_ttl: timedelta):
        """Instantiate a client.

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
        self._cache_endpoint = credential_provider.cache_endpoint
        self._data_clients = [
            _ScsDataClient(configuration, credential_provider, default_ttl)
            for _ in range(SimpleCacheClient._NUM_CLIENTS)
        ]

    def __enter__(self) -> SimpleCacheClient:
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        self._control_client.close()
        for data_client in self._data_clients:
            data_client.close()

    def create_cache(self, cache_name: str) -> CreateCacheResponse:
        """Creates a cache if it doesn't exist.

        Args:
            cache_name (str): Name of the cache to be created.

        Returns:
            CreateCacheResponse:
        """
        return self._control_client.create_cache(cache_name)

    def delete_cache(self, cache_name: str) -> DeleteCacheResponse:
        """Deletes a cache and all of the items within it.

        Args:
            cache_name (str): Name of the cache to be deleted.

        Returns:
            DeleteCacheResponse:
        """
        return self._control_client.delete_cache(cache_name)

    def list_caches(self, next_token: Optional[str] = None) -> ListCachesResponse:
        """Lists all caches.

        Args:
            next_token: A token to specify where to start paginating. This is the NextToken from a previous response.

        Returns:
            ListCachesResponse:
        """
        return self._control_client.list_caches(next_token)

    def create_signing_key(self, ttl: timedelta) -> CreateSigningKeyResponse:
        """Creates a Momento signing key

        Args:
            ttl: The key's time-to-live represented as a timedelta

        Returns:
            CreateSigningKeyResponse

        Raises:
            SdkException: validation, server-side, or other runtime error
        """
        return self._control_client.create_signing_key(ttl, self._cache_endpoint)

    def revoke_signing_key(self, key_id: str) -> RevokeSigningKeyResponse:
        """Revokes a Momento signing key, all tokens signed by which will be invalid

        Args:
            key_id: The id of the Momento signing key to revoke

        Returns:
            RevokeSigningKeyResponse

        Raises:
            SdkException: validation, server-side, or other runtime error
        """
        return self._control_client.revoke_signing_key(key_id)

    def list_signing_keys(self, next_token: Optional[str] = None) -> ListSigningKeysResponse:
        """Lists all Momento signing keys for the provided auth token.

        Args:
            next_token: Token to continue paginating through the list. It's used to handle large paginated lists.

        Returns:
            ListSigningKeysResponse

        Raises:
            SdkException: validation, server-side, or other runtime error
        """
        return self._control_client.list_signing_keys(self._cache_endpoint, next_token)

    def set(
        self,
        cache_name: str,
        key: TScalarKey,
        value: TScalarValue,
        ttl: Optional[timedelta] = None,
    ) -> CacheSetResponse:
        """Set the value in cache with a given time to live (TTL) seconds.

        Args:
            cache_name (str): Name of the cache to store the item in.
            key (TScalarKey): The key to set.
            value (TScalarValue): The value to be stored.
            ttl (Optional[timedelta], optional): TTL for the item in cache.
            This TTL takes precedence over the TTL used when initializing a cache client.
            Defaults to client TTL. If specified must be strictly positive.

        Returns:
            CacheSetResponse:
        """
        return self._data_client.set(cache_name, key, value, ttl)

    def get(self, cache_name: str, key: TScalarKey) -> CacheGetResponse:
        """Get the cache value stored for the given key.

        Args:
            cache_name (str): Name of the cache to perform the lookup in.
            key (TScalarKey): The key to lookup.

        Returns:
            CacheGetResponse:
        """
        return self._data_client.get(cache_name, key)

    def delete(self, cache_name: str, key: TScalarKey) -> CacheDeleteResponse:
        """Remove the key from the cache.

        Args:
            cache_name (str): Name of the cache to delete the key from.
            key (TScalarKey): The key to delete.

        Returns:
            CacheDeleteResponse:
        """
        return self._data_client.delete(cache_name, key)

    # DICTIONARY COLLECTION METHODS
    def dictionary_fetch(
        self, cache_name: TCacheName, dictionary_name: TDictionaryName
    ) -> CacheDictionaryFetchResponse:
        """Fetch the entire dictionary from the cache.

        Args:
            cache_name (TCacheName): Name of the cache to perform the lookup in.
            dictionary_name (TDictionaryName): The name of the dictionary to fetch.

        Returns:
            CacheDictionaryFetchResponse: result of the fetch operation and the associated dictionary.
        """
        pass

    def dictionary_get_field(
        self, cache_name: TCacheName, dictionary_name: TDictionaryName, field: TDictionaryField
    ) -> CacheDictionaryGetFieldResponse:
        """Get the cache value stored for the given dictionary and field.

        Args:
            cache_name (TCacheName): Name of the cache to perform the lookup in.
            dictionary_name (TDictionaryName): The dictionary to lookup.
            field (TDictionaryField): The field in the dictionary to lookup.

        Returns:
            CacheDictionaryGetFieldResponse: the status and value for the field.
        """
        pass

    def dictionary_get_fields(
        self, cache_name: TCacheName, dictionary_name: TDictionaryName, fields: TDictionaryFields
    ) -> CacheDictionaryGetFieldsResponse:
        """Get several values from a dictionary.

        Args:
            cache_name (TCacheName): Name of the cache to perform the lookup in.
            dictionary_name (TDictionaryName): The dictionary to lookup.
            fields (TDictionaryFields): The fields in the dictionary to lookup.

        Returns:
            CacheDictionaryGetFieldsResponse: the status and associated value for each field.
        """
        pass

    def dictionary_increment(
        self,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
        field: TDictionaryField,
        amount: int = 1,
        ttl: CollectionTtl = CollectionTtl.from_cache_ttl(),
    ) -> CacheDictionaryIncrementResponse:
        """Add an integer quantity to a dictionary value.

        Args:
            cache_name (TCacheName): Name of the cache to store the dictionary in.
            dictionary_name (TDictionaryName): Name of the dictionary to increment in.
            field (TDictionaryField): The field to increment.
            amount (int, optional): The quantity to add to the value. May be positive, negative, or zero. Defaults to 1.
            ttl (CollectionTtl, optional): TTL for the dictionary in cache. This TTL takes precedence over the TTL
                used when initializing a cache client. Defaults to client TTL.
                Defaults to CollectionTtl.from_cache_ttl().

        Returns:
            CacheDictionaryIncrementResponse: result of the increment operation.
        """
        pass

    def dictionary_remove_field(
        self, cache_name: TCacheName, dictionary_name: TDictionaryName, field: TDictionaryField
    ) -> CacheDictionaryRemoveFieldResponse:
        """Remove a field from a dictionary.

        Performs a no-op if the dictionary or field do not exist.

        Args:
            cache_name (TCacheName): Name of the cache to perform the lookup in.
            dictionary_name (TDictionaryName): Name of the dictionary to remove the field from.
            field (TDictionaryField): Name of the field to remove from the dictionary.

        Returns:
            CacheDictionaryRemoveFieldResponse: result of the remove operation.
        """
        pass

    def dictionary_remove_fields(
        self, cache_name: TCacheName, dictionary_name: TDictionaryName, fields: TDictionaryFields
    ) -> CacheDictionaryRemoveFieldsResponse:
        """Remove fields from a dictionary.

        Performs a no-op if the dictionary or a particular field does not exist.

        Args:
            cache_name (TCacheName): Name of the cache to perform the lookup in.
            dictionary_name (TDictionaryName): Name of the dictionary to remove the fields from.
            fields (TDictionaryFields): The fields to remove from the dictionary.

        Returns:
            CacheDictionaryRemoveFieldsResponse: result of the remove fields operation.
        """
        pass

    def dictionary_set_field(
        self,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
        field: TDictionaryField,
        value: TDictionaryValue,
        ttl: CollectionTtl = CollectionTtl.from_cache_ttl(),
    ) -> CacheDictionarySetFieldResponse:
        """Set the dictionary field to a value with a given time to live (TTL) seconds.

        Args:
            cache_name (TCacheName): Name of the cache to store the dictionary in.
            dictionary_name (TDictionaryName): Name of the dictionary to set.
            field (TDictionaryField): The field in the dictionary to set.
            value (TDictionaryValue): The value to be stored.
            ttl (CollectionTtl, optional): TTL for the dictionary in cache.
                This TTL takes precedence over the TTL used when initializing a cache client.
                Defaults to CollectionTtl.from_cache_ttl().

        Returns:
            CacheDictionarySetFieldResponse: result of the set operation.
        """
        pass

    def dictionary_set_fields(
        self,
        cache_name: TCacheName,
        dictionary_name: TDictionaryName,
        items: TDictionaryItems,
        ttl: CollectionTtl = CollectionTtl.from_cache_ttl(),
    ) -> CacheDictionarySetFieldsResponse:
        """Set several dictionary field-value pairs in the cache.

        Args:
            cache_name (TCacheName): Name of the cache to perform the lookup in.
            dictionary_name (TDictionaryName): Name of the dictionary to set.
            items (TDictionaryItems): Field value pairs to store.
            ttl (CollectionTtl, optional): TTL for the dictionary in cache.
                This TTL takes precedence over the TTL used when initializing a cache client.
                Defaults to CollectionTtl.from_cache_ttl().

        Returns:
            CacheDictionarySetFieldsResponse: result of the set fields operation.
        """
        pass

    # LIST COLLECTION METHODS
    def list_concatenate_back(
        self,
        cache_name: TCacheName,
        list_name: TListName,
        values: TListValuesInput,
        ttl: CollectionTtl = CollectionTtl.from_cache_ttl(),
        truncate_front_to_size: Optional[int] = None,
    ) -> CacheListConcatenateBackResponse:
        """
        Add values to the end of the list.

        Args:
            cache_name (TCacheName): The cache where the list is.
            list_name (TListName): The name of the list to concatenate.
            values: (TListValuesInput): The values to concatenate.
            ttl: (CollectionTtl): How to treat the list's TTL. Defaults to `CollectionTtl.from_cache_ttl()`
            truncate_front_to_size (Optional[int]): If the list exceeds this size, remove values from
                                                    the start of the list.

        Returns:
            CacheListConcatenateBackResponse:
        """

        return self._data_client.list_concatenate_back(cache_name, list_name, values, ttl, truncate_front_to_size)

    def list_concatenate_front(
        self,
        cache_name: TCacheName,
        list_name: TListName,
        values: TListValuesInput,
        ttl: CollectionTtl = CollectionTtl.from_cache_ttl(),
        truncate_back_to_size: Optional[int] = None,
    ) -> CacheListConcatenateFrontResponse:
        """
        Add values to the start of the list.

        Args:
            cache_name (TCacheName): The cache where the list is.
            list_name (TListName): The name of the list to concatenate.
            values: (TListValuesInput): The values to concatenate.
            ttl: (CollectionTtl): How to treat the list's TTL. Defaults to `CollectionTtl.from_cache_ttl()`
            truncate_back_to_size (Optional[int]): If the list exceeds this size, remove values from
                                                   the end of the list.

        Returns:
            CacheListConcatenateFrontResponse:
        """

        return self._data_client.list_concatenate_front(cache_name, list_name, values, ttl, truncate_back_to_size)

    def list_fetch(self, cache_name: TCacheName, list_name: TListName) -> CacheListFetchResponse:
        """
        Gets all values from the list.

        Args:
            cache_name (TCacheName): The cache where the list is.
            list_name (TListName): The name of the list to fetch.

        Returns:
            CacheListFetchResponse:
        """

        return self._data_client.list_fetch(cache_name, list_name)

    def list_length(self, cache_name: TCacheName, list_name: TListName) -> CacheListLengthResponse:
        """
        Gets the number of values in the list.

        Args:
            cache_name (TCacheName): The cache where the list is.
            list_name (TListName): The name of the list to fetch.

        Returns:
            CacheListLengthResponse:
        """

        return self._data_client.list_length(cache_name, list_name)

    def list_pop_back(self, cache_name: TCacheName, list_name: TListName) -> CacheListPopBackResponse:
        """
        Gets removes and returns the last value from the list.

        Args:
            cache_name (TCacheName): The cache where the list is.
            list_name (TListName): The name of the list to fetch.

        Returns:
            CacheListPopBackResponse:
        """

        return self._data_client.list_pop_back(cache_name, list_name)

    def list_pop_front(self, cache_name: TCacheName, list_name: TListName) -> CacheListPopFrontResponse:
        """
        Gets removes and returns the first value from the list.

        Args:
            cache_name (TCacheName): The cache where the list is.
            list_name (TListName): The name of the list to fetch.

        Returns:
            CacheListPopFrontResponse:
        """

        return self._data_client.list_pop_front(cache_name, list_name)

    def list_push_back(
        self,
        cache_name: TCacheName,
        list_name: TListName,
        value: TListValue,
        ttl: CollectionTtl = CollectionTtl.from_cache_ttl(),
        truncate_front_to_size: Optional[int] = None,
    ) -> CacheListPushBackResponse:
        """
        Add values to the end of the list.

        Args:
            cache_name (TCacheName): The cache where the list is.
            list_name (TListName): The name of the list to push to.
            values: (TListValuesInput): The values to push.
            ttl: (CollectionTtl): How to treat the list's TTL. Defaults to `CollectionTtl.from_cache_ttl()`
            truncate_front_to_size (Optional[int]): If the list exceeds this size, remove values from
                                                    the start of the list.

        Returns:
            CacheListPushBackResponse:
        """

        return self._data_client.list_push_back(cache_name, list_name, value, ttl, truncate_front_to_size)

    def list_push_front(
        self,
        cache_name: TCacheName,
        list_name: TListName,
        value: TListValue,
        ttl: CollectionTtl = CollectionTtl.from_cache_ttl(),
        truncate_back_to_size: Optional[int] = None,
    ) -> CacheListPushFrontResponse:
        """
        Add values to the start of the list.

        Args:
            cache_name (TCacheName): The cache where the list is.
            list_name (TListName): The name of the list to push to.
            values: (TListValuesInput): The values to push.
            ttl: (CollectionTtl): How to treat the list's TTL. Defaults to `CollectionTtl.from_cache_ttl()`
            truncate_back_to_size (Optional[int]): If the list exceeds this size, remove values from
                                                   the end of the list.

        Returns:
            CacheListPushFrontResponse:
        """

        return self._data_client.list_push_front(cache_name, list_name, value, ttl, truncate_back_to_size)

    def list_remove_value(
        self,
        cache_name: TCacheName,
        list_name: TListName,
        value: TListValue,
    ) -> CacheListRemoveValueResponse:
        """
        Removes all matching values from the list.

        Example:
            client.list_concatenate_front(cache_name, list_name, ['up', 'up', 'down', 'down', 'left', 'right'])
            client.list_remove_value(cache_name, list_name, 'up')
            fetch_resp = client.list_fetch(cache_name, list_name)

            # ['down', 'down', 'left', 'right']
            print(fetch_resp.values_string)

        Args:
            cache_name (TCacheName): The cache where the list is.
            list_name (TListName): The name of the list to remove values from.
            value: (TListValue): The value to remove.
        """

        return self._data_client.list_remove_value(cache_name, list_name, value)

    # SET COLLECTION METHODS

    @property
    def _data_client(self) -> _ScsDataClient:
        client = self._data_clients[self._next_client_index]
        self._next_client_index = (self._next_client_index + 1) % len(self._data_clients)
        return client

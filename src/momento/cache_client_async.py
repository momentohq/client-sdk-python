from __future__ import annotations

from datetime import timedelta
from types import TracebackType
from typing import Iterable, Optional, Type

from momento import logs
from momento.auth import CredentialProvider
from momento.config import Configuration
from momento.errors import UnknownException
from momento.requests import CollectionTtl, SortOrder
from momento.responses.data.sorted_set.fetch import CacheSortedSetFetchResponse
from momento.responses.data.sorted_set.get_rank import CacheSortedSetGetRankResponse
from momento.responses.data.sorted_set.get_score import (
    CacheSortedSetGetScore,
    CacheSortedSetGetScoreResponse,
)
from momento.responses.data.sorted_set.get_scores import (
    CacheSortedSetGetScores,
    CacheSortedSetGetScoresResponse,
)
from momento.responses.data.sorted_set.increment import CacheSortedSetIncrementResponse
from momento.responses.data.sorted_set.put_element import (
    CacheSortedSetPutElement,
    CacheSortedSetPutElementResponse,
)
from momento.responses.data.sorted_set.put_elements import (
    CacheSortedSetPutElements,
    CacheSortedSetPutElementsResponse,
)
from momento.responses.data.sorted_set.remove_element import (
    CacheSortedSetRemoveElement,
    CacheSortedSetRemoveElementResponse,
)
from momento.responses.data.sorted_set.remove_elements import (
    CacheSortedSetRemoveElements,
    CacheSortedSetRemoveElementsResponse,
)

try:
    from momento.internal._utilities import _validate_request_timeout
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
    CacheDeleteResponse,
    CacheDictionaryFetchResponse,
    CacheDictionaryGetField,
    CacheDictionaryGetFieldResponse,
    CacheDictionaryGetFields,
    CacheDictionaryGetFieldsResponse,
    CacheDictionaryIncrementResponse,
    CacheDictionaryRemoveField,
    CacheDictionaryRemoveFieldResponse,
    CacheDictionaryRemoveFields,
    CacheDictionaryRemoveFieldsResponse,
    CacheDictionarySetField,
    CacheDictionarySetFieldResponse,
    CacheDictionarySetFields,
    CacheDictionarySetFieldsResponse,
    CacheFlushResponse,
    CacheGetResponse,
    CacheIncrementResponse,
    CacheListConcatenateBackResponse,
    CacheListConcatenateFrontResponse,
    CacheListFetchResponse,
    CacheListLengthResponse,
    CacheListPopBackResponse,
    CacheListPopFrontResponse,
    CacheListPushBackResponse,
    CacheListPushFrontResponse,
    CacheListRemoveValueResponse,
    CacheSetAddElement,
    CacheSetAddElementResponse,
    CacheSetAddElements,
    CacheSetAddElementsResponse,
    CacheSetFetchResponse,
    CacheSetIfNotExistsResponse,
    CacheSetRemoveElement,
    CacheSetRemoveElementResponse,
    CacheSetRemoveElements,
    CacheSetRemoveElementsResponse,
    CacheSetResponse,
    CreateCacheResponse,
    CreateSigningKeyResponse,
    DeleteCacheResponse,
    ListCachesResponse,
    ListSigningKeysResponse,
    RevokeSigningKeyResponse,
)
from momento.typing import TDictionaryItems, TSortedSetElements


class CacheClientAsync:
    """Async Cache Client.

    Cache and control methods return a response object unique to each request.
    The response object is resolved to a type-safe object of one of several
    sub-types. See the documentation for each response type for details.

    Pattern matching can be used to operate on the appropriate subtype.
    For example, in python 3.10+ if you're deleting a key::

        response = await client.delete(cache_name, key)
        match response:
            case CacheDelete.Success():
                ...they key was deleted or not found...
            case CacheDelete.Error():
                ...there was an error trying to delete the key...

    or equivalently in earlier versions of python::

        response = await client.delete(cache_name, key)
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
        Example::

            from datetime import timedelta
            from momento import Configurations, CredentialProvider, CacheClientAsync

            configuration = Configurations.Laptop.latest()
            credential_provider = CredentialProvider.from_environment_variable("MOMENTO_AUTH_TOKEN")
            ttl_seconds = timedelta(seconds=60)
            client = CacheClientAsync(configuration, credential_provider, ttl_seconds)
        """
        _validate_request_timeout(configuration.get_transport_strategy().get_grpc_configuration().get_deadline())
        self._logger = logs.logger
        self._next_client_index = 0
        self._control_client = _ScsControlClient(configuration, credential_provider)
        self._cache_endpoint = credential_provider.cache_endpoint
        self._data_clients = [
            _ScsDataClient(configuration, credential_provider, default_ttl)
            for _ in range(CacheClientAsync._NUM_CLIENTS)
        ]

    async def __aenter__(self) -> CacheClientAsync:
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

    async def create_cache(self, cache_name: str) -> CreateCacheResponse:
        """Creates a cache if it doesn't exist.

        Args:
            cache_name (str): Name of the cache to be created.

        Returns:
            CreateCacheResponse: The result of a Create Cache operation.
        """
        return await self._control_client.create_cache(cache_name)

    async def delete_cache(self, cache_name: str) -> DeleteCacheResponse:
        """Deletes a cache and all of the items within it.

        Args:
            cache_name (str): Name of the cache to be deleted.

        Returns:
            DeleteCacheResponse: The result of a Delete Cache operation.
        """
        return await self._control_client.delete_cache(cache_name)

    async def flush_cache(self, cache_name: str) -> CacheFlushResponse:
        """Flushes a cache and empties it of all its items.

        Args:
            cache_name (str): Name of the cache to be flushed.

        Returns:
            CacheFlushResponse: The result of a Flush Cache operation.
        """
        return await self._control_client.flush(cache_name)

    async def list_caches(self) -> ListCachesResponse:
        """Lists all caches.

        Returns:
            ListCachesResponse: The result of a list cache operation.
        """
        return await self._control_client.list_caches()

    async def create_signing_key(self, ttl: timedelta) -> CreateSigningKeyResponse:
        """Creates a Momento signing key.

        Args:
            ttl (timedelta): The key's time-to-live represented as a timedelta

        Returns:
            CreateSigningKeyResponse: The result of a Create Signing key operation.
        """
        return await self._control_client.create_signing_key(ttl, self._cache_endpoint)

    async def revoke_signing_key(self, key_id: str) -> RevokeSigningKeyResponse:
        """Revokes a Momento signing key, all tokens signed by which will be invalid.

        Args:
            key_id (str): The id of the Momento signing key to revoke

        Returns:
            RevokeSigningKeyResponse: The result of a Revoke Signing Key operation.
        """
        return await self._control_client.revoke_signing_key(key_id)

    async def list_signing_keys(self) -> ListSigningKeysResponse:
        """Lists all Momento signing keys for the provided auth token.

        Returns:
            ListSigningKeysResponse: The result of a List Signing keys operation.
        """
        return await self._control_client.list_signing_keys(self._cache_endpoint)

    async def increment(
        self,
        cache_name: str,
        key: str | bytes,
        amount: int = 1,
        ttl: Optional[timedelta] = None,
    ) -> CacheIncrementResponse:
        """Add to a value.

        Args:
            cache_name (str): Name of the cache to store the item in.
            key (str|bytes): The key to set.
            amount (int, optional): The quantity to add to the value. Defaults to 1.
            ttl (Optional[timedelta], optional): TTL for the item in cache.
            This TTL takes precedence over the TTL used when initializing a cache client.
            Defaults to client TTL. If specified must be strictly positive.

        Returns:
            CacheIncrementResponse
        """
        return await self._data_client.increment(cache_name, key, amount, ttl)

    async def set(
        self,
        cache_name: str,
        key: str | bytes,
        value: str | bytes,
        ttl: Optional[timedelta] = None,
    ) -> CacheSetResponse:
        """Set the value in cache with a given time to live (TTL) seconds.

        Args:
            cache_name (str): Name of the cache to store the item in.
            key (str | bytes): The key to set.
            value (str | bytes): The value to be stored.
            ttl (Optional[timedelta], optional): TTL for the item in cache.
            This TTL takes precedence over the TTL used when initializing a cache client.
            Defaults to client TTL. If specified must be strictly positive.

        Returns:
            CacheSetResponse:
        """
        return await self._data_client.set(cache_name, key, value, ttl)

    async def set_if_not_exists(
        self,
        cache_name: str,
        key: str | bytes,
        value: str | bytes,
        ttl: Optional[timedelta] = None,
    ) -> CacheSetIfNotExistsResponse:
        """Like `set`, but it will only set if the key does not already exist.

        Args:
            cache_name (str): Name of the cache to store the item in.
            key (str | bytes): The key to set.
            value (str | bytes): The value to be stored if the key does not exist.
            ttl (Optional[timedelta], optional): TTL for the item in cache.
            This TTL takes precedence over the TTL used when initializing a cache client.
            Defaults to client TTL. If specified must be strictly positive.

        Returns:
            CacheSetIfNotExistsResponse:
        """
        return await self._data_client.set_if_not_exists(cache_name, key, value, ttl)

    async def get(self, cache_name: str, key: str | bytes) -> CacheGetResponse:
        """Get the cache value stored for the given key.

        Args:
            cache_name (str): Name of the cache to perform the lookup in.
            key (str | bytes): The key to lookup.

        Returns:
            CacheGetResponse:
        """
        return await self._data_client.get(cache_name, key)

    async def delete(self, cache_name: str, key: str | bytes) -> CacheDeleteResponse:
        """Remove the key from the cache.

        Args:
            cache_name (str): Name of the cache to delete the key from.
            key (str | bytes): The key to delete.

        Returns:
            CacheDeleteResponse:
        """
        return await self._data_client.delete(cache_name, key)

    # DICTIONARY COLLECTION METHODS
    async def dictionary_fetch(self, cache_name: str, dictionary_name: str) -> CacheDictionaryFetchResponse:
        """Fetch the entire dictionary from the cache.

        Args:
            cache_name (str): Name of the cache to perform the lookup in.
            dictionary_name (str): The name of the dictionary to fetch.

        Returns:
            CacheDictionaryFetchResponse: result of the fetch operation and the associated dictionary.
        """
        return await self._data_client.dictionary_fetch(cache_name, dictionary_name)

    async def dictionary_get_field(
        self, cache_name: str, dictionary_name: str, field: str | bytes
    ) -> CacheDictionaryGetFieldResponse:
        """Get the cache value stored for the given dictionary and field.

        Args:
            cache_name (str): Name of the cache to perform the lookup in.
            dictionary_name (str): The dictionary to lookup.
            field (str | bytes): The field in the dictionary to lookup.

        Returns:
            CacheDictionaryGetFieldResponse: the status and value for the field.
        """
        get_fields_response = await self.dictionary_get_fields(cache_name, dictionary_name, [field])
        if isinstance(get_fields_response, CacheDictionaryGetFields.Hit):
            if len(get_fields_response.responses) == 0:
                return CacheDictionaryGetField.Error(UnknownException("Unknown get fields response had no data"))
            return get_fields_response.responses[0]
        elif isinstance(get_fields_response, CacheDictionaryGetFields.Miss):
            return CacheDictionaryGetField.Miss()
        elif isinstance(get_fields_response, CacheDictionaryGetFields.Error):
            return CacheDictionaryGetField.Error(get_fields_response.inner_exception)
        else:
            return CacheDictionaryGetField.Error(UnknownException(f"Unknown get field response: {get_fields_response}"))

    async def dictionary_get_fields(
        self, cache_name: str, dictionary_name: str, fields: Iterable[str | bytes]
    ) -> CacheDictionaryGetFieldsResponse:
        """Get several values from a dictionary.

        Args:
            cache_name (str): Name of the cache to perform the lookup in.
            dictionary_name (str): The dictionary to lookup.
            fields (Iterable[str | bytes]): The fields in the dictionary to lookup.

        Returns:
            CacheDictionaryGetFieldsResponse: the status and associated value for each field.
        """
        return await self._data_client.dictionary_get_fields(cache_name, dictionary_name, fields)

    async def dictionary_increment(
        self,
        cache_name: str,
        dictionary_name: str,
        field: str | bytes,
        amount: int = 1,
        *,
        ttl: CollectionTtl = CollectionTtl.from_cache_ttl(),
    ) -> CacheDictionaryIncrementResponse:
        """Add an integer quantity to a dictionary value.

        Args:
            cache_name (str): Name of the cache to store the dictionary in.
            dictionary_name (str): Name of the dictionary to increment in.
            field (str | bytes): The field to increment.
            amount (int, optional): The quantity to add to the value. May be positive, negative, or zero. Defaults to 1.
            ttl (CollectionTtl, optional): TTL for the dictionary in cache. This TTL takes precedence over the TTL
                used when initializing a cache client. Defaults to client TTL.
                Defaults to CollectionTtl.from_cache_ttl().

        Returns:
            CacheDictionaryIncrementResponse: result of the increment operation.
        """
        return await self._data_client.dictionary_increment(cache_name, dictionary_name, field, amount, ttl)

    async def dictionary_remove_field(
        self, cache_name: str, dictionary_name: str, field: str | bytes
    ) -> CacheDictionaryRemoveFieldResponse:
        """Remove a field from a dictionary.

        Performs a no-op if the dictionary or field do not exist.

        Args:
            cache_name (str): Name of the cache to perform the lookup in.
            dictionary_name (str): Name of the dictionary to remove the field from.
            field (str | bytes): Name of the field to remove from the dictionary.

        Returns:
            CacheDictionaryRemoveFieldResponse: result of the remove operation.
        """
        remove_fields_response = await self.dictionary_remove_fields(cache_name, dictionary_name, fields=[field])
        if isinstance(remove_fields_response, CacheDictionaryRemoveFields.Success):
            return CacheDictionaryRemoveField.Success()
        elif isinstance(remove_fields_response, CacheDictionaryRemoveFields.Error):
            return CacheDictionaryRemoveField.Error(remove_fields_response.inner_exception)
        else:
            return CacheDictionaryRemoveField.Error(
                UnknownException(f"Unknown remove fields response: {remove_fields_response}")
            )

    async def dictionary_remove_fields(
        self, cache_name: str, dictionary_name: str, fields: Iterable[str | bytes]
    ) -> CacheDictionaryRemoveFieldsResponse:
        """Remove fields from a dictionary.

        Performs a no-op if the dictionary or a particular field does not exist.

        Args:
            cache_name (str): Name of the cache to perform the lookup in.
            dictionary_name (str): Name of the dictionary to remove the fields from.
            fields (Iterable[str | bytes]): The fields to remove from the dictionary.

        Returns:
            CacheDictionaryRemoveFieldsResponse: result of the remove fields operation.
        """
        return await self._data_client.dictionary_remove_fields(cache_name, dictionary_name, fields)

    async def dictionary_set_field(
        self,
        cache_name: str,
        dictionary_name: str,
        field: str | bytes,
        value: str | bytes,
        *,
        ttl: CollectionTtl = CollectionTtl.from_cache_ttl(),
    ) -> CacheDictionarySetFieldResponse:
        """Set the dictionary field to a value with a given time to live (TTL) seconds.

        Args:
            cache_name (str): Name of the cache to store the dictionary in.
            dictionary_name (str): Name of the dictionary to set.
            field (str | bytes): The field in the dictionary to set.
            value (str | bytes): The value to be stored.
            ttl (CollectionTtl, optional): TTL for the dictionary in cache.
                This TTL takes precedence over the TTL used when initializing a cache client.
                Defaults to CollectionTtl.from_cache_ttl().

        Returns:
            CacheDictionarySetFieldResponse: result of the set operation.
        """
        set_fields_response = await self.dictionary_set_fields(
            cache_name, dictionary_name, items={field: value}, ttl=ttl
        )
        if isinstance(set_fields_response, CacheDictionarySetFields.Success):
            return CacheDictionarySetField.Success()
        elif isinstance(set_fields_response, CacheDictionarySetFields.Error):
            return CacheDictionarySetField.Error(set_fields_response.inner_exception)
        else:
            return CacheDictionarySetField.Error(
                UnknownException(f"Unknown set fields response: {set_fields_response}")
            )

    async def dictionary_set_fields(
        self,
        cache_name: str,
        dictionary_name: str,
        items: TDictionaryItems,
        *,
        ttl: CollectionTtl = CollectionTtl.from_cache_ttl(),
    ) -> CacheDictionarySetFieldsResponse:
        """Set several dictionary field-value pairs in the cache.

        Args:
            cache_name (str): Name of the cache to perform the lookup in.
            dictionary_name (str): Name of the dictionary to set.
            items (TDictionaryItems): Field value pairs to store.
            ttl (CollectionTtl, optional): TTL for the dictionary in cache.
                This TTL takes precedence over the TTL used when initializing a cache client.
                Defaults to CollectionTtl.from_cache_ttl().

        Returns:
            CacheDictionarySetFieldsResponse: result of the set fields operation.
        """
        return await self._data_client.dictionary_set_fields(cache_name, dictionary_name, items, ttl)

    # LIST COLLECTION METHODS
    async def list_concatenate_back(
        self,
        cache_name: str,
        list_name: str,
        values: Iterable[str | bytes],
        *,
        ttl: CollectionTtl = CollectionTtl.from_cache_ttl(),
        truncate_front_to_size: Optional[int] = None,
    ) -> CacheListConcatenateBackResponse:
        """Add values to the end of the list.

        Args:
            cache_name (str): The cache where the list is.
            list_name (str): The name of the list to concatenate.
            values: (Iterable[str | bytes]): The values to concatenate.
            ttl: (CollectionTtl, optional): How to treat the list's TTL. Defaults to `CollectionTtl.from_cache_ttl()`
            truncate_front_to_size (Optional[int]): If the list exceeds this size, remove values from
            the start of the list.

        Returns:
            CacheListConcatenateBackResponse:
        """
        return await self._data_client.list_concatenate_back(cache_name, list_name, values, ttl, truncate_front_to_size)

    async def list_concatenate_front(
        self,
        cache_name: str,
        list_name: str,
        values: Iterable[str | bytes],
        *,
        ttl: CollectionTtl = CollectionTtl.from_cache_ttl(),
        truncate_back_to_size: Optional[int] = None,
    ) -> CacheListConcatenateFrontResponse:
        """Add values to the start of the list.

        Args:
            cache_name (str): The cache where the list is.
            list_name (str): The name of the list to concatenate.
            values: (Iterable[str | bytes]): The values to concatenate.
            ttl: (CollectionTtl, optional): How to treat the list's TTL. Defaults to `CollectionTtl.from_cache_ttl()`
            truncate_back_to_size (Optional[int], optional): If the list exceeds this size, remove values from
                the end of the list.

        Returns:
            CacheListConcatenateFrontResponse:
        """
        return await self._data_client.list_concatenate_front(cache_name, list_name, values, ttl, truncate_back_to_size)

    async def list_fetch(self, cache_name: str, list_name: str) -> CacheListFetchResponse:
        """Gets all values from the list.

        Args:
            cache_name (str): The cache where the list is.
            list_name (str): The name of the list to fetch.

        Returns:
            CacheListFetchResponse:
        """
        return await self._data_client.list_fetch(cache_name, list_name)

    async def list_length(self, cache_name: str, list_name: str) -> CacheListLengthResponse:
        """Gets the number of values in the list.

        Args:
            cache_name (str): The cache where the list is.
            list_name (str): The name of the list to fetch.

        Returns:
            CacheListLengthResponse:
        """
        return await self._data_client.list_length(cache_name, list_name)

    async def list_pop_back(self, cache_name: str, list_name: str) -> CacheListPopBackResponse:
        """Gets, removes, and returns the last value from the list.

        Args:
            cache_name (str): The cache where the list is.
            list_name (str): The name of the list to fetch.

        Returns:
            CacheListPopBackResponse:
        """
        return await self._data_client.list_pop_back(cache_name, list_name)

    async def list_pop_front(self, cache_name: str, list_name: str) -> CacheListPopFrontResponse:
        """Gets, removes, and returns the first value from the list.

        Args:
            cache_name (str): The cache where the list is.
            list_name (str): The name of the list to fetch.

        Returns:
            CacheListPopFrontResponse:
        """
        return await self._data_client.list_pop_front(cache_name, list_name)

    async def list_push_back(
        self,
        cache_name: str,
        list_name: str,
        value: str | bytes,
        *,
        ttl: CollectionTtl = CollectionTtl.from_cache_ttl(),
        truncate_front_to_size: Optional[int] = None,
    ) -> CacheListPushBackResponse:
        """Add values to the end of the list.

        Args:
            cache_name (str): The cache where the list is.
            list_name (str): The name of the list to push to.
            value: (str | bytes): The value to push.
            ttl: (CollectionTtl, optional): How to treat the list's TTL. Defaults to `CollectionTtl.from_cache_ttl()`
            truncate_front_to_size (Optional[int]): If the list exceeds this size, remove values from
            the start of the list.

        Returns:
            CacheListPushBackResponse:
        """
        return await self._data_client.list_push_back(cache_name, list_name, value, ttl, truncate_front_to_size)

    async def list_push_front(
        self,
        cache_name: str,
        list_name: str,
        value: str | bytes,
        *,
        ttl: CollectionTtl = CollectionTtl.from_cache_ttl(),
        truncate_back_to_size: Optional[int] = None,
    ) -> CacheListPushFrontResponse:
        """Add values to the start of the list.

        Args:
            cache_name (str): The cache where the list is.
            list_name (str): The name of the list to push to.
            value: (str | bytes): The value to push.
            ttl: (CollectionTtl, optional): How to treat the list's TTL. Defaults to `CollectionTtl.from_cache_ttl()`
            truncate_back_to_size (Optional[int]): If the list exceeds this size, remove values from
                the end of the list.

        Returns:
            CacheListPushFrontResponse:
        """
        return await self._data_client.list_push_front(cache_name, list_name, value, ttl, truncate_back_to_size)

    async def list_remove_value(
        self,
        cache_name: str,
        list_name: str,
        value: str | bytes,
    ) -> CacheListRemoveValueResponse:
        """Removes all matching values from the list.

        Example:
            await client.list_concatenate_front(cache_name, list_name, ['up', 'up', 'down', 'down', 'left', 'right'])
            await client.list_remove_value(cache_name, list_name, 'up')
            fetch_resp = client.list_fetch(cache_name, list_name)

            # ['down', 'down', 'left', 'right']
            print(fetch_resp.values_string)

        Args:
            cache_name (str): The cache where the list is.
            list_name (str): The name of the list to remove values from.
            value: (str | bytes): The value to remove.

        Returns:
            CacheListRemoveValueResponse
        """
        return await self._data_client.list_remove_value(cache_name, list_name, value)

    # SET COLLECTION METHODS
    async def set_add_element(
        self,
        cache_name: str,
        set_name: str,
        element: str | bytes,
        *,
        ttl: CollectionTtl = CollectionTtl.from_cache_ttl(),
    ) -> CacheSetAddElementResponse:
        """Adds an element to a set.

        Args:
            cache_name (str): The cache name with the set.
            set_name (str): The name of the set to add to.
            element (str | bytes): The element to add.
            ttl: (CollectionTtl, optional): How to treat the set's TTL. Defaults to `CollectionTtl.from_cache_ttl()`

        Returns:
            CacheSetAddElementResponse
        """
        resp = await self.set_add_elements(cache_name, set_name, (element,), ttl=ttl)

        if isinstance(resp, CacheSetAddElements.Success):
            return CacheSetAddElement.Success()
        elif isinstance(resp, CacheSetAddElements.Error):
            return CacheSetAddElement.Error(resp.inner_exception)
        else:
            raise UnknownException(f"Unknown response type: {type(resp)}")

    async def set_add_elements(
        self,
        cache_name: str,
        set_name: str,
        elements: Iterable[str | bytes],
        *,
        ttl: CollectionTtl = CollectionTtl.from_cache_ttl(),
    ) -> CacheSetAddElementsResponse:
        """Add an elements to a set.

        Args:
            cache_name (str): The cache name with the set.
            set_name (str): The name of the set to add to.
            elements (Iterable[str | bytes]): The element to add.
            ttl: (CollectionTtl, optional): How to treat the set's TTL. Defaults to `CollectionTtl.from_cache_ttl()`

        Returns:
            CacheSetAddElementsResponse
        """
        return await self._data_client.set_add_elements(cache_name, set_name, elements, ttl)

    async def set_fetch(
        self,
        cache_name: str,
        set_name: str,
    ) -> CacheSetFetchResponse:
        """Fetches a set.

        Args:
            cache_name (str): The cache name with the set.
            set_name (str): The name of the set to fetch.

        Returns:
            CacheSetFetchResponse
        """
        return await self._data_client.set_fetch(cache_name, set_name)

    async def set_remove_element(
        self, cache_name: str, set_name: str, element: str | bytes
    ) -> CacheSetRemoveElementResponse:
        """Remove an element from a set.

        Args:
            cache_name (str): The cache name with the set.
            set_name (str): The name of the set to remove from.
            element (str | bytes): The element to remove.

        Returns:
            CacheSetRemoveElementResponse
        """
        resp = await self.set_remove_elements(cache_name, set_name, (element,))
        if isinstance(resp, CacheSetRemoveElements.Success):
            return CacheSetRemoveElement.Success()
        elif isinstance(resp, CacheSetRemoveElements.Error):
            return CacheSetRemoveElement.Error(resp.inner_exception)
        else:
            raise UnknownException(f"Unknown response type: {type(resp)}")

    async def set_remove_elements(
        self, cache_name: str, set_name: str, elements: Iterable[str | bytes]
    ) -> CacheSetRemoveElementsResponse:
        """Remove elements from a set.

        Args:
            cache_name (str): The cache name with the set.
            set_name (str): The name of the set to remove from.
            elements (Iterable[str | bytes]): The element to remove.

        Returns:
            CacheSetRemoveElementsResponse
        """
        return await self._data_client.set_remove_elements(cache_name, set_name, elements)

    async def sorted_set_put_element(
        self,
        cache_name: str,
        sorted_set_name: str,
        value: str | bytes,
        score: float,
        *,
        ttl: CollectionTtl = CollectionTtl.from_cache_ttl(),
    ) -> CacheSortedSetPutElementResponse:
        """Sets a sorted set element to a value and score with a given time to live (TTL) seconds.

        Args:
            cache_name (str): Name of the cache containing the sorted set.
            sorted_set_name (str): Name of the sorted set to set.
            value (str | bytes): The value of the element to add.
            score (float): The score of the element to add.
            ttl: (CollectionTtl, optional): How to treat the sorted set's TTL.
                Defaults to `CollectionTtl.from_cache_ttl()`

        Returns:
            CacheSortedSetPutElementResponse: result of the put operation.
        """
        put_elements_response = await self.sorted_set_put_elements(
            cache_name, sorted_set_name, elements={value: score}, ttl=ttl
        )
        if isinstance(put_elements_response, CacheSortedSetPutElements.Success):
            return CacheSortedSetPutElement.Success()
        elif isinstance(put_elements_response, CacheSortedSetPutElements.Error):
            return CacheSortedSetPutElement.Error(put_elements_response.inner_exception)
        else:
            return CacheSortedSetPutElement.Error(
                UnknownException(f"Unknown put elements response: {put_elements_response}")
            )

    async def sorted_set_put_elements(
        self,
        cache_name: str,
        sorted_set_name: str,
        elements: TSortedSetElements,
        *,
        ttl: CollectionTtl = CollectionTtl.from_cache_ttl(),
    ) -> CacheSortedSetPutElementsResponse:
        """Puts elements into a sorted set.

        Args:
            cache_name (str): Name of the cache containing the sorted set.
            sorted_set_name (str): The name of the sorted set to add to.
            elements (TSortedSetElements): The elements to add.
            ttl: (CollectionTtl, optional): How to treat the sorted set's TTL.
                Defaults to `CollectionTtl.from_cache_ttl()`

        Returns:
            CacheSortedSetPutElementsResponse
        """
        return await self._data_client.sorted_set_put_elements(cache_name, sorted_set_name, elements, ttl)

    async def sorted_set_fetch_by_score(
        self,
        cache_name: str,
        sorted_set_name: str,
        min_score: Optional[float] = None,
        max_score: Optional[float] = None,
        sort_order: SortOrder = SortOrder.ASCENDING,
        offset: Optional[int] = None,
        count: Optional[int] = None,
    ) -> CacheSortedSetFetchResponse:
        """Fetches a sorted set by score range.

        Args:
            cache_name (str): Name of the cache containing the sorted set.
            sorted_set_name (str): The name of the sorted set to fetch.
            min_score (Optional[float]): The minimum score of the range to fetch from the sorted set.
                                         If None, fetches from the lowest score. Defaults to None.
            max_score (Optional[float]): The maximum score of the range to fetch from the sorted set.
                                         If None, fetches until the highest score. Defaults to None.
            sort_order (SortOrder): The sort order to use when fetching the sorted set.
                                    Defaults to SortOrder.ASCENDING.
            offset (Optional[int]): The number of elements to skip before starting to return elements.
                                    If None, starts from the first element within the specified range. Defaults to None.
            count (Optional[int]): The maximum number of elements to return.
                                   If None, returns all elements within the specified range. Defaults to None.

        Returns:
            CacheSortedSetFetchResponse: The fetched sorted set data with associated metadata.
        """
        return await self._data_client.sorted_set_fetch_by_score(
            cache_name, sorted_set_name, min_score, max_score, sort_order, offset, count
        )

    async def sorted_set_fetch_by_rank(
        self,
        cache_name: str,
        sorted_set_name: str,
        start_rank: Optional[int] = None,
        end_rank: Optional[int] = None,
        sort_order: SortOrder = SortOrder.ASCENDING,
    ) -> CacheSortedSetFetchResponse:
        """Fetches a sorted set by rank range.

        Args:
            cache_name (str): Name of the cache containing the sorted set.
            sorted_set_name (str): The name of the sorted set to fetch.
            start_rank (Optional[int]): The start rank of the range to fetch from the sorted set.
                                        If None, fetches from the beginning of the set. Defaults to None.
            end_rank (Optional[int]): The end rank of the range to fetch from the sorted set.
                                      If None, fetches until the end of the set. Defaults to None.
            sort_order (SortOrder): The sort order to use when fetching the sorted set.
                                              Defaults to SortOrder.ASCENDING.

        Returns:
            CacheSortedSetFetchResponse: The fetched sorted set data with associated metadata.
        """
        return await self._data_client.sorted_set_fetch_by_rank(
            cache_name, sorted_set_name, start_rank, end_rank, sort_order
        )

    async def sorted_set_get_score(
        self, cache_name: str, sorted_set_name: str, value: str | bytes
    ) -> CacheSortedSetGetScoreResponse:
        """Get the score stored for the given sorted set and value.

        Args:
            cache_name (str): Name of the cache to perform the lookup in.
            sorted_set_name (str): The sorted set to look up.
            value (str | bytes): The value in the sorted set to look up.

        Returns:
            CacheSortedSetGetScoreResponse: the status and score for the value.
        """
        get_scores_response = await self.sorted_set_get_scores(cache_name, sorted_set_name, [value])
        if isinstance(get_scores_response, CacheSortedSetGetScores.Hit):
            if len(get_scores_response.responses) == 0:
                return CacheSortedSetGetScore.Error(UnknownException("Unknown get scores response had no data"))
            return get_scores_response.responses[0]
        elif isinstance(get_scores_response, CacheSortedSetGetScores.Miss):
            bytes_value = value.encode("utf-8") if isinstance(value, str) else value
            return CacheSortedSetGetScore.Miss(bytes_value)
        elif isinstance(get_scores_response, CacheSortedSetGetScores.Error):
            return CacheSortedSetGetScore.Error(get_scores_response.inner_exception)
        else:
            return CacheSortedSetGetScore.Error(UnknownException(f"Unknown get scores response: {get_scores_response}"))

    async def sorted_set_get_scores(
        self, cache_name: str, sorted_set_name: str, values: Iterable[str | bytes]
    ) -> CacheSortedSetGetScoresResponse:
        """Get several scores from a sorted set.

        Args:
            cache_name (str): Name of the cache to perform the lookup in.
            sorted_set_name (str): The sorted set to look up.
            values (Iterable[str | bytes]): The values in the sorted set to look up.

        Returns:
            CacheSortedSetGetScoresResponse: the status and associated score for each value.
        """
        return await self._data_client.sorted_set_get_scores(cache_name, sorted_set_name, values)

    async def sorted_set_get_rank(
        self, cache_name: str, sorted_set_name: str, value: str | bytes, sort_order: SortOrder = SortOrder.ASCENDING
    ) -> CacheSortedSetGetRankResponse:
        """Get the rank for the given sorted set and value.

        Args:
            cache_name (str): Name of the cache to perform the lookup in.
            sorted_set_name (str): The sorted set to look up.
            value (str | bytes): The value in the sorted set to look up.
            sort_order (SortOrder): The sort order to use when determining the rank
                                    of the value in the sorted set. Defaults to SortOrder.ASCENDING.

        Returns:
            CacheSortedSetGetScoresResponse: the status and associated score for each value.
        """
        return await self._data_client.sorted_set_get_rank(cache_name, sorted_set_name, value, sort_order)

    async def sorted_set_remove_element(
        self, cache_name: str, sorted_set_name: str, value: str | bytes
    ) -> CacheSortedSetRemoveElementResponse:
        """Remove an element from a sorted set.

        Performs a no-op if the sorted set or element does not exist.

        Args:
            cache_name (str): Name of the cache to perform the lookup in.
            sorted_set_name (str): Name of the sorted set to remove the element from.
            value (str | bytes): Value of the element to remove from the sorted set.

        Returns:
            CacheSortedSetRemoveElementResponse: result of the remove operation.
        """
        remove_elements_response = await self.sorted_set_remove_elements(cache_name, sorted_set_name, values=[value])
        if isinstance(remove_elements_response, CacheSortedSetRemoveElements.Success):
            return CacheSortedSetRemoveElement.Success()
        elif isinstance(remove_elements_response, CacheSortedSetRemoveElements.Error):
            return CacheSortedSetRemoveElement.Error(remove_elements_response.inner_exception)
        else:
            return CacheSortedSetRemoveElement.Error(
                UnknownException(f"Unknown remove elements response: {remove_elements_response}")
            )

    async def sorted_set_remove_elements(
        self, cache_name: str, sorted_set_name: str, values: Iterable[str | bytes]
    ) -> CacheSortedSetRemoveElementsResponse:
        """Remove elements from a sorted set.

        Performs a no-op if the sorted or a particular element does not exist.

        Args:
            cache_name (str): Name of the cache to perform the lookup in.
            sorted_set_name (str): Name of the sorted set to remove the elements from.
            values (Iterable[str | bytes]): The values of the elements to remove from the sorted set.

        Returns:
            CacheSortedSetRemoveElementsResponse: result of the remove elements operation.
        """
        return await self._data_client.sorted_set_remove_elements(cache_name, sorted_set_name, values)

    async def sorted_set_increment(
        self,
        cache_name: str,
        sorted_set_name: str,
        value: str | bytes,
        score: float = 1.0,
        *,
        ttl: CollectionTtl = CollectionTtl.from_cache_ttl(),
    ) -> CacheSortedSetIncrementResponse:
        """Increments the score for the given sorted set and value.

        If the value doesn't exist in the sorted set, it will be added and its score set to the given score.

        Args:
            cache_name (str): Name of the cache to perform the lookup in.
            sorted_set_name (str): The sorted set to look up.
            value (str | bytes): The value in the sorted set to look up.
            score (float): The quantity to add to the score. May be positive, negative, or zero. Defaults to 1.0.
            ttl: (CollectionTtl, optional): How to treat the sorted set's TTL.
                Defaults to `CollectionTtl.from_cache_ttl()`

        Returns:
            CacheSortedSetIncrementResponse: the status and associated score for each value.
        """
        return await self._data_client.sorted_set_increment(cache_name, sorted_set_name, value, score, ttl)

    @property
    def _data_client(self) -> _ScsDataClient:
        client = self._data_clients[self._next_client_index]
        self._next_client_index = (self._next_client_index + 1) % len(self._data_clients)
        return client

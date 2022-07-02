from typing import cast, Optional, List, Union
import warnings

from .. import INCUBATING_WARNING_MSG
from ...aio.simple_cache_client import SimpleCacheClient
from ..._utilities._data_validation import _as_bytes

from ..cache_operation_types import (
    CacheGetStatus,
    CacheDictionaryGetUnaryResponse,
    CacheDictionaryGetMultiResponse,
    CacheDictionarySetUnaryResponse,
    CacheDictionarySetMultiResponse,
    CacheDictionaryGetAllResponse,
    CacheExistsResponse,
    BytesDictionary,
    DictionaryKey,
    DictionaryValue,
    Dictionary,
)
from ..._utilities._data_validation import _validate_request_timeout
from .utils import (
    convert_dict_items_to_bytes,
    deserialize_dictionary,
    serialize_dictionary,
)


class SimpleCacheClientIncubating(SimpleCacheClient):
    def __init__(
        self,
        auth_token: str,
        default_ttl_seconds: int,
        data_client_operation_timeout_ms: Optional[int],
    ):
        warnings.warn(INCUBATING_WARNING_MSG)
        super().__init__(
            auth_token, default_ttl_seconds, data_client_operation_timeout_ms
        )

    async def dictionary_set(
        self,
        cache_name: str,
        dictionary_name: str,
        key: DictionaryKey,
        value: DictionaryValue,
        ttl_seconds: Optional[int] = None,
        *,
        refresh_ttl: bool,
    ) -> CacheDictionarySetUnaryResponse:
        """Store a dictionary item in the cache.

        Inserts a `value` for `key` in into a dictionary `dictionary_name`.
        Updates (overwrites) values if the key already exists.

        Args:
            cache_name (str): Name of the cache to store the dictionary in.
            dictionary_name (str): The name of the dictionary in the cache.
            key (DictionaryKey): The key to set.
            value (DictionaryValue): The value to store.
            ttl_seconds (Optional[int], optional): Time to live in seconds for the dictionary
                as a whole.
            refresh_ttl (bool): If, when performing an update, to refresh the ttl.

        Returns:
            CacheDictionarySetUnaryResponse: data stored in the cache
        """
        dictionary_get_response = await self.get(cache_name, dictionary_name)
        cached_dictionary: BytesDictionary = {}
        if dictionary_get_response.status() == CacheGetStatus.HIT:
            cached_dictionary = deserialize_dictionary(
                cast(bytes, dictionary_get_response.value_as_bytes())
            )

        bytes_key = _as_bytes(key)
        bytes_value = _as_bytes(value)
        cached_dictionary[bytes_key] = bytes_value

        await self.set(
            cache_name, dictionary_name, serialize_dictionary(cached_dictionary)
        )
        return CacheDictionarySetUnaryResponse(
            dictionary_name=dictionary_name, key=bytes_key, value=bytes_value
        )

    async def dictionary_set_multi(
        self,
        cache_name: str,
        dictionary_name: str,
        dictionary: Dictionary,
        ttl_seconds: Optional[int] = None,
        *,
        refresh_ttl: bool,
    ) -> CacheDictionarySetMultiResponse:
        """Store dictionary items (key-value pairs) in the cache.

        Inserts items from `dictionary` into a dictionary `dictionary_name`.
        Updates (overwrites) values if the key already exists.

        Args:
            cache_name (str): Name of the cache to store the dictionary in.
            dictionary_name (str): The name of the dictionary in the cache.
            dictionary (Dictionary): The items (key-value pairs) to be stored.
            ttl_seconds (Optional[int], optional): Time to live in seconds for the dictionary
                as a whole.
            refresh_ttl (bool): If, when performing an update, to refresh the ttl.

        Returns:
            CacheDictionarySetMultiResponse: data stored in the cache
        """
        dictionary_get_response = await self.get(cache_name, dictionary_name)
        cached_dictionary: BytesDictionary = {}
        if dictionary_get_response.status() == CacheGetStatus.HIT:
            cached_dictionary = deserialize_dictionary(
                cast(bytes, dictionary_get_response.value_as_bytes())
            )

        bytes_dictionary = convert_dict_items_to_bytes(dictionary)
        cached_dictionary.update(bytes_dictionary)

        await self.set(
            cache_name, dictionary_name, serialize_dictionary(cached_dictionary)
        )
        return CacheDictionarySetMultiResponse(
            dictionary_name=dictionary_name, dictionary=bytes_dictionary
        )

    async def dictionary_get(
        self,
        cache_name: str,
        dictionary_name: str,
        key: DictionaryKey,
    ) -> CacheDictionaryGetUnaryResponse:
        """Retrieve a dictionary value from the cache.

        Args:
            cache_name (str): Name of the cache to get the dictionary from.
            dictionary_name (str): Name of the dictionary to query.
            key (DictionaryKey): The key to index in the dictionary.

        Returns:
            CacheDictionaryGetUnaryResponse: A wrapper for the value (if present)
                and status (HIT or MISS).
        """
        dictionary_get_response = await self.get(cache_name, dictionary_name)
        if dictionary_get_response.status() == CacheGetStatus.MISS:
            return CacheDictionaryGetUnaryResponse(
                value=None, status=CacheGetStatus.MISS
            )

        dictionary: BytesDictionary = deserialize_dictionary(
            cast(bytes, dictionary_get_response.value_as_bytes())
        )

        try:
            value = dictionary[_as_bytes(key, "Unsupported type for key: ")]
        except KeyError:
            return CacheDictionaryGetUnaryResponse(
                value=None, status=CacheGetStatus.MISS
            )

        return CacheDictionaryGetUnaryResponse(value=value, status=CacheGetStatus.HIT)

    async def dictionary_get_multi(
        self,
        cache_name: str,
        dictionary_name: str,
        *keys: DictionaryKey,
    ) -> CacheDictionaryGetMultiResponse:
        """Retrieve dictionary values from the cache.

        Args:
            cache_name (str): Name of the cache to get the dictionary from.
            dictionary_name (str): Name of the dictionary to query.
            keys (DictionaryKey): The item(s) to index in the dictionary.

        Returns:
            CacheDictionaryGetMultiResponse: a wrapper over a list of values
                and statuses.
        """
        if len(keys) == 0:
            raise ValueError("Argument keys must be non-empty")

        dictionary_get_response = await self.get(cache_name, dictionary_name)
        if dictionary_get_response.status() == CacheGetStatus.MISS:
            return CacheDictionaryGetMultiResponse(
                values=[None for _ in range(len(keys))],
                status=[CacheGetStatus.MISS for _ in range(len(keys))],
            )

        dictionary: BytesDictionary = deserialize_dictionary(
            cast(bytes, dictionary_get_response.value_as_bytes())
        )

        values: List[Optional[DictionaryValue]] = []
        results: List[CacheGetStatus] = []
        for key in keys:
            try:
                value = dictionary[_as_bytes(key, "Unsupported type for key: ")]
                values.append(value)
                results.append(CacheGetStatus.HIT)
            except KeyError:
                values.append(None)
                results.append(CacheGetStatus.MISS)

        return CacheDictionaryGetMultiResponse(values=values, status=results)

    async def dictionary_get_all(
        self, cache_name: str, dictionary_name: str
    ) -> CacheDictionaryGetAllResponse:
        """Retrieve the entire dictionary from the cache.

        Args:
            cache_name (str): Name of the cache to get the dictionary from.
            dictionary_name (str): Name of the dictionary to retrieve.

        Returns:
            CacheDictionaryGetAllResponse: Value (the mapping, if present) and status (HIT or MISS).
        """
        get_response = await self.get(cache_name, dictionary_name)
        if get_response.status() == CacheGetStatus.MISS:
            return CacheDictionaryGetAllResponse(value=None, status=CacheGetStatus.MISS)

        value = deserialize_dictionary(cast(bytes, get_response.value_as_bytes()))
        return CacheDictionaryGetAllResponse(value=value, status=CacheGetStatus.HIT)

    async def exists(
        self, cache_name: str, *keys: Union[str, bytes]
    ) -> CacheExistsResponse:
        """Test if `keys` exist in the cache.

        Args:
            cache_name (str): Name of the cache to query for the keys.
            keys (str): Key(s) to test for existence.

        Examples:
        >>> if client.exists("my-cache", "key"):
        ...     print("key is present")

        Examples:
        >>> if client.exists("my-cache", "key"):
        ...     print("key is present")

        >>> keys = ["key1", "key2", "key3"]
        ... response = client.exists("my-cache", *keys)
        ... if response.all():
        ...     print("All keys are present")
        ... else:
        ...     missing_keys = response.missing_keys()
        ...     print(f"num_missing={len(missing_keys)}; num_exists={response.num_exists()}")
        ...     print(f"{=missing_keys}")

        Returns:
            CacheExistsResponse: Wrapper object containing the results
                of the exists test.
        """
        get_multi_response = await self.get_multi(cache_name, *keys)
        mask = [status == CacheGetStatus.HIT for status in get_multi_response.status()]
        return CacheExistsResponse(keys, mask)


def init(
    auth_token: str,
    item_default_ttl_seconds: int,
    request_timeout_ms: Optional[int] = None,
) -> SimpleCacheClientIncubating:
    """Creates a SimpleCacheClientIncubating.
    !! Includes non-final, experimental features and APIs subject to change  !!

    Args:
        auth_token: Momento Token to authenticate the requests with Simple Cache Service
        item_default_ttl_seconds: A default Time To Live in seconds for cache objects created by this client. It is
            possible to override this setting when calling the set method.
        request_timeout_ms: An optional timeout in milliseconds to allow for Get and Set operations to complete.
            Defaults to 5 seconds. The request will be terminated if it takes longer than this value and will result
            in TimeoutError.
    Returns:
        SimpleCacheClientIncubating
    Raises:
        IllegalArgumentError: If method arguments fail validations
    """
    _validate_request_timeout(request_timeout_ms)
    return SimpleCacheClientIncubating(
        auth_token, item_default_ttl_seconds, request_timeout_ms
    )

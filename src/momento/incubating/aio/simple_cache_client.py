from typing import cast, Optional
import warnings

from .. import INCUBATING_WARNING_MSG
from ...aio.simple_cache_client import SimpleCacheClient
from ..._utilities._data_validation import _as_bytes

from ..cache_operation_types import (
    CacheGetStatus,
    CacheDictionaryGetResponse,
    CacheDictionarySetResponse,
    CacheDictionaryGetAllResponse,
    BytesDictionary,
    DictionaryKey,
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
        dictionary: Dictionary,
    ) -> CacheDictionarySetResponse:
        """Store dictionary items (key-value pairs) in the cache.

        Inserts items from `dictionary` into a dictionary `dictionary_name`.
        Updates (overwrites) values if the key already exists.

        Args:
            cache_name (str): Name of the cache to store the dictionary in.
            dictionary_name (str): The name of the dictionary in the cache.
            dictionary (Dictionary): The items (key-value pairs) to be stored.

        Returns:
            CacheDictionarySetResponse: data stored in the cache
        """
        dictionary_get_response = await self.get(cache_name, dictionary_name)
        cached_dictionary: BytesDictionary = {}
        if dictionary_get_response.status() == CacheGetStatus.HIT:
            cached_dictionary = deserialize_dictionary(
                cast(bytes, dictionary_get_response.value_as_bytes())
            )

        cached_dictionary.update(convert_dict_items_to_bytes(dictionary))

        set_response = await self.set(
            cache_name, dictionary_name, serialize_dictionary(cached_dictionary)
        )
        return CacheDictionarySetResponse(key=set_response._key, value=dictionary)

    async def dictionary_get(
        self,
        cache_name: str,
        dictionary_name: str,
        key: DictionaryKey,
    ) -> CacheDictionaryGetResponse:
        """Retrieve a dictionary value from the cache.

        Args:
            cache_name (str): Name of the cache to get the dictionary from.
            dictionary_name (str): Name of the dictionary to query.
            key (DictionaryKey): The item to index in the dictionary.

        Returns:
            CacheDictionaryGetResponse: Value (if present) and status (HIT or MISS).
        """
        dictionary_get_response = await self.get(cache_name, dictionary_name)
        if dictionary_get_response.status() == CacheGetStatus.MISS:
            return CacheDictionaryGetResponse(value=None, result=CacheGetStatus.MISS)

        dictionary: BytesDictionary = deserialize_dictionary(
            cast(bytes, dictionary_get_response.value_as_bytes())
        )

        try:
            value = dictionary[_as_bytes(key, "Unsupported type for key: ")]
        except KeyError:
            return CacheDictionaryGetResponse(value=None, result=CacheGetStatus.MISS)

        return CacheDictionaryGetResponse(value=value, result=CacheGetStatus.HIT)

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
            return CacheDictionaryGetAllResponse(value=None, result=CacheGetStatus.MISS)

        value = deserialize_dictionary(cast(bytes, get_response.value_as_bytes()))
        return CacheDictionaryGetAllResponse(value=value, result=CacheGetStatus.HIT)


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

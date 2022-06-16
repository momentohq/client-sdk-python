from typing import Optional
import warnings

from . import INCUBATING_WARNING_MSG
from .aio import simple_cache_client as aio
from .._async_utils import wait_for_coroutine
from .cache_operation_types import (
    CacheDictionaryGetUnaryResponse,
    CacheDictionaryGetMultiResponse,
    CacheDictionarySetUnaryResponse,
    CacheDictionarySetMultiResponse,
    CacheDictionaryGetAllResponse,
    DictionaryKey,
    DictionaryValue,
    Dictionary,
)
from ..simple_cache_client import SimpleCacheClient
from .._utilities._data_validation import _validate_request_timeout


class SimpleCacheClientIncubating(SimpleCacheClient):
    def __init__(
        self,
        auth_token: str,
        default_ttl_seconds: int,
        data_client_operation_timeout_ms: Optional[int],
    ):
        warnings.warn(INCUBATING_WARNING_MSG)
        self._init_loop()
        self._momento_async_client: aio.SimpleCacheClientIncubating = (
            aio.SimpleCacheClientIncubating(
                auth_token=auth_token,
                default_ttl_seconds=default_ttl_seconds,
                data_client_operation_timeout_ms=data_client_operation_timeout_ms,
            )
        )

    def dictionary_set(
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
        coroutine = self._momento_async_client.dictionary_set(
            cache_name,
            dictionary_name,
            key,
            value,
            ttl_seconds,
            refresh_ttl=refresh_ttl,
        )
        return wait_for_coroutine(self._loop, coroutine)

    def dictionary_multi_set(
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
        coroutine = self._momento_async_client.dictionary_multi_set(
            cache_name,
            dictionary_name,
            dictionary,
            ttl_seconds,
            refresh_ttl=refresh_ttl,
        )
        return wait_for_coroutine(self._loop, coroutine)

    def dictionary_get(
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
        coroutine = self._momento_async_client.dictionary_get(
            cache_name, dictionary_name, key
        )
        return wait_for_coroutine(self._loop, coroutine)

    def dictionary_multi_get(
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
        coroutine = self._momento_async_client.dictionary_multi_get(
            cache_name, dictionary_name, *keys
        )
        return wait_for_coroutine(self._loop, coroutine)

    def dictionary_get_all(
        self, cache_name: str, dictionary_name: str
    ) -> CacheDictionaryGetAllResponse:
        """Retrieve the entire dictionary from the cache.

        Args:
            cache_name (str): Name of the cache to get the dictionary from.
            dictionary_name (str): Name of the dictionary to retrieve.

        Returns:
            CacheDictionaryGetAllResponse: Value (the mapping, if present) and status (HIT or MISS).
        """
        coroutine = self._momento_async_client.dictionary_get_all(
            cache_name, dictionary_name
        )
        return wait_for_coroutine(self._loop, coroutine)


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

import logging
from datetime import timedelta

from example_utils.example_logging import initialize_logging

from momento import SimpleCacheClient
from momento.auth import CredentialProvider
from momento.config import Laptop
from momento.responses import CacheGet, CacheSet, CreateCache, ListCaches

_AUTH_PROVIDER = CredentialProvider.from_environment_variable("MOMENTO_AUTH_TOKEN")
_CACHE_NAME = "cache"
_ITEM_DEFAULT_TTL_SECONDS = timedelta(seconds=60)
_KEY = "MyKey"
_VALUE = "MyValue"

_logger = logging.getLogger("momento-example")


def _print_start_banner() -> None:
    _logger.info("******************************************************************")
    _logger.info("*                      Momento Example Start                     *")
    _logger.info("******************************************************************")


def _print_end_banner() -> None:
    _logger.info("******************************************************************")
    _logger.info("*                       Momento Example End                      *")
    _logger.info("******************************************************************")


def _create_cache(cache_client: SimpleCacheClient, cache_name: str) -> None:
    create_cache_response = cache_client.create_cache(cache_name)
    if isinstance(create_cache_response, CreateCache.Success):
        pass
    elif isinstance(create_cache_response, CreateCache.CacheAlreadyExists):
        _logger.info(f"Cache with name: {cache_name!r} already exists.")
    elif isinstance(create_cache_response, CreateCache.Error):
        _logger.error(f"Error creating cache: {create_cache_response.message}")
    else:
        _logger.error("Unreachable")


def _list_caches(cache_client: SimpleCacheClient) -> None:
    _logger.info("Listing caches:")
    list_caches_response = cache_client.list_caches()
    while True:
        if isinstance(list_caches_response, ListCaches.Success):
            for cache_info in list_caches_response.caches:
                _logger.info(f"- {cache_info.name!r}")
            next_token = list_caches_response.next_token
            if next_token is None:
                break
        elif isinstance(list_caches_response, ListCaches.Error):
            _logger.error(f"Error creating cache: {list_caches_response.message}")
        else:
            _logger.error("Unreachable")

        list_caches_response = cache_client.list_caches(next_token)
    _logger.info("")


if __name__ == "__main__":
    initialize_logging()
    _print_start_banner()
    with SimpleCacheClient(Laptop.latest(), _AUTH_PROVIDER, _ITEM_DEFAULT_TTL_SECONDS) as cache_client:
        _create_cache(cache_client, _CACHE_NAME)
        _list_caches(cache_client)

        _logger.info(f"Setting Key: {_KEY!r} Value: {_VALUE!r}")
        set_response = cache_client.set(_CACHE_NAME, _KEY, _VALUE)
        if isinstance(set_response, CacheSet.Success):
            pass
        elif isinstance(set_response, CacheSet.Error):
            _logger.error(f"Error creating cache: {set_response.message}")
        else:
            _logger.error("Unreachable")

        _logger.info(f"Getting Key: {_KEY!r}")
        get_response = cache_client.get(_CACHE_NAME, _KEY)
        if isinstance(get_response, CacheGet.Hit):
            _logger.info(f"Look up resulted in a hit: {get_response}")
            _logger.info(f"Looked up Value: {get_response.value_string!r}")
        elif isinstance(get_response, CacheGet.Miss):
            _logger.info("Look up resulted in a: miss. This is unexpected.")
        elif isinstance(get_response, CacheGet.Error):
            _logger.error(f"Error creating cache: {get_response.message}")
        else:
            _logger.error("Unreachable")
    _print_end_banner()

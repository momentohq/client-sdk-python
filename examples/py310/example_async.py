import asyncio
import logging
from datetime import timedelta

from momento import CacheClientAsync, Configurations, CredentialProvider
from momento.responses import CacheGet, CacheSet, CreateCache, ListCaches

from example_utils.example_logging import initialize_logging

_AUTH_PROVIDER = CredentialProvider.from_environment_variable("MOMENTO_API_KEY")
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


async def _create_cache(cache_client: CacheClientAsync, cache_name: str) -> None:
    create_cache_response = await cache_client.create_cache(cache_name)
    match create_cache_response:
        case CreateCache.Success():
            pass
        case CreateCache.CacheAlreadyExists():
            _logger.info(f"Cache with name: {cache_name!r} already exists.")
        case CreateCache.Error() as error:
            _logger.error(f"Error creating cache: {error.message}")
        case _:
            _logger.error("Unreachable")


async def _list_caches(cache_client: CacheClientAsync) -> None:
    _logger.info("Listing caches:")
    list_caches_response = await cache_client.list_caches()
    match list_caches_response:
        case ListCaches.Success() as success:
            for cache_info in success.caches:
                _logger.info(f"- {cache_info.name!r}")
        case ListCaches.Error() as error:
            _logger.error(f"Error creating cache: {error.message}")
        case _:
            _logger.error("Unreachable")
    _logger.info("")


async def main() -> None:
    initialize_logging()
    _print_start_banner()
    async with await CacheClientAsync.create(Configurations.Laptop.v1(), _AUTH_PROVIDER, _ITEM_DEFAULT_TTL_SECONDS) as cache_client:
        await _create_cache(cache_client, _CACHE_NAME)
        await _list_caches(cache_client)

        _logger.info(f"Setting Key: {_KEY!r} Value: {_VALUE!r}")
        set_response = await cache_client.set(_CACHE_NAME, _KEY, _VALUE)
        match set_response:
            case CacheSet.Success():
                pass
            case CacheSet.Error() as error:
                _logger.error(f"Error creating cache: {error.message}")
            case _:
                _logger.error("Unreachable")

        _logger.info(f"Getting Key: {_KEY!r}")
        get_response = await cache_client.get(_CACHE_NAME, _KEY)
        match get_response:
            case CacheGet.Hit() as hit:
                _logger.info(f"Look up resulted in a hit: {hit}")
                _logger.info(f"Looked up Value: {hit.value_string!r}")
            case CacheGet.Miss():
                _logger.info("Look up resulted in a: miss. This is unexpected.")
            case CacheGet.Error() as error:
                _logger.error(f"Error creating cache: {error.message}")
            case _:
                _logger.error("Unreachable")
    _print_end_banner()


if __name__ == "__main__":
    asyncio.run(main())

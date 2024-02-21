import logging
from datetime import timedelta

from momento import CacheClient, Configurations, CredentialProvider
from momento.responses import CacheGet, CacheDelete, CacheSet, CreateCache, ListCaches

from example_utils.example_logging import initialize_logging

_AUTH_PROVIDER = CredentialProvider.from_environment_variable("MOMENTO_API_KEY")
_CACHE_NAME = "dym-test"
_ITEM_DEFAULT_TTL_SECONDS = timedelta(seconds=60)
_KEY = "MyKey"

_logger = logging.getLogger("momento-example")


def _print_start_banner() -> None:
    _logger.info("******************************************************************")
    _logger.info("*                      Momento Example Start                     *")
    _logger.info("******************************************************************")


def _print_end_banner() -> None:
    _logger.info("******************************************************************")
    _logger.info("*                       Momento Example End                      *")
    _logger.info("******************************************************************")


if __name__ == "__main__":
    initialize_logging()
    _print_start_banner()
    with CacheClient.create(Configurations.Laptop.v1(), _AUTH_PROVIDER, _ITEM_DEFAULT_TTL_SECONDS) as cache_client:

        _logger.info(f"Getting Key: {_KEY!r}")
        get_response = cache_client.get(_CACHE_NAME, _KEY)
        if isinstance(get_response, CacheGet.Hit):
            _logger.info(f"Look up resulted in a hit: {get_response}")
            _logger.info(f"Looked up Value: {get_response.value_string!r}")
        elif isinstance(get_response, CacheGet.Miss):
            _logger.info("Look up resulted in a: miss.")
        elif isinstance(get_response, CacheGet.Error):
            _logger.error(f"Error creating cache: {get_response.message}")
        else:
            _logger.error("Unreachable")

        _logger.info(f"Deleting Key: {_KEY!r}")
        delete_response = cache_client.delete(_CACHE_NAME, _KEY)
        if isinstance(delete_response, CacheDelete.Success):
            _logger.info("Deleted.")
        elif isinstance(delete_response, CacheDelete.Error):
            _logger.error(f"Error deleting item: {cache_delete_error.message}")
        else:
            _logger.error("Unreachable")
    _print_end_banner()

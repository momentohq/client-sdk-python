<head>
  <meta name="Momento Python Client Library Documentation" content="Python client software development kit for Momento Serverless Cache">
</head>
<img src="https://docs.momentohq.com/img/logo.svg" alt="logo" width="400"/>

[![project status](https://momentohq.github.io/standards-and-practices/badges/project-status-official.svg)](https://github.com/momentohq/standards-and-practices/blob/main/docs/momento-on-github.md)
[![project stability](https://momentohq.github.io/standards-and-practices/badges/project-stability-alpha.svg)](https://github.com/momentohq/standards-and-practices/blob/main/docs/momento-on-github.md) 

# Momento Python Client Library


Python client SDK for Momento Serverless Cache: a fast, simple, pay-as-you-go caching solution without
any of the operational overhead required by traditional caching solutions!



## Getting Started :running:

### Requirements

- [Python 3.7](https://www.python.org/downloads/) or above is required
- A Momento Auth Token is required, you can generate one using the [Momento CLI](https://github.com/momentohq/momento-cli)

### Examples

Ready to dive right in? Just check out the [examples](./examples/README.md) directory for complete, working examples of
how to use the SDK.

### Installation

The [Momento SDK is available on PyPi](https://pypi.org/project/momento/).  To install via pip:

```bash
pip install momento
```

### Usage

Here is a quickstart you can use in your own project:

```python
import logging
from datetime import timedelta

from example_utils.example_logging import initialize_logging

from momento import SimpleCacheClient
from momento.auth.credential_provider import EnvMomentoTokenProvider
from momento.config.configurations import Laptop
from momento.responses import CacheGet, CacheSet, CreateCache, ListCaches

_AUTH_PROVIDER = EnvMomentoTokenProvider("MOMENTO_AUTH_TOKEN")
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
    match create_cache_response:
        case CreateCache.Success():
            pass
        case CreateCache.CacheAlreadyExists():
            _logger.info(f"Cache with name: {cache_name!r} already exists.")
        case CreateCache.Error() as error:
            _logger.error(f"Error creating cache: {error.message}")
        case _:
            _logger.error("Unreachable")


def _list_caches(cache_client: SimpleCacheClient) -> None:
    _logger.info("Listing caches:")
    list_caches_response = cache_client.list_caches()
    while True:
        match list_caches_response:
            case ListCaches.Success() as success:
                for cache_info in success.caches:
                    _logger.info(f"- {cache_info.name!r}")
                next_token = success.next_token
                if next_token is None:
                    break
            case ListCaches.Error() as error:
                _logger.error(f"Error creating cache: {error.message}")
            case _:
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
        match set_response:
            case CacheSet.Success():
                pass
            case CacheSet.Error() as error:
                _logger.error(f"Error creating cache: {error.message}")
            case _:
                _logger.error("Unreachable")

        _logger.info(f"Getting Key: {_KEY!r}")
        get_response = cache_client.get(_CACHE_NAME, _KEY)
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

```

Note that the above code requires an environment variable named MOMENTO_AUTH_TOKEN which must
be set to a valid [Momento authentication token](https://docs.momentohq.com/docs/getting-started#obtain-an-auth-token).

### Error Handling

Coming Soon!

### Tuning

Coming Soon!

----------------------------------------------------------------------------------------
For more info, visit our website at [https://gomomento.com](https://gomomento.com)!

from utils.instrumentation import example_observability_setup_tracing

example_observability_setup_tracing()

from datetime import timedelta

from momento import CacheClient, Configurations, CredentialProvider
from momento.responses import CacheGet, CacheSet, CreateCache

_AUTH_PROVIDER = CredentialProvider.from_environment_variable("MOMENTO_API_KEY")
_ITEM_DEFAULT_TTL_SECONDS = timedelta(seconds=60)
_CACHE_NAME = "test-cache"
_KEY = "test-key"
_VALUE = "test-value"

def _create_cache(cache_client: CacheClient) -> None:
    create_cache_response = cache_client.create_cache(_CACHE_NAME)
    match create_cache_response:
        case CreateCache.Success():
            pass
        case CreateCache.CacheAlreadyExists():
            print(f"Cache with name: {_CACHE_NAME!r} already exists.")
        case CreateCache.Error() as error:
            print(f"Error creating cache: {error.message}")
        case _:
            print("Unreachable")


def _set_cache(cache_client: CacheClient) -> None:
    set_cache_response = cache_client.set(_CACHE_NAME, _KEY, _VALUE)
    match set_cache_response:
        case CacheSet.Success():
            pass
        case CacheSet.Error() as error:
            print(f"Error setting value: {error.message}")
        case _:
            print("Unreachable")


def _get_cache(cache_client: CacheClient) -> None:
    get_cache_response = cache_client.get(_CACHE_NAME, _KEY)
    match get_cache_response:
        case CacheGet.Hit():
            print("Value is " + get_cache_response.value_string)
            pass
        case CacheGet.Error() as error:
            print(f"Error setting value: {error.message}")
        case _:
            print("Unreachable")


def main() -> None:
    with CacheClient(Configurations.Laptop.v1(), _AUTH_PROVIDER, _ITEM_DEFAULT_TTL_SECONDS) as cache_client:
        _create_cache(cache_client)
        _set_cache(cache_client)
        _get_cache(cache_client)


main()
print('Success! Zipkin at http://localhost:9411 should contain traces for the cache creation, get, and set.')

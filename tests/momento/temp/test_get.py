
from datetime import timedelta
from pprint import pprint
from momento.auth.credential_provider import CredentialProvider
from momento.cache_client_async import CacheClientAsync
from momento.config.configurations import Configurations
from momento.errors.error_details import MomentoErrorCode
from momento.responses.control.cache.list import ListCaches
from momento.responses.data.scalar.get import CacheGet
from momento.responses.data.scalar.set import CacheSet
from momento.retry.fixed_timeout_retry_strategy import FixedTimeoutRetryStrategy
from momento.retry.retry_strategy import RetryStrategy


async def create_cache(client: CacheClientAsync) -> None:
    print("Creating cache:")
    create_cache_response = await client.create_cache("cache")
    match create_cache_response:
        case CacheSet.Success() as success:
            print(f"Cache created successfully: {success.cache_name!r}")
        case CacheSet.Error() as error:
            print(f"Error creating cache: {error.message}")
        case _:
            print("Unreachable")
    print("")

async def list_caches(client) -> None:
    print("Listing caches:")
    list_caches_response = await client.list_caches()
    match list_caches_response:
        case ListCaches.Success() as success:
            for cache_info in success.caches:
                print(f"- {cache_info.name!r}")
        case ListCaches.Error() as error:
            print(f"Error listing caches: {error.message}")
        case _:
            print("Unreachable")
    print("")

async def set_key(client: CacheClientAsync) -> None:
    cache_name = "cache"
    set_resp = await client.set(cache_name, "key", "value")
    pprint(set_resp)
    if isinstance(set_resp, CacheSet.Error):
        pprint(set_resp)
        raise set_resp.inner_exception.with_traceback(None)
    elif isinstance(set_resp, CacheSet.Success):
        print("=======SUCCESS=======")
        pprint(set_resp)

def describe_get() -> None:
    async def it_should_return_error() -> None:
        credential_provider = CredentialProvider.from_environment_variable("MOMENTO_API_KEY")
        ttl = timedelta(seconds=600)
        config = {
            'configuration': Configurations.Laptop.v1().with_retry_strategy(
                FixedTimeoutRetryStrategy(retry_timeout_millis=1000, retry_delay_interval_millis=100)
            ).with_client_timeout(timedelta(seconds=3)),
            'credential_provider': credential_provider,
            'default_ttl': ttl,
        }
        client = await CacheClientAsync.create(**config)

        # These are here mostly to make sure control plane calls are working
        # and that the cache is created with the expected key set.

        await create_cache(client)
        await list_caches(client)
        await set_key(client)

        ################
        # To test the retry strategy, use a cache name for a cache that does not exist
        # and uncomment the NOT_FOUIND status code in the default_eligibility_strategy.py file.
        ################

        cache_name = "cachexyz"
        get_resp = await client.get(cache_name, "key")
        pprint(get_resp)
        if isinstance(get_resp, CacheGet.Error):
            pprint(get_resp)
            assert get_resp.error_code == MomentoErrorCode.NOT_FOUND_ERROR
        elif isinstance(get_resp, CacheGet.Miss):
            print("=======MISS=======")
            pprint(get_resp)
            assert True
        elif isinstance(get_resp, CacheGet.Hit):
            print("=======HIT=======")
            pprint(get_resp.value_string)
            assert get_resp.value_string == "value"
        assert False

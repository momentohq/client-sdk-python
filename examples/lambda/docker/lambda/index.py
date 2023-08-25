from datetime import timedelta

from momento import CacheClient, Configurations, CredentialProvider
from momento.responses import CacheGet, CacheSet, CreateCache


def handler(event, lambda_context):
    cache_name = "default-cache"
    with CacheClient(
            configuration=Configurations.Laptop.v1(),
            credential_provider=CredentialProvider.from_environment_variable("MOMENTO_AUTH_TOKEN"),
            default_ttl=timedelta(seconds=60),
    ) as cache_client:
        create_cache_response = cache_client.create_cache(cache_name)

        if isinstance(create_cache_response, CreateCache.CacheAlreadyExists):
            print(f"Cache with name: {cache_name} already exists.")
        elif isinstance(create_cache_response, CreateCache.Error):
            raise create_cache_response.inner_exception

        print("Setting Key: key to Value: value")
        set_response = cache_client.set(cache_name, "key", "value")

        if isinstance(set_response, CacheSet.Error):
            raise set_response.inner_exception

        print("Getting Key: key")
        get_response = cache_client.get(cache_name, "key")

        if isinstance(get_response, CacheGet.Hit):
            print(f"Look up resulted in a hit: {get_response}")
            print(f"Looked up Value: {get_response.value_string!r}")
        elif isinstance(get_response, CacheGet.Miss):
            print("Look up resulted in a: miss. This is unexpected.")
        elif isinstance(get_response, CacheGet.Error):
            raise get_response.inner_exception
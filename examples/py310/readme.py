from datetime import timedelta

from momento import CacheClient, Configurations, CredentialProvider
from momento.responses import CacheGet, CacheSet, CreateCache, ListCaches

cache_client = CacheClient(
    Configurations.Laptop.v1(),
    CredentialProvider.from_environment_variable("MOMENTO_AUTH_TOKEN"),
    timedelta(seconds=60)
)

cache_client.create_cache("cache")
cache_client.set("cache", "my-key", "my-value")
get_response = cache_client.get("cache", "my-key")
match get_response:
    case CacheGet.Hit() as hit:
        print(f"Got value: {hit.value_string}")
    case _:
        print(f"Response was not a hit: {get_response}")

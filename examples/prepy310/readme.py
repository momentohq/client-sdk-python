from datetime import timedelta
from momento import CacheClient, Configurations, CredentialProvider
from momento.responses import CacheGet, CacheSet, CreateCache

cache_client = CacheClient(
    configuration=Configurations.Laptop.v1(),
    credential_provider=CredentialProvider.from_environment_variable('MOMENTO_AUTH_TOKEN'),
    default_ttl=timedelta(seconds=60)
)
cache_client.create_cache("cache")
cache_client.set("cache", "myKey", "myValue")
get_response = cache_client.get("cache", "myKey")
if isinstance(get_response, CacheGet.Hit):
    print(f"Got value: {get_response.value_string}")

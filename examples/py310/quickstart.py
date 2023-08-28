from datetime import timedelta

from momento import CacheClient, Configurations, CredentialProvider
from momento.responses import CacheGet, CacheSet, CreateCache

if __name__ == "__main__":
    cache_name = "default-cache"
    with CacheClient.create(
        configuration=Configurations.Laptop.v1(),
        credential_provider=CredentialProvider.from_environment_variable("MOMENTO_AUTH_TOKEN"),
        default_ttl=timedelta(seconds=60),
    ) as cache_client:
        create_cache_response = cache_client.create_cache(cache_name)
        match create_cache_response:
            case CreateCache.CacheAlreadyExists():
                print(f"Cache with name: {cache_name} already exists.")
            case CreateCache.Error() as error:
                raise error.inner_exception

        print("Setting Key: foo to Value: FOO")
        set_response = cache_client.set(cache_name, "foo", "FOO")
        match set_response:
            case CacheSet.Error() as error:
                raise error.inner_exception

        print("Getting Key: foo")
        get_response = cache_client.get(cache_name, "foo")
        match get_response:
            case CacheGet.Hit() as hit:
                print(f"Look up resulted in a hit: {hit}")
                print(f"Looked up Value: {hit.value_string!r}")
            case CacheGet.Miss():
                print("Look up resulted in a: miss. This is unexpected.")
            case CacheGet.Error() as error:
                raise error.inner_exception

import asyncio
from datetime import timedelta

from momento import (
    CacheClientAsync,
    Configurations,
    CredentialProvider,
)
from momento.responses import (
    CacheGet,
    CacheSet,
)

database: dict[str, str] = {}
database["test-key"] = "test-value"
async def example_patterns_WriteThroughCaching(cache_client: CacheClientAsync):
    database.set("test-key", "test-value")
    set_response = await cache_client.set("test-cache", "test-key", "test-value")
    return

# end example

async def example_patterns_ReadAsideCaching(cache_client: CacheClientAsync):
    get_response = await cache_client.get("test-cache", "test-key")
    match get_response:
        case CacheGet.Hit() as hit:
            print(f"Retrieved value for key 'test-key': {hit.value_string}")
            return
    print(f"cache miss, fetching from database")
    actual_value = database.get("test-key")
    await cache_client.set("test-cache", "test-key", actual_value)
    return

# end example

async def main():
    example_API_CredentialProviderFromEnvVar()

    await example_API_InstantiateCacheClient()
    cache_client = await CacheClientAsync.create(
        Configurations.Laptop.latest(),
        CredentialProvider.from_environment_variable("MOMENTO_API_KEY"),
        timedelta(seconds=60),
    )

    await example_patterns_ReadAsideCaching(cache_client)
    await example_patterns_WriteThroughCaching(cache_client)


if __name__ == "__main__":
    asyncio.run(main())

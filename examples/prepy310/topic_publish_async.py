import asyncio
import logging
from datetime import timedelta

from momento import (
    CacheClient,
    Configurations,
    CredentialProvider,
    TopicClientAsync,
    TopicConfigurations,
)
from momento.responses import CreateCache, TopicPublish

from example_utils.example_logging import initialize_logging

_AUTH_PROVIDER = CredentialProvider.from_environment_variable("MOMENTO_API_KEY")
_CACHE_NAME = "cache"
_logger = logging.getLogger("topic-publish-example")


def setup_cache() -> None:
    with CacheClient.create(Configurations.Laptop.latest(), _AUTH_PROVIDER, timedelta(seconds=60)) as client:
        response = client.create_cache(_CACHE_NAME)
        if isinstance(response, CreateCache.Error):
            raise response.inner_exception


async def main() -> None:
    initialize_logging()
    setup_cache()
    _logger.info("hello")
    async with TopicClientAsync(TopicConfigurations.Default.v1(), _AUTH_PROVIDER) as client:
        response = await client.publish("cache", "my_topic", "my_value")
        if isinstance(response, TopicPublish.Error):
            print("error: ", response.message)

if __name__ == "__main__":
    asyncio.run(main())

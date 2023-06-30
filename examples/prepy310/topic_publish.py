import asyncio
import logging

from momento import CredentialProvider, TopicClientAsync, TopicConfigurations

from example_utils.example_logging import initialize_logging

_AUTH_PROVIDER = CredentialProvider.from_environment_variable("MOMENTO_AUTH_TOKEN")
_logger = logging.getLogger("topic-publish-example")


async def main() -> None:
    initialize_logging()
    _logger.info("hello")
    async with TopicClientAsync(TopicConfigurations.Default.v1(), _AUTH_PROVIDER) as client:
        await client.publish("cache", "my_topic", "my_value")

if __name__ == "__main__":
    asyncio.run(main())

import asyncio
import logging

from example_utils.example_logging import initialize_logging

from momento import CredentialProvider, TopicClientAsync, TopicConfigurations
from momento.responses import TopicPublish

_AUTH_PROVIDER = CredentialProvider.from_environment_variable("MOMENTO_AUTH_TOKEN")
_logger = logging.getLogger("topic-publish-example")


async def main() -> None:
    initialize_logging()
    _logger.info("hello")
    async with TopicClientAsync(TopicConfigurations.Default.v1(), _AUTH_PROVIDER) as client:
        response = await client.publish("cache", "my_topic", "my_value")
        if isinstance(response, TopicPublish.Error):
            print("error: ", response.message)

if __name__ == "__main__":
    asyncio.run(main())

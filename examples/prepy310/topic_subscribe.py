import asyncio
import logging

from momento import CredentialProvider, TopicClientAsync, TopicConfigurations
from momento.responses import TopicSubscribe
from example_utils.example_logging import initialize_logging

_AUTH_PROVIDER = CredentialProvider.from_environment_variable("MOMENTO_AUTH_TOKEN")
_logger = logging.getLogger("topic-subscribe-example")


async def main() -> None:
    initialize_logging()
    _logger.info("hello")
    async with TopicClientAsync(TopicConfigurations.Default.v1(), _AUTH_PROVIDER) as client:
        result = await client.subscribe("cache", "my_topic")
        if isinstance(result, TopicSubscribe.Subscription):
            print("polling for items . . .")
            while True:
                print(await result.item())
        elif isinstance(result, TopicSubscribe.Error):
            print("got error: ", result.message)

if __name__ == "__main__":
    asyncio.run(main())

import asyncio
import logging

from example_utils.example_logging import initialize_logging

from momento import CredentialProvider, TopicClientAsync, TopicConfigurations
from momento.errors import SdkException
from momento.responses import TopicSubscribe, TopicSubscriptionItem

_AUTH_PROVIDER = CredentialProvider.from_environment_variable("MOMENTO_AUTH_TOKEN")
_NUM_SUBSCRIBERS = 10
_logger = logging.getLogger("topic-subscribe-example")


async def main() -> None:
    initialize_logging()
    _logger.info("hello")
    async with TopicClientAsync(
            TopicConfigurations.Default.v1().with_max_subscriptions(_NUM_SUBSCRIBERS), _AUTH_PROVIDER
    ) as client:
        tasks = []
        for i in range(0, _NUM_SUBSCRIBERS):
            subscription = await client.subscribe("cache", "my_topic")
            match subscription:
                case TopicSubscribe.SubscriptionAsync():
                    tasks.append(asyncio.create_task(poll_subscription(subscription)))
                case TopicSubscribe.Error():
                    print("got subscription error: ", subscription.message)

        if len(tasks) == 0:
            raise Exception("no subscriptions were successful")

        print(f"{len(tasks)} subscriptions polling for items. . .")
        try:
            await asyncio.gather(*tasks)
        except SdkException:
            print(f"got exception")
            for task in tasks:
                task.cancel()


async def poll_subscription(subscription: TopicSubscribe.SubscriptionAsync):
    async for item in subscription:
        match item:
            case TopicSubscriptionItem.Success():
                print(f"got item: {item.value_string} ({item.value_bytes})")
            case TopicSubscriptionItem.Error():
                print("stream closed")
                print(item.inner_exception.message)
                return


if __name__ == "__main__":
    asyncio.run(main())


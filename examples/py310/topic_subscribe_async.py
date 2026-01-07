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
from momento.errors import SdkException
from momento.responses import CreateCache, TopicSubscribe, TopicSubscriptionItem

from example_utils.example_logging import initialize_logging

_AUTH_PROVIDER = CredentialProvider.from_environment_variables_v2()
_CACHE_NAME = "cache"
_NUM_SUBSCRIBERS = 10
_logger = logging.getLogger("topic-subscribe-example")


def setup_cache() -> None:
    with CacheClient.create(Configurations.Laptop.latest(), _AUTH_PROVIDER, timedelta(seconds=60)) as client:
        response = client.create_cache(_CACHE_NAME)
        match response:
            case CreateCache.Error():
                raise response.inner_exception


async def main() -> None:
    initialize_logging()
    setup_cache()
    _logger.info("hello")
    # You may need to adjust the timeout to accommodate your network conditions, runtime, etc
    async with TopicClientAsync(
        TopicConfigurations.Default.v1()
        .with_max_subscriptions(_NUM_SUBSCRIBERS)
        .with_client_timeout(timedelta(seconds=10)),
        _AUTH_PROVIDER,
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
            print("got exception")
            for task in tasks:
                task.cancel()


async def poll_subscription(subscription: TopicSubscribe.SubscriptionAsync):
    async for item in subscription:
        match item:
            case TopicSubscriptionItem.Text():
                print(f"got item as string: {item.value}")
            case TopicSubscriptionItem.Binary():
                print(f"got item as bytes: {item.value!r}")
            case TopicSubscriptionItem.Error():
                print("stream closed")
                print(item.inner_exception.message)
                return


if __name__ == "__main__":
    asyncio.run(main())

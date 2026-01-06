import logging
from datetime import timedelta

from momento import (
    CacheClient,
    Configurations,
    CredentialProvider,
    TopicClient,
    TopicConfigurations,
)
from momento.responses import CreateCache, TopicSubscribe, TopicSubscriptionItem

from example_utils.example_logging import initialize_logging

_AUTH_PROVIDER = CredentialProvider.from_environment_variables_v2()
_CACHE_NAME = "cache"
_logger = logging.getLogger("topic-subscribe-example")


def setup_cache() -> None:
    with CacheClient.create(Configurations.Laptop.latest(), _AUTH_PROVIDER, timedelta(seconds=60)) as client:
        response = client.create_cache(_CACHE_NAME)
        if isinstance(response, CreateCache.Error):
            raise response.inner_exception


def main() -> None:
    initialize_logging()
    setup_cache()
    _logger.info("hello")
    with TopicClient(
        TopicConfigurations.Default.v1().with_client_timeout(timedelta(seconds=10)), _AUTH_PROVIDER
    ) as client:
        subscription = client.subscribe("cache", "my_topic")
        if isinstance(subscription, TopicSubscribe.Error):
            raise Exception("got subscription error: ", subscription.message)

        if isinstance(subscription, TopicSubscribe.Subscription):
            print("polling for items. . .")
            for item in subscription:
                if isinstance(item, TopicSubscriptionItem.Text):
                    print(f"got item as string: {item.value}")
                elif isinstance(item, TopicSubscriptionItem.Binary):
                    print(f"got item as bytes: {item.value!r}")
                elif isinstance(item, TopicSubscriptionItem.Error):
                    print(f"got item error: {item.message}")


if __name__ == "__main__":
    main()

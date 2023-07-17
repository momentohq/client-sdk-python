import logging
from datetime import timedelta

from example_utils.example_logging import initialize_logging

from momento import (
    CacheClient,
    Configurations,
    CredentialProvider,
    TopicClient,
    TopicConfigurations,
)
from momento.responses import CreateCache, TopicSubscribe, TopicSubscriptionItem

_AUTH_PROVIDER = CredentialProvider.from_environment_variable("MOMENTO_AUTH_TOKEN")
_CACHE_NAME = "cache"
_logger = logging.getLogger("topic-subscribe-example")


def setup_cache() -> None:
    with CacheClient(Configurations.Laptop.latest(), _AUTH_PROVIDER, timedelta(seconds=60)) as client:
        response = client.create_cache(_CACHE_NAME)
        if isinstance(response, CreateCache.Error):
            raise response.inner_exception


def main() -> None:
    initialize_logging()
    setup_cache()
    _logger.info("hello")
    with TopicClient(TopicConfigurations.Default.v1(), _AUTH_PROVIDER) as client:
        subscription = client.subscribe("cache", "my_topic")
        if isinstance(subscription, TopicSubscribe.Error):
            raise Exception("got subscription error: ", subscription.message)

        if isinstance(subscription, TopicSubscribe.Subscription):
            print("polling for items. . .")
            for item in subscription:
                if isinstance(item, TopicSubscriptionItem.Success):
                    print(f"got item: {item.value_string} ({item.value_bytes})")
                    return
                elif isinstance(item, TopicSubscriptionItem.Error):
                    print(f"got item error: {item.message}")
                    return


if __name__ == "__main__":
    main()

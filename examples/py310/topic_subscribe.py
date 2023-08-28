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
    with CacheClient.create(Configurations.Laptop.latest(), _AUTH_PROVIDER, timedelta(seconds=60)) as client:
        response = client.create_cache(_CACHE_NAME)
        match response:
            case CreateCache.Error():
                raise response.inner_exception


def main() -> None:
    initialize_logging()
    setup_cache()
    _logger.info("hello")
    with TopicClient(TopicConfigurations.Default.v1(), _AUTH_PROVIDER) as client:
        subscription = client.subscribe("cache", "my_topic")
        match subscription:
            case TopicSubscribe.Error():
                raise Exception("got subscription error: ", subscription.message)
            case TopicSubscribe.Subscription():
                print("polling for items. . .")
                for item in subscription:
                    match item:
                        case TopicSubscriptionItem.Success():
                            print(f"got item: {item.value_string} ({item.value_bytes})")
                        case TopicSubscriptionItem.Error():
                            print(f"got item error: {item.message}")


if __name__ == "__main__":
    main()

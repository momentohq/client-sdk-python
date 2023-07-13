import logging

from example_utils.example_logging import initialize_logging

from momento import CredentialProvider, TopicClient, TopicConfigurations
from momento.responses import TopicSubscribe, TopicSubscriptionItem

_AUTH_PROVIDER = CredentialProvider.from_environment_variable("MOMENTO_AUTH_TOKEN")
_logger = logging.getLogger("topic-subscribe-example")


def main() -> None:
    initialize_logging()
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
                elif isinstance(item, TopicSubscriptionItem.Error):
                    print(f"got item error: {item.message}")


if __name__ == "__main__":
    main()

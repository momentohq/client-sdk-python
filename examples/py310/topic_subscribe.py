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

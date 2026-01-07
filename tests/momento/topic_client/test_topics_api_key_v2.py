from momento import TopicClientAsync
from momento.responses import TopicSubscribe, TopicSubscriptionItem

from tests.utils import uuid_str

# Async tests


async def test_succeeds_with_nonexistent_topic_async(topic_client_async_v2: TopicClientAsync, cache_name: str) -> None:
    topic = uuid_str()

    resp = await topic_client_async_v2.subscribe(cache_name, topic)
    assert isinstance(resp, TopicSubscribe.SubscriptionAsync)


async def test_subscribe_happy_path_string_async(topic_client_async_v2: TopicClientAsync, cache_name: str) -> None:
    topic = uuid_str()
    value = uuid_str()

    _ = await topic_client_async_v2.publish(cache_name, topic_name=topic, value=value)

    subscribe_response = await topic_client_async_v2.subscribe(cache_name, topic_name=topic)
    assert isinstance(subscribe_response, TopicSubscribe.SubscriptionAsync)

    item_task = subscribe_response.__anext__()
    item_response = await item_task
    assert isinstance(item_response, TopicSubscriptionItem.Text)
    assert item_response.value == value


# Sync tests


def test_succeeds_with_nonexistent_topic(topic_client_v2: TopicClientAsync, cache_name: str) -> None:
    topic = uuid_str()

    resp = topic_client_v2.subscribe(cache_name, topic)
    assert isinstance(resp, TopicSubscribe.Subscription)


def test_subscribe_happy_path_string(topic_client_v2: TopicClientAsync, cache_name: str) -> None:
    topic = uuid_str()
    value = uuid_str()

    _ = topic_client_v2.publish(cache_name, topic_name=topic, value=value)

    subscribe_response = topic_client_v2.subscribe(cache_name, topic_name=topic)
    assert isinstance(subscribe_response, TopicSubscribe.Subscription)

    item_response = next(subscribe_response)
    assert isinstance(item_response, TopicSubscriptionItem.Text)
    assert item_response.value == value

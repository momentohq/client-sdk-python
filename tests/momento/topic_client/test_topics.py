from functools import partial

from momento import CacheClient, TopicClient
from momento.responses import TopicPublish, TopicSubscribe, TopicSubscriptionItem
from pytest import fixture
from pytest_describe import behaves_like

from tests.utils import uuid_bytes, uuid_str

from .shared_behaviors import (
    TCacheNameValidator,
    TTopicValidator,
    a_cache_name_validator,
    a_topic_validator,
)


@behaves_like(a_cache_name_validator, a_topic_validator)
def describe_publish() -> None:
    @fixture
    def cache_name_validator(topic_client: TopicClient) -> TCacheNameValidator:
        topic_name = uuid_str()
        value = uuid_str()
        return partial(topic_client.publish, topic_name=topic_name, value=value)

    @fixture
    def topic_validator(topic_client: TopicClient) -> TTopicValidator:
        cache_name = uuid_str()
        value = uuid_str()
        return partial(topic_client.publish, cache_name=cache_name, value=value)

    def publish_happy_path(client: CacheClient, topic_client: TopicClient, cache_name: str) -> None:
        topic = uuid_str()
        value = uuid_str()

        resp = topic_client.publish(cache_name, topic_name=topic, value=value)
        assert isinstance(resp, TopicPublish.Success)


@behaves_like(a_cache_name_validator, a_topic_validator)
def describe_subscribe() -> None:
    @fixture
    def cache_name_validator(topic_client: TopicClient) -> TCacheNameValidator:
        topic_name = uuid_str()
        return partial(topic_client.subscribe, topic_name=topic_name)

    @fixture
    def topic_validator(topic_client: TopicClient) -> TTopicValidator:
        cache_name = uuid_str()
        return partial(topic_client.subscribe, cache_name=cache_name)

    def subscribe_happy_path_string(client: CacheClient, topic_client: TopicClient, cache_name: str) -> None:
        topic = uuid_str()
        value = uuid_str()

        subscribe_response = topic_client.subscribe(cache_name, topic_name=topic)
        assert isinstance(subscribe_response, TopicSubscribe.Subscription)

        _ = topic_client.publish(cache_name, topic_name=topic, value=value)

        item_response = next(subscribe_response)
        assert isinstance(item_response, TopicSubscriptionItem.Text)
        assert item_response.value == value

    def subscribe_happy_path_binary(client: CacheClient, topic_client: TopicClient, cache_name: str) -> None:
        topic = uuid_str()
        value = uuid_bytes()

        subscribe_response = topic_client.subscribe(cache_name, topic_name=topic)
        assert isinstance(subscribe_response, TopicSubscribe.Subscription)

        _ = topic_client.publish(cache_name, topic_name=topic, value=value)

        item_response = next(subscribe_response)
        assert isinstance(item_response, TopicSubscriptionItem.Binary)
        assert item_response.value == value

    def succeeds_with_nonexistent_topic(client: CacheClient, topic_client: TopicClient, cache_name: str) -> None:
        topic = uuid_str()

        resp = topic_client.subscribe(cache_name, topic)
        assert isinstance(resp, TopicSubscribe.Subscription)

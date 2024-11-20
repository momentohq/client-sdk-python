from functools import partial

from momento import TopicClient
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

    def publish_happy_path(topic_client: TopicClient, cache_name: str) -> None:
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

    def subscribe_happy_path_string(topic_client: TopicClient, cache_name: str) -> None:
        topic = uuid_str()
        value = uuid_str()

        _ = topic_client.publish(cache_name, topic_name=topic, value=value)

        subscribe_response = topic_client.subscribe(cache_name, topic_name=topic)
        assert isinstance(subscribe_response, TopicSubscribe.Subscription)

        item_response = next(subscribe_response)

        assert isinstance(item_response, TopicSubscriptionItem.Text)
        assert item_response.value == value

    def subscribe_happy_path_string_with_nonzero_resume(topic_client: TopicClient, cache_name: str) -> None:
        topic = uuid_str()
        value = uuid_str()

        _ = topic_client.publish(cache_name, topic_name=topic, value="1")
        _ = topic_client.publish(cache_name, topic_name=topic, value="2")
        _ = topic_client.publish(cache_name, topic_name=topic, value=value)

        subscribe_response = topic_client.subscribe(
            cache_name, topic_name=topic, resume_at_topic_sequence_number=3, resume_at_topic_sequence_page=0
        )
        assert isinstance(subscribe_response, TopicSubscribe.Subscription)

        item_response = next(subscribe_response)

        assert isinstance(item_response, TopicSubscriptionItem.Text)
        assert item_response.value == value

    def subscribe_happy_path_string_with_discontinuity(topic_client: TopicClient, cache_name: str) -> None:
        topic = uuid_str()
        value = uuid_str()

        _ = topic_client.publish(cache_name, topic_name=topic, value=value)

        subscribe_response = topic_client.subscribe(
            cache_name, topic_name=topic, resume_at_topic_sequence_number=5, resume_at_topic_sequence_page=5
        )
        assert isinstance(subscribe_response, TopicSubscribe.Subscription)

        item_response = next(subscribe_response)

        assert isinstance(item_response, TopicSubscriptionItem.Text)
        assert item_response.value == value

    def subscribe_happy_path_binary(topic_client: TopicClient, cache_name: str) -> None:
        topic = uuid_str()
        value = uuid_bytes()

        subscribe_response = topic_client.subscribe(cache_name, topic_name=topic)
        assert isinstance(subscribe_response, TopicSubscribe.Subscription)

        _ = topic_client.publish(cache_name, topic_name=topic, value=value)

        item_response = next(subscribe_response)
        assert isinstance(item_response, TopicSubscriptionItem.Binary)
        assert item_response.value == value

    def succeeds_with_nonexistent_topic(topic_client: TopicClient, cache_name: str) -> None:
        topic = uuid_str()

        resp = topic_client.subscribe(cache_name, topic)
        assert isinstance(resp, TopicSubscribe.Subscription)

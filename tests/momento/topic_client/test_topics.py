from functools import partial

from pytest import fixture
from pytest_describe import behaves_like

from momento import CacheClient, TopicClientAsync
from momento.errors import MomentoErrorCode
from momento.responses import TopicPublish, TopicSubscribe, TopicSubscriptionItem
from tests.utils import uuid_str

from ..cache_client.shared_behaviors import (
    TCacheNameValidator,
    TTopicValidator,
    a_cache_name_validator,
    a_topic_validator,
)


@behaves_like(a_cache_name_validator, a_topic_validator)
def describe_publish() -> None:
    @fixture
    def cache_name_validator(topic_client: TopicClientAsync) -> TCacheNameValidator:
        topic_name = uuid_str()
        value = uuid_str()
        return partial(topic_client.publish, topic_name=topic_name, value=value)

    @fixture
    def topic_validator(topic_client: TopicClientAsync) -> TTopicValidator:
        cache_name = uuid_str()
        value = uuid_str()
        return partial(topic_client.publish, cache_name=cache_name, value=value)

    def publish_happy_path(client: CacheClient, topic_client: TopicClientAsync, cache_name: str) -> None:
        topic = uuid_str()
        value = uuid_str()

        resp = topic_client.publish(cache_name, topic_name=topic, value=value)
        assert isinstance(resp, TopicPublish.Success)

    def with_empty_topic_name(topic_client: TopicClientAsync, cache_name: str):
        topic = ""
        value = uuid_str()

        resp = topic_client.publish(cache_name, topic, value)
        assert isinstance(resp, TopicPublish.Error)
        if isinstance(resp, TopicPublish.Error):
            assert resp.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR


@behaves_like(a_cache_name_validator, a_topic_validator)
def describe_subscribe() -> None:
    @fixture
    def cache_name_validator(topic_client: TopicClientAsync) -> TCacheNameValidator:
        topic_name = uuid_str()
        return partial(topic_client.subscribe, topic_name=topic_name)

    @fixture
    def topic_validator(topic_client: TopicClientAsync) -> TTopicValidator:
        cache_name = uuid_str()
        return partial(topic_client.subscribe, cache_name=cache_name)

    def subscribe_happy_path(client: CacheClient, topic_client: TopicClientAsync, cache_name: str) -> None:
        topic = uuid_str()
        value = uuid_str()

        subscribe_response = topic_client.subscribe(cache_name, topic_name=topic)
        assert isinstance(subscribe_response, TopicSubscribe.SubscriptionAsync)

        item_task = subscribe_response.item()
        publish_response = topic_client.publish(cache_name, topic_name=topic, value=value)

        print(publish_response)
        item_response = item_task
        assert isinstance(item_response, TopicSubscriptionItem.Success)
        assert item_response.value_string == value

    def errors_with_empty_topic_name(topic_client: TopicClientAsync, cache_name: str):
        topic = ""

        resp = topic_client.subscribe(cache_name, topic)
        assert isinstance(resp, TopicSubscribe.Error)
        if isinstance(resp, TopicSubscribe.Error):
            assert resp.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR

    def succeeds_with_nonexistent_topic(client: CacheClient, topic_client: TopicClientAsync, cache_name: str) -> None:
        topic = uuid_str()

        resp = topic_client.subscribe(cache_name, topic)
        assert isinstance(resp, TopicSubscribe.SubscriptionAsync)

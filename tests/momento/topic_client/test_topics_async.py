from functools import partial

from momento import CacheClientAsync, TopicClientAsync
from momento.errors import MomentoErrorCode
from momento.responses import TopicPublish, TopicSubscribe, TopicSubscriptionItem, PubsubResponse

from pytest import fixture
from pytest_describe import behaves_like

from tests.utils import uuid_str

from ..cache_client.shared_behaviors_async import (
    a_cache_name_validator, TCacheNameValidator, a_topic_validator, TTopicValidator, TCacheName, TConnectionValidator,
    a_connection_validator
)


@behaves_like(a_cache_name_validator, a_topic_validator)
def describe_publish() -> None:

    @fixture
    def cache_name_validator(topic_client_async: TopicClientAsync) -> TCacheNameValidator:
        topic_name = uuid_str()
        value = uuid_str()
        return partial(topic_client_async.publish, topic_name=topic_name, value=value)

    @fixture
    def topic_validator(topic_client_async: TopicClientAsync) -> TTopicValidator:
        cache_name = uuid_str()
        value = uuid_str()
        return partial(topic_client_async.publish, cache_name=cache_name, value=value)

    async def publish_happy_path(client: CacheClientAsync, topic_client_async: TopicClientAsync, cache_name: str) -> None:
        topic = uuid_str()
        value = uuid_str()

        resp = await topic_client_async.publish(cache_name, topic_name=topic, value=value)
        assert isinstance(resp, TopicPublish.Success)

    async def with_empty_topic_name(topic_client_async: TopicClientAsync, cache_name: str):
        topic = ""
        value = uuid_str()

        resp = await topic_client_async.publish(cache_name, topic, value)
        assert isinstance(resp, TopicPublish.Error)
        if isinstance(resp, TopicPublish.Error):
            assert resp.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR


@behaves_like(a_cache_name_validator, a_topic_validator)
def describe_subscribe() -> None:

    @fixture
    def cache_name_validator(topic_client_async: TopicClientAsync) -> TCacheNameValidator:
        topic_name = uuid_str()
        return partial(topic_client_async.subscribe, topic_name=topic_name)

    @fixture
    def topic_validator(topic_client_async: TopicClientAsync) -> TTopicValidator:
        cache_name = uuid_str()
        return partial(topic_client_async.subscribe, cache_name=cache_name)

    async def subscribe_happy_path(client: CacheClientAsync, topic_client_async: TopicClientAsync, cache_name: str) -> None:
        topic = uuid_str()
        value = uuid_str()

        subscribe_response = await topic_client_async.subscribe(cache_name, topic_name=topic)
        assert isinstance(subscribe_response, TopicSubscribe.SubscriptionAsync)

        item_task = subscribe_response.item()
        publish_response = await topic_client_async.publish(cache_name, topic_name=topic, value=value)

        print(publish_response)
        item_response = await item_task
        assert(isinstance(item_response, TopicSubscriptionItem.Success))
        assert item_response.value_string == value

    async def errors_with_empty_topic_name(topic_client_async: TopicClientAsync, cache_name: str):
        topic = ""

        resp = await topic_client_async.subscribe(cache_name, topic)
        assert isinstance(resp, TopicSubscribe.Error)
        if isinstance(resp, TopicSubscribe.Error):
            assert resp.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR

    async def succeeds_with_nonexistent_topic(
            client: CacheClientAsync, topic_client_async: TopicClientAsync, cache_name: str
    ) -> None:
        topic = uuid_str()

        resp = await topic_client_async.subscribe(cache_name, topic)
        assert isinstance(resp, TopicSubscribe.SubscriptionAsync)

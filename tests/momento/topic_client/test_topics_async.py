from datetime import timedelta
from functools import partial

from momento import CacheClientAsync, TopicClientAsync
from momento.config.topic_configurations import TopicConfigurations
from momento.errors.error_details import MomentoErrorCode
from momento.responses import TopicPublish, TopicSubscribe, TopicSubscriptionItem
from pytest import fixture
from pytest_describe import behaves_like

from tests.conftest import TEST_AUTH_PROVIDER
from tests.utils import uuid_bytes, uuid_str

from .shared_behaviors_async import (
    TCacheNameValidator,
    TTopicValidator,
    a_cache_name_validator,
    a_topic_validator,
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

    async def publish_happy_path(
        client: CacheClientAsync, topic_client_async: TopicClientAsync, cache_name: str
    ) -> None:
        topic = uuid_str()
        value = uuid_str()

        resp = await topic_client_async.publish(cache_name, topic_name=topic, value=value)
        assert isinstance(resp, TopicPublish.Success)


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

    async def subscribe_happy_path_string(
        client: CacheClientAsync, topic_client_async: TopicClientAsync, cache_name: str
    ) -> None:
        topic = uuid_str()
        value = uuid_str()

        _ = await topic_client_async.publish(cache_name, topic_name=topic, value=value)

        subscribe_response = await topic_client_async.subscribe(cache_name, topic_name=topic)
        assert isinstance(subscribe_response, TopicSubscribe.SubscriptionAsync)

        item_task = subscribe_response.__anext__()
        item_response = await item_task
        assert isinstance(item_response, TopicSubscriptionItem.Text)
        assert item_response.value == value

    async def subscribe_happy_path_string_resume_at_sequence(
        client: CacheClientAsync, topic_client_async: TopicClientAsync, cache_name: str
    ) -> None:
        topic = uuid_str()
        value = uuid_str()

        _ = await topic_client_async.publish(cache_name, topic_name=topic, value="foo")
        _ = await topic_client_async.publish(cache_name, topic_name=topic, value=value)
        _ = await topic_client_async.publish(cache_name, topic_name=topic, value="bar")

        subscribe_response = await topic_client_async.subscribe(
            cache_name, topic_name=topic, resume_at_topic_sequence_number=2
        )
        assert isinstance(subscribe_response, TopicSubscribe.SubscriptionAsync)

        item_task = subscribe_response.__anext__()
        item_response = await item_task
        assert isinstance(item_response, TopicSubscriptionItem.Text)
        assert item_response.value == value

    async def subscribe_happy_path_string_resume_at_invalid_sequence(
        client: CacheClientAsync, topic_client_async: TopicClientAsync, cache_name: str
    ) -> None:
        topic = uuid_str()
        value = uuid_str()

        _ = await topic_client_async.publish(cache_name, topic_name=topic, value=value)

        subscribe_response = await topic_client_async.subscribe(
            cache_name, topic_name=topic, resume_at_topic_sequence_number=300, resume_at_topic_sequence_page=5435435
        )
        assert isinstance(subscribe_response, TopicSubscribe.SubscriptionAsync)

        item_task = subscribe_response.__anext__()
        item_response = await item_task
        assert isinstance(item_response, TopicSubscriptionItem.Text)
        assert item_response.value == value

    async def subscribe_happy_path_binary(
        client: CacheClientAsync, topic_client_async: TopicClientAsync, cache_name: str
    ) -> None:
        topic = uuid_str()
        value = uuid_bytes()

        subscribe_response = await topic_client_async.subscribe(cache_name, topic_name=topic)
        assert isinstance(subscribe_response, TopicSubscribe.SubscriptionAsync)

        item_task = subscribe_response.__anext__()
        publish_response = await topic_client_async.publish(cache_name, topic_name=topic, value=value)

        print(publish_response)
        item_response = await item_task
        assert isinstance(item_response, TopicSubscriptionItem.Binary)
        assert item_response.value == value

    async def succeeds_with_nonexistent_topic(
        client: CacheClientAsync, topic_client_async: TopicClientAsync, cache_name: str
    ) -> None:
        topic = uuid_str()

        resp = await topic_client_async.subscribe(cache_name, topic)
        assert isinstance(resp, TopicSubscribe.SubscriptionAsync)

    async def deadline_exceeded_when_timeout_is_shorter_than_subscribe_response(
        client: CacheClientAsync, topic_client_async: TopicClientAsync, cache_name: str
    ) -> None:
        topic = uuid_str()

        # Default config uses 5 second timeout, should succeed
        resp = await topic_client_async.subscribe(cache_name, topic)
        assert isinstance(resp, TopicSubscribe.SubscriptionAsync)

        # Using a topic client configured with 1ms timeout should cause deadline exceeded error
        async with TopicClientAsync(
            TopicConfigurations.Default.latest().with_client_timeout(timedelta(milliseconds=1)), TEST_AUTH_PROVIDER
        ) as short_timeout_client:
            resp = await short_timeout_client.subscribe(cache_name, topic)
            assert isinstance(resp, TopicSubscribe.Error)
            assert resp.error_code == MomentoErrorCode.TIMEOUT_ERROR

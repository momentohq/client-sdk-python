# from functools import partial

from momento import CacheClient, TopicClient
from momento.errors import MomentoErrorCode
from momento.responses import TopicPublish, TopicSubscribe, TopicSubscriptionItem
from tests.utils import uuid_str

# from pytest import fixture
# from pytest_describe import behaves_like


# from ..cache_client.shared_behaviors_async import a_cache_name_validator, TCacheNameValidator


# @behaves_like(a_cache_name_validator)
def describe_publish() -> None:

    # @fixture
    # def cache_name_validator(topic_client_async: TopicClientAsync) -> TCacheNameValidator:
    #     topic_name = uuid_str()
    #     value = uuid_str()
    #     return partial(topic_client_async.publish, topic_name, value)

    def with_success(client: CacheClient, topic_client: TopicClient, cache_name: str) -> None:
        topic = uuid_str()
        value = uuid_str()

        resp = topic_client.publish(cache_name, topic_name=topic, value=value)
        assert isinstance(resp, TopicPublish.Success)

    def with_invalid_cache(topic_client: TopicClient) -> None:
        cache_name = uuid_str()
        topic = uuid_str()
        value = uuid_str()

        resp = topic_client.publish(cache_name, topic, value)
        assert isinstance(resp, TopicPublish.Error)
        if isinstance(resp, TopicPublish.Error):
            assert resp.error_code == MomentoErrorCode.NOT_FOUND_ERROR

    def with_empty_cache_name(topic_client: TopicClient):
        cache_name = ""
        topic = uuid_str()
        value = uuid_str()

        resp = topic_client.publish(cache_name, topic, value)
        assert isinstance(resp, TopicPublish.Error)
        if isinstance(resp, TopicPublish.Error):
            assert resp.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR

    def with_empty_topic_name(topic_client: TopicClient, cache_name: str):
        topic = ""
        value = uuid_str()

        resp = topic_client.publish(cache_name, topic, value)
        assert isinstance(resp, TopicPublish.Error)
        if isinstance(resp, TopicPublish.Error):
            assert resp.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR


def describe_subscribe() -> None:
    def happy_path(client: CacheClient, topic_client: TopicClient, cache_name: str) -> None:
        topic = uuid_str()
        value = uuid_str()

        subscribe_response = topic_client.subscribe(cache_name, topic_name=topic)
        assert isinstance(subscribe_response, TopicSubscribe.Subscription)

        publish_response = topic_client.publish(cache_name, topic_name=topic, value=value)

        item_response = subscribe_response.item()
        assert isinstance(item_response, TopicSubscriptionItem.Success)
        assert item_response.value_string == value

    def errors_with_invalid_cache(topic_client: TopicClient) -> None:
        cache_name = uuid_str()
        topic = uuid_str()

        resp = topic_client.subscribe(cache_name, topic)
        assert isinstance(resp, TopicSubscribe.Error)
        if isinstance(resp, TopicSubscribe.Error):
            assert resp.error_code == MomentoErrorCode.NOT_FOUND_ERROR

    def errors_with_empty_cache_name(topic_client: TopicClient):
        cache_name = ""
        topic = uuid_str()

        resp = topic_client.subscribe(cache_name, topic)
        assert isinstance(resp, TopicSubscribe.Error)
        if isinstance(resp, TopicSubscribe.Error):
            assert resp.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR

    def errors_with_empty_topic_name(topic_client: TopicClient, cache_name: str):
        topic = ""

        resp = topic_client.subscribe(cache_name, topic)
        assert isinstance(resp, TopicSubscribe.Error)
        if isinstance(resp, TopicSubscribe.Error):
            assert resp.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR

    def succeeds_with_nonexistent_topic(client: CacheClient, topic_client: TopicClient, cache_name: str) -> None:
        topic = uuid_str()

        resp = topic_client.subscribe(cache_name, topic)
        assert isinstance(resp, TopicSubscribe.Subscription)

import os
import time
from typing import Optional

import pytest
from momento.errors.error_details import MomentoErrorCode
from momento.responses.pubsub.subscribe import TopicSubscribe
from momento.topic_client import TopicClient

from tests.conftest import TEST_AUTH_PROVIDER, TEST_TOPIC_CONFIGURATION
from tests.utils import uuid_str

# NOTE: Run these tests locally after setting `SUBSCRIBE_TEST_CACHE` to a cache
# with a subscription limit greater than 2000.
#
# Cannot run these tests against momento-local because python grpc insecure
# channel uses only one grpc/TCP connection under the hood (see
# https://github.com/grpc/grpc/issues/20985) and these tests expect to open
# 2, 10, and 20 connections to test with bursts of 200, 1000, and 2000
# concurrent subscriptions, which would require higher momento subscriptions
# limits that CI/CD does not currently support.


def get_subscribe_test_cache() -> str:
    subscribe_test_cache: Optional[str] = os.getenv("SUBSCRIBE_TEST_CACHE")
    if not subscribe_test_cache:
        raise RuntimeError("Integration tests require SUBSCRIBE_TEST_CACHE env var; see README for more details.")
    return subscribe_test_cache


@pytest.mark.timeout(10)
@pytest.mark.subscription_initialization
def test_should_not_silently_queue_subscribe_requests() -> None:
    with TopicClient(TEST_TOPIC_CONFIGURATION, TEST_AUTH_PROVIDER) as client:
        topic = uuid_str()
        cache = get_subscribe_test_cache()

        # Subscribing 100 times on one channel should succeed
        subscriptions = []
        for _ in range(100):
            resp = client.subscribe(cache, topic)
            assert isinstance(resp, TopicSubscribe.Subscription)
            subscriptions.append(resp)

        # Subscribing 1 more time should fail
        resp = client.subscribe(cache, topic)
        assert isinstance(resp, TopicSubscribe.Error)
        assert resp.error_code == MomentoErrorCode.CLIENT_RESOURCE_EXHAUSTED

        # Cleanup
        for subscription in subscriptions:
            subscription.unsubscribe()


@pytest.mark.timeout(120)
@pytest.mark.subscription_initialization
@pytest.mark.parametrize(
    "num_grpc_channels",
    [
        (2),
        (10),
    ],
)
def test_multiple_stream_channels_handles_subscribe_and_unsubscribe_requests(
    num_grpc_channels: int,
) -> None:
    topic = uuid_str()
    cache = get_subscribe_test_cache()
    max_stream_capacity = num_grpc_channels * 100

    with TopicClient(
        TEST_TOPIC_CONFIGURATION.with_max_subscriptions(max_stream_capacity), TEST_AUTH_PROVIDER
    ) as client:
        # Should subscribe up to max_stream_capacity just fine.
        # Not a burst of concurrent requests since it's the synchronous client.
        subscriptions = []
        for _ in range(max_stream_capacity):
            subscription = client.subscribe(cache, topic)
            assert isinstance(subscription, TopicSubscribe.Subscription)
            subscriptions.append(subscription)
        assert len(subscriptions) == max_stream_capacity

        # Should fail to subscribe beyond max_stream_capacity
        resp = client.subscribe(cache, topic)
        assert isinstance(resp, TopicSubscribe.Error)
        assert resp.error_code == MomentoErrorCode.CLIENT_RESOURCE_EXHAUSTED

        # Unsubscribe half of the subscriptions
        for subscription in subscriptions[: max_stream_capacity // 2]:
            subscription.unsubscribe()

        # Wait for unsubscribes to complete
        time.sleep(1)

        # Should be able to subscribe again, but still not allowing more than max_stream_capacity.
        failed_subscriptions = 0
        for _ in range(max_stream_capacity // 2 + 1):
            subscription = client.subscribe(cache, topic)
            if isinstance(subscription, TopicSubscribe.Error):
                failed_subscriptions += 1
            elif isinstance(subscription, TopicSubscribe.Subscription):
                subscriptions.append(subscription)
            else:
                raise Exception("Unexpected subscription type")

        assert failed_subscriptions == 1

        # Cleanup
        for subscription in subscriptions:
            subscription.unsubscribe()

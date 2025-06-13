import asyncio
import time

import pytest
from momento.errors.error_details import MomentoErrorCode
from momento.responses.pubsub.subscribe import TopicSubscribe
from momento.topic_client_async import TopicClientAsync

from tests.conftest import TEST_AUTH_PROVIDER, TEST_TOPIC_CONFIGURATION
from tests.momento.local.test_topic_subscribe import get_subscribe_test_cache
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


@pytest.mark.timeout(10)
@pytest.mark.asyncio
@pytest.mark.subscription_initialization
async def test_should_not_silently_queue_subscribe_requests() -> None:
    async with TopicClientAsync(TEST_TOPIC_CONFIGURATION, TEST_AUTH_PROVIDER) as client:
        topic = uuid_str()
        cache = get_subscribe_test_cache()

        # Subscribing 100 times on one channel should succeed
        subscriptions = []
        for _ in range(100):
            resp = await client.subscribe(cache, topic)
            assert isinstance(resp, TopicSubscribe.SubscriptionAsync)
            subscriptions.append(resp)

        # Subscribing 1 more time should fail
        resp = await client.subscribe(cache, topic)
        assert isinstance(resp, TopicSubscribe.Error)
        assert resp.error_code == MomentoErrorCode.CLIENT_RESOURCE_EXHAUSTED

        # Cleanup
        for subscription in subscriptions:
            subscription.unsubscribe()


@pytest.mark.asyncio
@pytest.mark.timeout(120)
@pytest.mark.subscription_initialization
@pytest.mark.parametrize(
    "num_grpc_channels",
    [
        (2),
        (10),
    ],
)
async def test_multiple_stream_channels_handles_subscribe_and_unsubscribe_requests(
    num_grpc_channels: int,
) -> None:
    topic = uuid_str()
    cache = get_subscribe_test_cache()
    max_stream_capacity = num_grpc_channels * 100

    async with TopicClientAsync(
        TEST_TOPIC_CONFIGURATION.with_max_subscriptions(max_stream_capacity), TEST_AUTH_PROVIDER
    ) as client:
        # Should subscribe up to max_stream_capacity just fine given a burst of concurrent requests.
        subscribe_requests = []
        for _ in range(max_stream_capacity):
            subscribe_requests.append(client.subscribe(cache, topic))

        subscriptions = await asyncio.gather(*subscribe_requests)
        assert len(subscriptions) == max_stream_capacity
        for subscription in subscriptions:
            assert isinstance(subscription, TopicSubscribe.SubscriptionAsync)

        # Should fail to subscribe beyond max_stream_capacity
        resp = await client.subscribe(cache, topic)
        assert isinstance(resp, TopicSubscribe.Error)
        assert resp.error_code == MomentoErrorCode.CLIENT_RESOURCE_EXHAUSTED

        # Unsubscribe half of the subscriptions
        for subscription in subscriptions[: max_stream_capacity // 2]:
            subscription.unsubscribe()

        # Wait for unsubscribes to complete
        time.sleep(1)

        # Should be able to subscribe again, but still not allowing more than max_stream_capacity.

        new_subscribe_requests = []
        len_subscribe_burst = max_stream_capacity // 2 + 10
        for _ in range(len_subscribe_burst):
            new_subscribe_requests.append(client.subscribe(cache, topic))
        assert len(new_subscribe_requests) == len_subscribe_burst

        new_subscriptions = await asyncio.gather(*new_subscribe_requests)
        assert len(new_subscriptions) == len_subscribe_burst

        failed_subscriptions = 0
        for subscription in new_subscriptions:
            if isinstance(subscription, TopicSubscribe.Error):
                failed_subscriptions += 1
            elif isinstance(subscription, TopicSubscribe.SubscriptionAsync):
                subscriptions.append(subscription)
            else:
                raise Exception("Unexpected subscription type")

        assert failed_subscriptions == 10

        # Cleanup
        for subscription in subscriptions:
            subscription.unsubscribe()

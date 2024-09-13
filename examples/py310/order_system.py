"""
This program simulates a restaurant order system using asynchronous caching and topic-based notifications.
This enables smooth communication between the kitchen and the waiter, allowing the waiter to receive real-time updates on order status.

Actors:
- Kitchen: The kitchen updates the status of orders (e.g., "Preparing", "Ready to Serve") and stores the current order status in a cache.
    After updating the cache, it publishes the order status to a topic to notify subscribers (e.g., waiters).
- Waiter: The waiter subscribes to order updates via a topic. When the kitchen updates the order status, the waiter is notified in real-time through the published message.
    The waiter then processes the update and notifies the customer accordingly.

Flow:
1. The kitchen updates the order status and stores it in a cache (using the `set` method).
2. After storing the order status, the kitchen publishes the update to an order topic, which triggers notifications for all subscribers.
3. The waiter subscribes to this topic and listens for updates. When a new status is published, the waiter receives the notification and informs the customer.
4. The kitchen can update the status multiple times, and the waiter will receive each update in real-time.

Key Components:
- Cache: Stores the latest state of each order (e.g., order number and status) for quick retrieval.
- Topic: Publishes notifications to inform subscribers about updates to the order's status.
"""


import asyncio
import logging
from datetime import timedelta

from momento import (
    CacheClientAsync,
    Configurations,
    CredentialProvider,
    TopicClientAsync,
    TopicConfigurations,
)
from momento.responses import CacheSet, CreateCache, TopicPublish, TopicSubscribe, TopicSubscriptionItem

from example_utils.example_logging import initialize_logging

_AUTH_PROVIDER = CredentialProvider.from_environment_variable("MOMENTO_API_KEY")
_logger = logging.getLogger("order-system-example")

# Constants
# The cache where we will store the order status
_CACHE_NAME = "cache"
# The topic where we will publish order updates
_ORDER_TOPIC = "order_updates"


async def setup_cache(client: CacheClientAsync, cache_name: str) -> None:
    """Ensures that the example cache exists.

    Args:
        client (CacheClientAsync): The cache client to use.

    Raises:
        response.inner_exception: If the cache creation fails.
    """
    response = await client.create_cache(cache_name)
    match response:
        case CreateCache.Success():
            _logger.info("Cache created successfully.")
        case CreateCache.CacheAlreadyExists():
            _logger.info("Using existing cache.")
        case CreateCache.Error():
            _logger.error(f"Failed to create cache: {response.message}")
            raise response.inner_exception


class Kitchen:
    """Class for the kitchen to update the order status."""

    def __init__(
        self, cache_client: CacheClientAsync, topic_client: TopicClientAsync, cache_name: str, order_topic: str
    ):
        self.cache_client = cache_client
        self.topic_client = topic_client
        self.cache_name = cache_name
        self.order_topic = order_topic

    async def update_order_status(self, order_number: int, status: str):
        """Method for the kitchen to update the order status."""
        order_message = f"Order {order_number}: {status}"
        _logger.info(f"Kitchen updating order {order_number} with status: {status}")

        set_response = await self.cache_client.set(self.cache_name, f"order_{order_number}", order_message)
        match set_response:
            case CacheSet.Success():
                _logger.info(f"Updated order status: {order_message}")
            case CacheSet.Error():
                _logger.error(f"Failed to update order status: {set_response.message}")
                return
            case _:
                _logger.error(f"Unexpected response: {set_response}")
                return

        publish_response = await self.topic_client.publish(_CACHE_NAME, _ORDER_TOPIC, order_message)
        match publish_response:
            case TopicPublish.Success():
                _logger.info(f"Published order status: {order_message}")
            case TopicPublish.Error():
                _logger.error(f"Failed to publish order status: {publish_response.message}")


class Waiter:
    """Class for the waiter to poll order status updates and notify the customer."""

    def __init__(self, client: TopicClientAsync, cache_name: str, order_topic: str):
        self.client = client
        self.cache_name = cache_name
        self.order_topic = order_topic

    async def poll_order_updates(self):
        """Method for the waiter to poll the order status updates."""
        subscription = await self.client.subscribe(self.cache_name, self.order_topic)
        match subscription:
            case TopicSubscribe.SubscriptionAsync():
                _logger.info("Waiter subscribed to order updates.")
                await self.poll_subscription(subscription)
            case TopicSubscribe.Error():
                _logger.error(f"Subscription error: {subscription.message}")

    async def poll_subscription(self, subscription: TopicSubscribe.SubscriptionAsync):
        """Poll and process subscription items."""
        async for item in subscription:
            match item:
                case TopicSubscriptionItem.Text():
                    _logger.info(f"Waiter received order update: {item.value}")
                    self.notify_customer(item.value)
                case TopicSubscriptionItem.Error():
                    _logger.error(f"Stream closed: {item.inner_exception.message}")
                    return

    def notify_customer(self, update: str):
        """Notify the customer about the order update."""
        _logger.info(f"Waiter notifies customer: {update}")


# Main function to initialize and run the system
async def main() -> None:
    initialize_logging()

    async with CacheClientAsync(
        Configurations.Laptop.latest(), _AUTH_PROVIDER, timedelta(seconds=60)
    ) as cache_client, TopicClientAsync(TopicConfigurations.Default.latest(), _AUTH_PROVIDER) as topic_client:
        await setup_cache(cache_client, _CACHE_NAME)
        kitchen = Kitchen(cache_client, topic_client, _CACHE_NAME, _ORDER_TOPIC)
        waiter = Waiter(topic_client, _CACHE_NAME, _ORDER_TOPIC)

        waiter_task = asyncio.create_task(waiter.poll_order_updates())
        await asyncio.sleep(1)
        _logger.info("The waiter is ready to update customers.")

        # Kitchen updates the order status
        await kitchen.update_order_status(order_number=1, status="Preparing")

        # Simulate kitchen preparing the order
        await asyncio.sleep(2)

        # Kitchen updates the order status
        await kitchen.update_order_status(order_number=1, status="Ready to Serve")

        # Simulate waiter serving the order
        await asyncio.sleep(5)
        _logger.info("The waiter has served the order.")

        # Now cancel the waiter task
        waiter_task.cancel()

        try:
            await waiter_task
        except asyncio.CancelledError:
            _logger.info("Waiter task cancelled successfully.")


if __name__ == "__main__":
    asyncio.run(main())

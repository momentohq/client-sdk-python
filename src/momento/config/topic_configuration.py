from __future__ import annotations

from abc import ABC, abstractmethod


class TopicConfigurationBase(ABC):
    @abstractmethod
    def get_max_subscriptions(self) -> int:
        pass

    @abstractmethod
    def with_max_subscriptions(self, max_subscriptions: int) -> TopicConfiguration:
        pass


class TopicConfiguration(TopicConfigurationBase):
    """Configuration options for Momento topic client."""

    def __init__(self, max_subscriptions: int = 0):
        """Instantiate a configuration.

        Args:
            max_subscriptions (int): The maximum number of subscriptions the client is expected
            to handle. Because each gRPC channel can handle 100 connections, we must explicitly
            open multiple channels to accommodate the load. NOTE: if the number of connection
            attempts exceeds the number the channels can support, program execution will block
            and hang.
        """
        self._max_subscriptions = max_subscriptions

    def get_max_subscriptions(self) -> int:
        return self._max_subscriptions

    def with_max_subscriptions(self, max_subscriptions: int) -> TopicConfiguration:
        return TopicConfiguration(max_subscriptions)

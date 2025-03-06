from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import timedelta

from momento.config.transport.topic_transport_strategy import TopicTransportStrategy


class TopicConfigurationBase(ABC):
    @abstractmethod
    def get_max_subscriptions(self) -> int:
        pass

    @abstractmethod
    def with_max_subscriptions(self, max_subscriptions: int) -> TopicConfiguration:
        pass

    @abstractmethod
    def get_transport_strategy(self) -> TopicTransportStrategy:
        pass

    @abstractmethod
    def with_transport_strategy(self, transport_strategy: TopicTransportStrategy) -> TopicConfiguration:
        pass

    @abstractmethod
    def with_client_timeout(self, client_timeout: timedelta) -> TopicConfiguration:
        pass


class TopicConfiguration(TopicConfigurationBase):
    """Configuration options for Momento topic client."""

    def __init__(self, transport_strategy: TopicTransportStrategy, max_subscriptions: int = 0):
        """Instantiate a configuration.

        Args:
            transport_strategy (TopicTransportStrategy): Configuration options for networking with
            the Momento pubsub service.
            max_subscriptions (int): The maximum number of subscriptions the client is expected
            to handle. Because each gRPC channel can handle 100 connections, we must explicitly
            open multiple channels to accommodate the load. NOTE: if the number of connection
            attempts exceeds the number the channels can support, program execution will block
            and hang.
        """
        self._max_subscriptions = max_subscriptions
        self._transport_strategy = transport_strategy

    def get_max_subscriptions(self) -> int:
        return self._max_subscriptions

    def with_max_subscriptions(self, max_subscriptions: int) -> TopicConfiguration:
        return TopicConfiguration(self._transport_strategy, max_subscriptions)

    def get_transport_strategy(self) -> TopicTransportStrategy:
        """Access the transport strategy.

        Returns:
            TopicTransportStrategy: the current configuration options for wire interactions with the Momento pubsub service.
        """
        return self._transport_strategy

    def with_transport_strategy(self, transport_strategy: TopicTransportStrategy) -> TopicConfiguration:
        """Copy constructor for overriding TopicTransportStrategy.

        Args:
            transport_strategy (TopicTransportStrategy): the new TopicTransportStrategy.

        Returns:
            TopicConfiguration: the new TopicConfiguration with the specified TopicTransportStrategy.
        """
        return TopicConfiguration(transport_strategy, self._max_subscriptions)

    def with_client_timeout(self, client_timeout: timedelta) -> TopicConfiguration:
        """Copies the TopicConfiguration and sets the new client-side timeout in the copy's TopicTransportStrategy.

        Args:
            client_timeout (timedelta): the new client-side timeout.

        Return:
            TopicConfiguration: the new TopicConfiguration.
        """
        return TopicConfiguration(self._transport_strategy.with_client_timeout(client_timeout), self._max_subscriptions)

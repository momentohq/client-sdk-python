from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import timedelta

from .transport.transport_strategy import TransportStrategy


class ConfigurationBase(ABC):

    # TODO: RetryStrategy and Middlewares

    @abstractmethod
    def get_transport_strategy(self) -> TransportStrategy:
        pass

    @abstractmethod
    def with_transport_strategy(self, transport_strategy: TransportStrategy) -> Configuration:
        pass

    @abstractmethod
    def with_client_timeout(self, client_timeout: timedelta) -> Configuration:
        pass


class Configuration(ConfigurationBase):
    """Configuration options for Momento Simple Cache Client"""

    def __init__(self, transport_strategy: TransportStrategy):
        """:param transport_strategy: Configuration options for wire interactions with the Momento service"""
        self._transport_strategy = transport_strategy

    def get_transport_strategy(self) -> TransportStrategy:
        """:return: the current configuration options for wire interactions with the Momento service"""
        return self._transport_strategy

    def with_transport_strategy(self, transport_strategy: TransportStrategy) -> Configuration:
        """Copy constructor for overriding TransportStrategy

        :param transport_strategy: TransportStrategy
        :return: a new Configuration object with the specified TransportStrategy
        """
        return Configuration(transport_strategy)

    def with_client_timeout(self, client_timeout: timedelta) -> Configuration:
        """Convenience copy constructor that updates the client-side timeout setting in the TransportStrategy

        :param client_timeout: timedelta specifying the new timeout value
        :return: a new Configuration object with its TransportStrategy updated to use the specified client timeout
        """
        return Configuration(self._transport_strategy.with_client_timeout(client_timeout))

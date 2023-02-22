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
    """Configuration options for Momento Simple Cache Client."""

    def __init__(self, transport_strategy: TransportStrategy):
        """Instantiate a Configuration.

        Args:
            transport_strategy (TransportStrategy): Configuration options for networking with
            the Momento service.
        """
        self._transport_strategy = transport_strategy

    def get_transport_strategy(self) -> TransportStrategy:
        """Access the transport strategy.

        Returns:
            TransportStrategy: the current configuration options for wire interactions with the Momento service.
        """
        return self._transport_strategy

    def with_transport_strategy(self, transport_strategy: TransportStrategy) -> Configuration:
        """Copy constructor for overriding TransportStrategy.

        Args:
            transport_strategy (TransportStrategy): the new TransportStrategy.

        Returns:
            Configuration: a new Configuration object with the specified TransportStrategy.
        """
        return Configuration(transport_strategy)

    def with_client_timeout(self, client_timeout: timedelta) -> Configuration:
        """Convenience copy constructor that updates the client-side timeout setting in the TransportStrategy.

        Args:
            client_timeout (timedelta): timedelta specifying the new timeout value.

        Return:
            Configuration: a new Configuration object with its TransportStrategy updated to use
            the specified client timeout.
        """
        return Configuration(self._transport_strategy.with_client_timeout(client_timeout))

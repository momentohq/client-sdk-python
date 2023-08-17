from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import timedelta

from .transport.transport_strategy import TransportStrategy


class VectorIndexConfigurationBase(ABC):
    @abstractmethod
    def get_transport_strategy(self) -> TransportStrategy:
        pass

    @abstractmethod
    def with_transport_strategy(self, transport_strategy: TransportStrategy) -> VectorIndexConfigurationBase:
        pass

    @abstractmethod
    def with_client_timeout(self, client_timeout: timedelta) -> VectorIndexConfigurationBase:
        pass


class VectorIndexConfiguration(VectorIndexConfigurationBase):
    """Configuration options for Momento Vector Index Client."""

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

    def with_transport_strategy(self, transport_strategy: TransportStrategy) -> VectorIndexConfiguration:
        """Copy constructor for overriding TransportStrategy.

        Args:
            transport_strategy (TransportStrategy): the new TransportStrategy.

        Returns:
            Configuration: the new Configuration with the specified TransportStrategy.
        """
        return VectorIndexConfiguration(transport_strategy)

    def with_client_timeout(self, client_timeout: timedelta) -> VectorIndexConfiguration:
        """Copies the Configuration and sets the new client-side timeout in the copy's TransportStrategy.

        Args:
            client_timeout (timedelta): the new client-side timeout.

        Return:
            Configuration: the new Configuration.
        """
        return VectorIndexConfiguration(self._transport_strategy.with_client_timeout(client_timeout))

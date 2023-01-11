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
    def __init__(self, transport_strategy: TransportStrategy):
        self._transport_strategy = transport_strategy

    def get_transport_strategy(self) -> TransportStrategy:
        return self._transport_strategy

    def with_transport_strategy(self, transport_strategy: TransportStrategy) -> Configuration:
        return Configuration(transport_strategy)

    def with_client_timeout(self, client_timeout: timedelta) -> Configuration:
        return Configuration(self._transport_strategy.with_client_timeout(client_timeout))

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import timedelta

from momento.config.transport.transport_strategy import TransportStrategy
from momento.retry.retry_strategy import RetryStrategy


class AuthConfigurationBase(ABC):
    @abstractmethod
    def get_retry_strategy(self) -> RetryStrategy:
        pass

    @abstractmethod
    def with_retry_strategy(self, retry_strategy: RetryStrategy) -> AuthConfiguration:
        pass

    @abstractmethod
    def get_transport_strategy(self) -> TransportStrategy:
        pass

    @abstractmethod
    def with_transport_strategy(self, transport_strategy: TransportStrategy) -> AuthConfiguration:
        pass

    @abstractmethod
    def with_client_timeout(self, client_timeout: timedelta) -> AuthConfiguration:
        pass


class AuthConfiguration(AuthConfigurationBase):
    """AuthConfiguration options for Momento Simple Cache Client."""

    def __init__(self, transport_strategy: TransportStrategy, retry_strategy: RetryStrategy):
        """Instantiate a AuthConfiguration.

        Args:
            transport_strategy (TransportStrategy): AuthConfiguration options for networking with
            the Momento service.
            retry_strategy (RetryStrategy): the strategy to use when determining whether to retry a grpc call.
        """
        self._transport_strategy = transport_strategy
        self._retry_strategy = retry_strategy

    def get_retry_strategy(self) -> RetryStrategy:
        """Access the retry strategy.

        Returns:
            RetryStrategy: the strategy to use when determining whether to retry a grpc call.
        """
        return self._retry_strategy

    def with_retry_strategy(self, retry_strategy: RetryStrategy) -> AuthConfiguration:
        """Copy constructor for overriding RetryStrategy.

        Args:
            retry_strategy (RetryStrategy): the new RetryStrategy.

        Returns:
            AuthConfiguration: the new AuthConfiguration with the specified RetryStrategy.
        """
        return AuthConfiguration(self._transport_strategy, retry_strategy)

    def get_transport_strategy(self) -> TransportStrategy:
        """Access the transport strategy.

        Returns:
            TransportStrategy: the current configuration options for wire interactions with the Momento service.
        """
        return self._transport_strategy

    def with_transport_strategy(self, transport_strategy: TransportStrategy) -> AuthConfiguration:
        """Copy constructor for overriding TransportStrategy.

        Args:
            transport_strategy (TransportStrategy): the new TransportStrategy.

        Returns:
            AuthConfiguration: the new AuthConfiguration with the specified TransportStrategy.
        """
        return AuthConfiguration(transport_strategy, self._retry_strategy)

    def with_client_timeout(self, client_timeout: timedelta) -> AuthConfiguration:
        """Copies the AuthConfiguration and sets the new client-side timeout in the copy's TransportStrategy.

        Args:
            client_timeout (timedelta): the new client-side timeout.

        Return:
            AuthConfiguration: the new AuthConfiguration.
        """
        return AuthConfiguration(self._transport_strategy.with_client_timeout(client_timeout), self._retry_strategy)

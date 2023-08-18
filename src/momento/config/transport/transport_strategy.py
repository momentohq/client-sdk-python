from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import timedelta
from typing import Optional

from momento.internal._utilities import _validate_request_timeout

from .grpc_configuration import GrpcConfiguration


class TransportStrategy(ABC):
    """Configures the network options for communicating with the Momento service."""

    @abstractmethod
    def get_grpc_configuration(self) -> GrpcConfiguration:
        """Access the gRPC configuration.

        Returns:
            GrpcConfiguration: low-level gRPC settings for the Momento client's communication.
        """
        pass

    @abstractmethod
    def with_grpc_configuration(self, grpc_configuration: GrpcConfiguration) -> TransportStrategy:
        """Copy constructor for overriding the gRPC configuration.

        Args:
            grpc_configuration (GrpcConfiguration): the new gRPC configuration.

        Returns:
            TransportStrategy: a new TransportStrategy with the specified gRPC config.
        """
        pass

    @abstractmethod
    def with_client_timeout(self, client_timeout: timedelta) -> TransportStrategy:
        """Copies the TransportStrategy and updates the copy's client-side timeout.

        Args:
            client_timeout (timedelta): the new client-side timeout.

        Returns:
            TransportStrategy: the new TransportStrategy.
        """
        pass

    @abstractmethod
    def with_eager_connection_timeout(self, eager_connection_timeout: timedelta) -> TransportStrategy:
        """Copies the TransportStrategy and updates the copy's eager_connection_timeout.

        Args:
            eager_connection_timeout (timedelta): the new eager connection timeout.

        Returns:
            TransportStrategy: the new TransportStrategy.
        """
        pass


class StaticGrpcConfiguration(GrpcConfiguration):
    DEFAULT_EAGER_CONNECTION_TIMEOUT_SECONDS = 30

    def __init__(self, deadline: timedelta, eager_connection_timeout: Optional[timedelta] = None):
        self._deadline = deadline
        self._eager_connection_timeout = eager_connection_timeout

    def get_deadline(self) -> timedelta:
        return self._deadline

    def with_deadline(self, deadline: timedelta) -> GrpcConfiguration:
        _validate_request_timeout(deadline)
        return StaticGrpcConfiguration(deadline, self._eager_connection_timeout)

    def get_eager_connection_timeout(self) -> timedelta:
        return self._eager_connection_timeout or timedelta(seconds=self.DEFAULT_EAGER_CONNECTION_TIMEOUT_SECONDS)

    def with_eager_connection_timeout(self, timeout: timedelta) -> GrpcConfiguration:
        _validate_request_timeout(timeout)
        return StaticGrpcConfiguration(self._deadline, timeout)


class StaticTransportStrategy(TransportStrategy):
    def __init__(self, grpc_configuration: GrpcConfiguration):
        self._grpc_configuration = grpc_configuration

    def get_grpc_configuration(self) -> GrpcConfiguration:
        return self._grpc_configuration

    def with_grpc_configuration(self, grpc_configuration: GrpcConfiguration) -> TransportStrategy:
        return StaticTransportStrategy(grpc_configuration)

    def with_client_timeout(self, client_timeout: timedelta) -> TransportStrategy:
        return StaticTransportStrategy(self._grpc_configuration.with_deadline(client_timeout))

    def with_eager_connection_timeout(self, eager_connection_timeout: timedelta) -> TransportStrategy:
        return StaticTransportStrategy(self._grpc_configuration.with_eager_connection_timeout(eager_connection_timeout))

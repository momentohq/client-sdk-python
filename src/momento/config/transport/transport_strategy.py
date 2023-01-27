from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import timedelta

from momento.internal._utilities import _validate_request_timeout

from .grpc_configuration import GrpcConfiguration


class TransportStrategy(ABC):
    """Configures the network options for communicating with the Momento service."""

    @abstractmethod
    def get_grpc_configuration(self) -> GrpcConfiguration:
        """:return: low-level gRPC settings for the Momento client's communication"""
        pass

    @abstractmethod
    def with_grpc_configuration(self, grpc_configuration: GrpcConfiguration) -> TransportStrategy:
        """Copy constructor for overriding the gRPC configuration

        :param grpc_configuration: GrpcConfiguration
        :return: a new TransportStrategy with the specified gRPC config.
        """
        pass

    @abstractmethod
    def with_client_timeout(self, client_timeout: timedelta) -> TransportStrategy:
        """Copy constructor to update the client-side timeout

        :param client_timeout: timedelta representing the new client timeout value
        :return: a new TransportStrategy with the specified client timeout.
        """
        pass


class StaticGrpcConfiguration(GrpcConfiguration):
    def __init__(self, deadline: timedelta):
        self._deadline = deadline

    def get_deadline(self) -> timedelta:
        return self._deadline

    def with_deadline(self, deadline: timedelta) -> GrpcConfiguration:
        _validate_request_timeout(deadline)
        return StaticGrpcConfiguration(deadline)


class StaticTransportStrategy(TransportStrategy):
    def __init__(self, grpc_configuration: GrpcConfiguration):
        self._grpc_configuration = grpc_configuration

    def get_grpc_configuration(self) -> GrpcConfiguration:
        return self._grpc_configuration

    def with_grpc_configuration(self, grpc_configuration: GrpcConfiguration) -> TransportStrategy:
        return StaticTransportStrategy(grpc_configuration)

    def with_client_timeout(self, client_timeout: timedelta) -> TransportStrategy:
        return StaticTransportStrategy(self._grpc_configuration.with_deadline(client_timeout))

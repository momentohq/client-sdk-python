from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import timedelta

from .grpc_configuration import GrpcConfiguration
from momento._utilities._data_validation import _validate_request_timeout


class TransportStrategy(ABC):
    @abstractmethod
    def get_grpc_configuration(self) -> GrpcConfiguration:
        pass

    @abstractmethod
    def with_grpc_configuration(self, grpc_configuration: GrpcConfiguration) -> TransportStrategy:
        pass

    @abstractmethod
    def with_client_timeout(self, client_timeout: timedelta) -> TransportStrategy:
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

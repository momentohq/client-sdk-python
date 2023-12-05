from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import timedelta
from pathlib import Path
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


class StaticGrpcConfiguration(GrpcConfiguration):
    def __init__(self, deadline: timedelta, root_certificates_pem: Optional[bytes] = None):
        self._deadline = deadline
        self._root_certificates_pem = root_certificates_pem

    def get_deadline(self) -> timedelta:
        return self._deadline

    def with_deadline(self, deadline: timedelta) -> GrpcConfiguration:
        _validate_request_timeout(deadline)
        return StaticGrpcConfiguration(deadline, self._root_certificates_pem)

    def with_root_certificates_pem(self, root_certificates_pem_path: Path) -> GrpcConfiguration:
        try:
            root_certificates_pem_bytes = root_certificates_pem_path.read_bytes()
        except FileNotFoundError as e:
            raise FileNotFoundError(f"Root certificate file not found at path: {root_certificates_pem_path}") from e
        except PermissionError as e:
            raise PermissionError(f"Root certificate file not readable at path: {root_certificates_pem_path}") from e
        return StaticGrpcConfiguration(self._deadline, root_certificates_pem_bytes)

    def get_root_certificates_pem(self) -> Optional[bytes]:
        return self._root_certificates_pem


class StaticTransportStrategy(TransportStrategy):
    def __init__(self, grpc_configuration: GrpcConfiguration):
        self._grpc_configuration = grpc_configuration

    def get_grpc_configuration(self) -> GrpcConfiguration:
        return self._grpc_configuration

    def with_grpc_configuration(self, grpc_configuration: GrpcConfiguration) -> TransportStrategy:
        return StaticTransportStrategy(grpc_configuration)

    def with_client_timeout(self, client_timeout: timedelta) -> TransportStrategy:
        return self.with_grpc_configuration(self._grpc_configuration.with_deadline(client_timeout))

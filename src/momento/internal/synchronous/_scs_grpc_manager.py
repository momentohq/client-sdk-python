from __future__ import annotations

import grpc
import pkg_resources
from momento_wire_types import cacheclient_pb2_grpc as cache_client
from momento_wire_types import controlclient_pb2_grpc as control_client

from momento.auth import CredentialProvider
from momento.config import Configuration
from momento.internal.synchronous._add_header_client_interceptor import (
    AddHeaderClientInterceptor,
    Header,
)
from momento.internal.synchronous._retry_interceptor import RetryInterceptor
from momento.retry import RetryStrategy


class _ControlGrpcManager:
    """Internal gRPC control mananger."""

    version = pkg_resources.get_distribution("momento").version

    def __init__(self, configuration: Configuration, credential_provider: CredentialProvider):
        self._secure_channel = grpc.secure_channel(
            target=credential_provider.control_endpoint, credentials=grpc.ssl_channel_credentials()
        )
        intercept_channel = grpc.intercept_channel(
            self._secure_channel, *_interceptors(credential_provider.auth_token, configuration.get_retry_strategy())
        )
        self._stub = control_client.ScsControlStub(intercept_channel)  # type: ignore[no-untyped-call]

    def close(self) -> None:
        self._secure_channel.close()

    def stub(self) -> control_client.ScsControlStub:
        return self._stub


class _DataGrpcManager:
    """Internal gRPC data mananger."""

    version = pkg_resources.get_distribution("momento").version

    def __init__(self, configuration: Configuration, credential_provider: CredentialProvider):
        self._secure_channel = grpc.secure_channel(
            target=credential_provider.cache_endpoint,
            credentials=grpc.ssl_channel_credentials(),
        )
        intercept_channel = grpc.intercept_channel(
            self._secure_channel, *_interceptors(credential_provider.auth_token, configuration.get_retry_strategy())
        )
        self._stub = cache_client.ScsStub(intercept_channel)  # type: ignore[no-untyped-call]

    def close(self) -> None:
        self._secure_channel.close()

    def stub(self) -> cache_client.ScsStub:
        return self._stub


def _interceptors(auth_token: str, retry_strategy: RetryStrategy) -> list[grpc.UnaryUnaryClientInterceptor]:
    headers = [Header("authorization", auth_token), Header("agent", f"python:{_ControlGrpcManager.version}")]
    return [AddHeaderClientInterceptor(headers), RetryInterceptor(retry_strategy)]

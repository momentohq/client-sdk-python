from __future__ import annotations

import grpc
from momento_wire_types import controlclient_pb2_grpc as control_client
from momento_wire_types import vectorindex_pb2_grpc as vector_index_client

from momento.auth import CredentialProvider
from momento.config import VectorIndexConfiguration
from momento.internal._utilities import momento_version
from momento.internal.synchronous._add_header_client_interceptor import (
    AddHeaderClientInterceptor,
    Header,
)


class _VectorIndexControlGrpcManager:
    """Internal gRPC control mananger."""

    version = momento_version

    def __init__(self, configuration: VectorIndexConfiguration, credential_provider: CredentialProvider):
        self._secure_channel = grpc.secure_channel(
            target=credential_provider.control_endpoint,
            credentials=grpc.ssl_channel_credentials(),
        )
        intercept_channel = grpc.intercept_channel(self._secure_channel, *_interceptors(credential_provider.auth_token))
        self._stub = control_client.ScsControlStub(intercept_channel)  # type: ignore[no-untyped-call]

    def close(self) -> None:
        self._secure_channel.close()

    def stub(self) -> control_client.ScsControlStub:
        return self._stub


class _VectorIndexDataGrpcManager:
    """Internal gRPC vector index data manager."""

    version = momento_version

    def __init__(self, configuration: VectorIndexConfiguration, credential_provider: CredentialProvider):
        self._secure_channel = grpc.secure_channel(
            target=credential_provider.vector_endpoint,
            credentials=grpc.ssl_channel_credentials(),
        )
        intercept_channel = grpc.intercept_channel(self._secure_channel, *_interceptors(credential_provider.auth_token))
        self._stub = vector_index_client.VectorIndexStub(intercept_channel)  # type: ignore[no-untyped-call]

    def close(self) -> None:
        self._secure_channel.close()

    def stub(self) -> vector_index_client.VectorIndexStub:
        return self._stub


def _interceptors(auth_token: str) -> list[grpc.UnaryUnaryClientInterceptor]:
    headers = [Header("authorization", auth_token), Header("agent", f"python:{_VectorIndexControlGrpcManager.version}")]
    return list(filter(None, [AddHeaderClientInterceptor(headers)]))

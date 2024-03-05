from __future__ import annotations

import grpc
from momento_wire_types import controlclient_pb2_grpc as control_client
from momento_wire_types import vectorindex_pb2_grpc as vector_index_client

from momento.auth import CredentialProvider
from momento.config import VectorIndexConfiguration
from momento.internal._utilities import momento_version
from momento.internal._utilities._channel_credentials import (
    channel_credentials_from_root_certs_or_default,
)
from momento.internal._utilities._grpc_channel_options import (
    grpc_control_channel_options_from_grpc_config,
    grpc_data_channel_options_from_grpc_config,
)

from ._add_header_client_interceptor import AddHeaderClientInterceptor, Header


class _VectorIndexControlGrpcManager:
    """Internal gRPC control manager."""

    version = momento_version

    def __init__(self, configuration: VectorIndexConfiguration, credential_provider: CredentialProvider):
        self._secure_channel = grpc.aio.secure_channel(
            target=credential_provider.control_endpoint,
            credentials=channel_credentials_from_root_certs_or_default(configuration),
            interceptors=_interceptors(credential_provider.auth_token),
            options=grpc_control_channel_options_from_grpc_config(
                grpc_config=configuration.get_transport_strategy().get_grpc_configuration(),
            ),
        )

    async def close(self) -> None:
        await self._secure_channel.close()

    def async_stub(self) -> control_client.ScsControlStub:
        return control_client.ScsControlStub(self._secure_channel)  # type: ignore[no-untyped-call]


class _VectorIndexDataGrpcManager:
    """Internal gRPC vector index data manager."""

    version = momento_version

    def __init__(self, configuration: VectorIndexConfiguration, credential_provider: CredentialProvider):
        self._secure_channel = grpc.aio.secure_channel(
            target=credential_provider.vector_endpoint,
            credentials=channel_credentials_from_root_certs_or_default(configuration),
            interceptors=_interceptors(credential_provider.auth_token),
            options=grpc_data_channel_options_from_grpc_config(
                configuration.get_transport_strategy().get_grpc_configuration()
            ),
        )

    async def close(self) -> None:
        await self._secure_channel.close()

    def async_stub(self) -> vector_index_client.VectorIndexStub:
        return vector_index_client.VectorIndexStub(self._secure_channel)  # type: ignore[no-untyped-call]


def _interceptors(auth_token: str) -> list[grpc.aio.ClientInterceptor]:
    headers = [
        Header("authorization", auth_token),
        Header("agent", f"python:{_VectorIndexControlGrpcManager.version}"),
    ]
    return list(
        filter(
            None,
            [AddHeaderClientInterceptor(headers)],
        )
    )

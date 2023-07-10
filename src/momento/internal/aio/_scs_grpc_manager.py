from __future__ import annotations

from typing import Optional

import grpc
from momento_wire_types import cacheclient_pb2_grpc as cache_client
from momento_wire_types import cachepubsub_pb2_grpc as pubsub_client
from momento_wire_types import controlclient_pb2_grpc as control_client

from momento.auth import CredentialProvider
from momento.config import Configuration, TopicConfiguration
from momento.internal._utilities import momento_version
from momento.retry import RetryStrategy

from ._add_header_client_interceptor import (
    AddHeaderClientInterceptor,
    AddHeaderStreamingClientInterceptor,
    Header,
)
from ._retry_interceptor import RetryInterceptor


class _ControlGrpcManager:
    """Internal gRPC control manager."""

    version = momento_version

    def __init__(self, configuration: Configuration, credential_provider: CredentialProvider):
        self._secure_channel = grpc.aio.secure_channel(
            target=credential_provider.control_endpoint,
            credentials=grpc.ssl_channel_credentials(),
            interceptors=_interceptors(credential_provider.auth_token, configuration.get_retry_strategy()),
        )

    async def close(self) -> None:
        await self._secure_channel.close()

    def async_stub(self) -> control_client.ScsControlStub:
        return control_client.ScsControlStub(self._secure_channel)  # type: ignore[no-untyped-call]


class _DataGrpcManager:
    """Internal gRPC data manager."""

    version = momento_version

    def __init__(self, configuration: Configuration, credential_provider: CredentialProvider):
        self._secure_channel = grpc.aio.secure_channel(
            target=credential_provider.cache_endpoint,
            credentials=grpc.ssl_channel_credentials(),
            interceptors=_interceptors(credential_provider.auth_token, configuration.get_retry_strategy()),
            # Here is where you would pass override configuration to the underlying C gRPC layer.
            # However, I have tried several different tuning options here and did not see any
            # performance improvements, so sticking with the defaults for now.
            # For more info on the performance investigations:
            # https://github.com/momentohq/client-sdk-python/issues/120
            # For more info on available gRPC config options:
            # https://grpc.github.io/grpc/python/grpc.html
            # https://grpc.github.io/grpc/python/glossary.html#term-channel_arguments
            # https://github.com/grpc/grpc/blob/v1.46.x/include/grpc/impl/codegen/grpc_types.h#L140
            options=[
                # ('grpc.max_concurrent_streams', 1000),
                # ('grpc.use_local_subchannel_pool', 1),
                # (experimental.ChannelOptions.SingleThreadedUnaryStream, 1)
            ],
        )

    async def close(self) -> None:
        await self._secure_channel.close()

    def async_stub(self) -> cache_client.ScsStub:
        return cache_client.ScsStub(self._secure_channel)  # type: ignore[no-untyped-call]


class _PubsubGrpcManager:
    """Internal gRPC pubsub manager."""

    version = momento_version

    def __init__(self, configuration: TopicConfiguration, credential_provider: CredentialProvider):
        self._secure_channel = grpc.aio.secure_channel(
            target=credential_provider.cache_endpoint,
            credentials=grpc.ssl_channel_credentials(),
            interceptors=_interceptors(credential_provider.auth_token, None),
        )

    async def close(self) -> None:
        await self._secure_channel.close()

    def async_stub(self) -> pubsub_client.PubsubStub:
        return pubsub_client.PubsubStub(self._secure_channel)  # type: ignore[no-untyped-call]


class _PubsubGrpcStreamManager:
    """Internal gRPC pubsub stream manager."""

    version = momento_version

    def __init__(self, configuration: TopicConfiguration, credential_provider: CredentialProvider):
        self._secure_channel = grpc.aio.secure_channel(
            target=credential_provider.cache_endpoint,
            credentials=grpc.ssl_channel_credentials(),
            interceptors=_stream_interceptors(credential_provider.auth_token),
        )

    async def close(self) -> None:
        await self._secure_channel.close()

    def async_stub(self) -> pubsub_client.PubsubStub:
        return pubsub_client.PubsubStub(self._secure_channel)  # type: ignore[no-untyped-call]


def _interceptors(auth_token: str, retry_strategy: Optional[RetryStrategy] = None) -> list[grpc.aio.ClientInterceptor]:
    headers = [
        Header("authorization", auth_token),
        Header("agent", f"python:{_ControlGrpcManager.version}"),
    ]
    return list(
        filter(
            None,
            [
                AddHeaderClientInterceptor(headers),
                RetryInterceptor(retry_strategy) if retry_strategy else None,
            ],
        )
    )


def _stream_interceptors(auth_token: str) -> list[grpc.aio.UnaryStreamClientInterceptor]:
    headers = [
        Header("authorization", auth_token),
        Header("agent", f"python:{_PubsubGrpcStreamManager.version}"),
    ]
    return [AddHeaderStreamingClientInterceptor(headers)]

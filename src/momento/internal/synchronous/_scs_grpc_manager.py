from __future__ import annotations

import datetime
from typing import Optional

import grpc
from momento_wire_types import cacheclient_pb2_grpc as cache_client
from momento_wire_types import cachepubsub_pb2_grpc as pubsub_client
from momento_wire_types import controlclient_pb2_grpc as control_client

from momento import logs
from momento.auth import CredentialProvider
from momento.config import Configuration, TopicConfiguration
from momento.internal._utilities import momento_version
from momento.internal.synchronous._add_header_client_interceptor import (
    AddHeaderClientInterceptor,
    AddHeaderStreamingClientInterceptor,
    Header,
)
from momento.internal.synchronous._retry_interceptor import RetryInterceptor
from momento.retry import RetryStrategy


class _ControlGrpcManager:
    """Internal gRPC control mananger."""

    version = momento_version

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

    version = momento_version

    def __init__(self, configuration: Configuration, credential_provider: CredentialProvider,
                 logger: logs.logger):
        self._logger = logger
        self._secure_channel = grpc.secure_channel(
            target=credential_provider.cache_endpoint,
            credentials=grpc.ssl_channel_credentials(),
        )

        intercept_channel = grpc.intercept_channel(
            self._secure_channel, *_interceptors(credential_provider.auth_token, configuration.get_retry_strategy())
        )
        self._stub = cache_client.ScsStub(intercept_channel)  # type: ignore[no-untyped-call]

        self._eagerly_connect(datetime.datetime.utcnow() + configuration.get_transport_strategy().get_grpc_configuration().get_deadline())

    def _eagerly_connect(self, deadline: datetime.datetime):
        def on_state_change(state):
            READY = grpc.ChannelConnectivity.READY
            CONNECTING = grpc.ChannelConnectivity.CONNECTING

            if state == READY:
                self._logger.info("Connected to gRPC service")
            elif state == CONNECTING:
                self._logger.info("Transitioned to CONNECTING to gRPC service")
                # Polling the connectivity state to achieve behavior similar to `watchConnectivityState`
                while self._secure_channel.check_connectivity_state(True) == CONNECTING:
                    self._logger.info("Waiting to CONNECTING to gRPC service")
                    if datetime.datetime.utcnow() >= deadline:
                        self._logger.error("Unable to connect: deadline exceeded.")
                        return
                # Recheck the state after exiting the loop
                new_state = self._secure_channel.check_connectivity_state(False)
                on_state_change(new_state)
            else:
                self._logger.error(f"Unexpected connection state: {state}. Please contact Momento if this persists.")

        # Initial check and connection attempt
        initial_state = self._secure_channel.check_connectivity_state(True)
        if initial_state == grpc.ChannelConnectivity.READY:
            self._logger.info("Already connected to gRPC service")
        else:
            self._secure_channel.subscribe(on_state_change, try_to_connect=True)

    def close(self) -> None:
        self._secure_channel.close()

    def stub(self) -> cache_client.ScsStub:
        return self._stub


class _PubsubGrpcManager:
    """Internal gRPC pubsub manager."""

    version = momento_version

    def __init__(self, configuration: TopicConfiguration, credential_provider: CredentialProvider):
        self._secure_channel = grpc.secure_channel(
            target=credential_provider.cache_endpoint,
            credentials=grpc.ssl_channel_credentials(),
        )
        intercept_channel = grpc.intercept_channel(
            self._secure_channel, *_interceptors(credential_provider.auth_token, None)
        )
        self._stub = pubsub_client.PubsubStub(intercept_channel)  # type: ignore[no-untyped-call]

    def close(self) -> None:
        self._secure_channel.close()

    def stub(self) -> pubsub_client.PubsubStub:
        return self._stub


class _PubsubGrpcStreamManager:
    """Internal gRPC pubsub stream manager."""

    version = momento_version

    def __init__(self, configuration: TopicConfiguration, credential_provider: CredentialProvider):
        self._secure_channel = grpc.secure_channel(
            target=credential_provider.cache_endpoint,
            credentials=grpc.ssl_channel_credentials(),
        )
        intercept_channel = grpc.intercept_channel(
            self._secure_channel, *_stream_interceptors(credential_provider.auth_token)
        )
        self._stub = pubsub_client.PubsubStub(intercept_channel)  # type: ignore[no-untyped-call]

    def close(self) -> None:
        self._secure_channel.close()

    def stub(self) -> pubsub_client.PubsubStub:
        return self._stub


def _interceptors(
    auth_token: str, retry_strategy: Optional[RetryStrategy] = None
) -> list[grpc.UnaryUnaryClientInterceptor]:
    headers = [Header("authorization", auth_token), Header("agent", f"python:{_ControlGrpcManager.version}")]
    return list(
        filter(
            None, [AddHeaderClientInterceptor(headers), RetryInterceptor(retry_strategy) if retry_strategy else None]
        )
    )


def _stream_interceptors(auth_token: str) -> list[grpc.UnaryStreamClientInterceptor]:
    headers = [
        Header("authorization", auth_token),
        Header("agent", f"python:{_PubsubGrpcStreamManager.version}"),
    ]
    return [AddHeaderStreamingClientInterceptor(headers)]

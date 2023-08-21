from __future__ import annotations

import threading
from datetime import timedelta
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

    def __init__(self, configuration: Configuration, credential_provider: CredentialProvider):
        self._logger = logs.logger
        self._secure_channel = grpc.secure_channel(
            target=credential_provider.cache_endpoint,
            credentials=grpc.ssl_channel_credentials(),
        )

        intercept_channel = grpc.intercept_channel(
            self._secure_channel, *_interceptors(credential_provider.auth_token, configuration.get_retry_strategy())
        )
        self._stub = cache_client.ScsStub(intercept_channel)  # type: ignore[no-untyped-call]

        self._eagerly_connect(configuration)

    """
        This method tries to connect to Momento's server eagerly in async fashion until
        EAGER_CONNECTION_TIMEOUT elapses.
    """

    def _eagerly_connect(self, configuration: Configuration) -> None:
        eager_connection_timeout: timedelta = (
            configuration.get_transport_strategy().get_grpc_configuration().get_eager_connection_timeout()
        )

        # An event to track whether we were able to establish an eager connection
        connection_event = threading.Event()

        def on_timeout() -> None:
            self._logger.debug(
                "We could not establish an eager connection within %d seconds",
                eager_connection_timeout.seconds,
            )
            # the subscription is no longer needed; it was only meant to watch if we could connect eagerly
            self._secure_channel.unsubscribe(on_state_change)

        """
        A callback that is triggered whenever a connection's state changes. We explicitly subscribe to
        to the channel to notify us of state transitions. This method essentially handles unsubscribing
        as soon as we reach the desired state (or an unexpected one). In theory this callback isn't needed
        to eagerly connect, but we still need it to not have a lurking subscription.
        """

        def on_state_change(state: grpc.ChannelConnectivity) -> None:
            ready: grpc.ChannelConnectivity = grpc.ChannelConnectivity.READY
            connecting: grpc.ChannelConnectivity = grpc.ChannelConnectivity.CONNECTING
            idle: grpc.ChannelConnectivity = grpc.ChannelConnectivity.IDLE
            if state == ready:
                self._logger.debug("Connected to Momento's server!")
                # we successfully connected within the timeout and we no longer need this subscription
                self._secure_channel.unsubscribe(on_state_change)
                # this indicates to the connection event that we were successful in establishing an eager connection
                connection_event.set()

            elif state == idle:
                self._logger.debug("State is idle; waiting to transition to CONNECTING")
            elif state == connecting:
                self._logger.debug("State transitioned to CONNECTING; waiting to get READY")
            else:
                self._logger.debug(f"Unexpected connection state: {state}.")
                # we could not connect within the timeout and we no longer need this subscription
                self._secure_channel.unsubscribe(on_state_change)

        # we subscribe to the channel that notifies us of state transitions, and the timer above will take care
        # of unsubscribing from the channel incase the timeout has elapsed.
        self._secure_channel.subscribe(on_state_change, try_to_connect=True)

        connection_established = connection_event.wait(eager_connection_timeout.seconds)
        if not connection_established:
            on_timeout()

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

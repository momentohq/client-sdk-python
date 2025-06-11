from __future__ import annotations

import uuid
from threading import Event
from typing import List, Optional

import grpc
from momento_wire_types import cacheclient_pb2_grpc as cache_client
from momento_wire_types import cachepubsub_pb2_grpc as pubsub_client
from momento_wire_types import controlclient_pb2_grpc as control_client
from momento_wire_types import token_pb2_grpc as token_client

from momento import logs
from momento.auth import CredentialProvider
from momento.config import Configuration, TopicConfiguration
from momento.config.auth_configuration import AuthConfiguration
from momento.config.middleware import MiddlewareRequestHandlerContext
from momento.config.middleware.models import CONNECTION_ID_KEY
from momento.config.middleware.synchronous import Middleware
from momento.errors.exceptions import ClientResourceExhaustedException, ConnectionException
from momento.internal._utilities import PYTHON_RUNTIME_VERSION, ClientType
from momento.internal._utilities._channel_credentials import (
    channel_credentials_from_root_certs_or_default,
)
from momento.internal._utilities._grpc_channel_options import (
    grpc_control_channel_options_from_grpc_config,
    grpc_data_channel_options_from_grpc_config,
    grpc_topic_channel_options_from_grpc_config,
)
from momento.internal.services import Service
from momento.internal.synchronous._add_header_client_interceptor import (
    AddHeaderClientInterceptor,
    AddHeaderStreamingClientInterceptor,
    Header,
)
from momento.internal.synchronous._middleware_interceptor import MiddlewareInterceptor
from momento.internal.synchronous._retry_interceptor import RetryInterceptor
from momento.retry import RetryStrategy


class _ControlGrpcManager:
    """Internal gRPC control manager."""

    def __init__(self, configuration: Configuration, credential_provider: CredentialProvider):
        if credential_provider.port == 443:
            self._channel = grpc.secure_channel(
                target=credential_provider.control_endpoint,
                credentials=channel_credentials_from_root_certs_or_default(configuration),
                options=grpc_control_channel_options_from_grpc_config(
                    grpc_config=configuration.get_transport_strategy().get_grpc_configuration(),
                ),
            )
        else:
            self._channel = grpc.insecure_channel(
                target=f"{credential_provider.control_endpoint}:{credential_provider.port}",
                options=grpc_control_channel_options_from_grpc_config(
                    grpc_config=configuration.get_transport_strategy().get_grpc_configuration(),
                ),
            )
        intercept_channel = grpc.intercept_channel(
            self._channel,
            *_interceptors(
                credential_provider.auth_token,
                ClientType.CACHE,
                configuration.get_sync_middlewares(),
                configuration.get_retry_strategy(),
            ),
        )
        self._stub = control_client.ScsControlStub(intercept_channel)  # type: ignore[no-untyped-call]

    def close(self) -> None:
        self._channel.close()

    def stub(self) -> control_client.ScsControlStub:
        return self._stub


class _DataGrpcManager:
    """Internal gRPC data manager."""

    def __init__(self, configuration: Configuration, credential_provider: CredentialProvider):
        self._logger = logs.logger
        if credential_provider.port == 443:
            self._channel = grpc.secure_channel(
                target=credential_provider.cache_endpoint,
                credentials=channel_credentials_from_root_certs_or_default(configuration),
                options=grpc_data_channel_options_from_grpc_config(
                    configuration.get_transport_strategy().get_grpc_configuration()
                ),
            )
        else:
            self._channel = grpc.insecure_channel(
                target=f"{credential_provider.cache_endpoint}:{credential_provider.port}",
                options=grpc_data_channel_options_from_grpc_config(
                    configuration.get_transport_strategy().get_grpc_configuration()
                ),
            )

        intercept_channel = grpc.intercept_channel(
            self._channel,
            *_interceptors(
                credential_provider.auth_token,
                ClientType.CACHE,
                configuration.get_sync_middlewares(),
                configuration.get_retry_strategy(),
            ),
        )
        self._stub = cache_client.ScsStub(intercept_channel)  # type: ignore[no-untyped-call]

    """
        This method tries to eagerly connect to Momento's server until
        EAGER_CONNECTION_TIMEOUT elapses.
    """

    def eagerly_connect(self, timeout_seconds: float) -> None:
        self._logger.debug(
            "Attempting to create an eager connection with Momento's server within " f"{timeout_seconds} seconds"
        )

        # An event to track whether we were able to establish an eager connection
        # This is required as we create a subscription to eagerly connect and observe the state changes
        # We do NOT want the subscription to lurk around after it's job is done.
        connection_event = Event()

        def on_timeout() -> None:
            self._logger.debug(
                "We could not establish an eager connection within %d seconds",
                timeout_seconds,
            )
            # the subscription is no longer needed; it was only meant to watch if we could connect eagerly
            self._channel.unsubscribe(on_state_change)
            self._channel.close()
            raise ConnectionException(
                message="Failed to connect to Momento's server within given eager connection timeout",
                service=Service.CACHE,
            )

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
                self._logger.debug("Connected to Momento's server! Happy Caching!")
                # we successfully connected within the timeout and we no longer need this subscription
                self._channel.unsubscribe(on_state_change)
                # this indicates to the connection event that we were successful in establishing an eager connection
                connection_event.set()

            elif state == idle:
                self._logger.debug("State is idle; waiting to transition to CONNECTING")
            elif state == connecting:
                self._logger.debug("State transitioned to CONNECTING; waiting to get READY")
            else:
                self._logger.warning(f"Unexpected connection state while trying to eagerly connect: {state}")
                # we could not connect within the timeout and we no longer need this subscription
                self._channel.unsubscribe(on_state_change)
                connection_event.set()

        # we subscribe to the channel that notifies us of state transitions, and the connection event above will take
        # care of unsubscribing from the channel incase the timeout has elapsed.
        self._channel.subscribe(on_state_change, try_to_connect=True)

        connection_established = connection_event.wait(timeout_seconds)
        if not connection_established:
            on_timeout()

    def close(self) -> None:
        self._logger.debug("Closing and tearing down gRPC channel")
        self._channel.close()

    def stub(self) -> cache_client.ScsStub:
        return self._stub


class _PubsubGrpcManager:
    """Internal gRPC pubsub manager."""

    def __init__(self, configuration: TopicConfiguration, credential_provider: CredentialProvider):
        if credential_provider.port == 443:
            self._channel = grpc.secure_channel(
                target=credential_provider.cache_endpoint,
                credentials=grpc.ssl_channel_credentials(),
                options=grpc_topic_channel_options_from_grpc_config(
                    configuration.get_transport_strategy().get_grpc_configuration()
                ),
            )
        else:
            self._channel = grpc.insecure_channel(
                target=f"{credential_provider.cache_endpoint}:{credential_provider.port}",
                options=grpc_topic_channel_options_from_grpc_config(
                    configuration.get_transport_strategy().get_grpc_configuration()
                ),
            )
        intercept_channel = grpc.intercept_channel(
            self._channel, *_interceptors(credential_provider.auth_token, ClientType.TOPIC, [], None)
        )
        self._stub = pubsub_client.PubsubStub(intercept_channel)  # type: ignore[no-untyped-call]

    def close(self) -> None:
        self._channel.close()

    def stub(self) -> pubsub_client.PubsubStub:
        return self._stub


class _PubsubGrpcStreamManager:
    """Internal gRPC pubsub stream manager."""

    def __init__(self, configuration: TopicConfiguration, credential_provider: CredentialProvider):
        if credential_provider.port == 443:
            self._secure_channel = grpc.secure_channel(
                target=credential_provider.cache_endpoint,
                credentials=grpc.ssl_channel_credentials(),
                options=grpc_topic_channel_options_from_grpc_config(
                    configuration.get_transport_strategy().get_grpc_configuration()
                ),
            )
        else:
            self._secure_channel = grpc.insecure_channel(
                target=f"{credential_provider.cache_endpoint}:{credential_provider.port}",
                options=grpc_topic_channel_options_from_grpc_config(
                    configuration.get_transport_strategy().get_grpc_configuration()
                ),
            )
        intercept_channel = grpc.intercept_channel(
            self._secure_channel, *_stream_interceptors(credential_provider.auth_token, ClientType.TOPIC)
        )
        self._stub = pubsub_client.PubsubStub(intercept_channel)  # type: ignore[no-untyped-call]
        self._active_streams_count = 0

    def close(self) -> None:
        self._secure_channel.close()

    def stub(self) -> pubsub_client.PubsubStub:
        if self._active_streams_count >= 100:
            raise ClientResourceExhaustedException(
                message="Already at max number of concurrent streams",
                service=Service.TOPICS,
            )
        self._active_streams_count += 1
        return self._stub

    def decrement_stream_count(self) -> None:
        self._active_streams_count -= 1


class _TokenGrpcManager:
    """Internal gRPC token manager."""

    def __init__(self, configuration: AuthConfiguration, credential_provider: CredentialProvider):
        if credential_provider.port == 443:
            self._channel = grpc.secure_channel(
                target=credential_provider.token_endpoint,
                credentials=grpc.ssl_channel_credentials(),
                options=grpc_control_channel_options_from_grpc_config(
                    grpc_config=configuration.get_transport_strategy().get_grpc_configuration(),
                ),
            )
        else:
            self._channel = grpc.insecure_channel(
                target=f"{credential_provider.token_endpoint}:{credential_provider.port}",
                options=grpc_control_channel_options_from_grpc_config(
                    grpc_config=configuration.get_transport_strategy().get_grpc_configuration(),
                ),
            )
        intercept_channel = grpc.intercept_channel(
            self._channel,
            *_interceptors(credential_provider.auth_token, ClientType.TOKEN, [], configuration.get_retry_strategy()),
        )
        self._stub = token_client.TokenStub(intercept_channel)  # type: ignore[no-untyped-call]

    def close(self) -> None:
        self._channel.close()

    def stub(self) -> token_client.TokenStub:
        return self._stub


def _interceptors(
    auth_token: str,
    client_type: ClientType,
    middleware: List[Middleware],
    retry_strategy: Optional[RetryStrategy] = None,
) -> list[grpc.UnaryUnaryClientInterceptor]:
    from momento import __version__ as momento_version

    context = MiddlewareRequestHandlerContext({CONNECTION_ID_KEY: str(uuid.uuid4())})

    headers = [
        Header("authorization", auth_token),
        Header("agent", f"python:{client_type.value}:{momento_version}"),
        Header("runtime-version", f"python {PYTHON_RUNTIME_VERSION}"),
    ]
    return list(
        filter(
            None,
            [
                AddHeaderClientInterceptor(headers),
                RetryInterceptor(retry_strategy) if retry_strategy else None,
                MiddlewareInterceptor(middleware, context) if middleware else None,
            ],
        )
    )


def _stream_interceptors(auth_token: str, client_type: ClientType) -> list[grpc.UnaryStreamClientInterceptor]:
    # This is here to avoid circular imports
    from momento import __version__ as momento_version

    headers = [
        Header("authorization", auth_token),
        Header("agent", f"python:{client_type.value}:{momento_version}"),
        Header("runtime-version", f"python {PYTHON_RUNTIME_VERSION}"),
    ]
    return [AddHeaderStreamingClientInterceptor(headers)]

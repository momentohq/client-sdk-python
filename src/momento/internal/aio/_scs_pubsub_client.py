from __future__ import annotations

import math
from datetime import timedelta
from typing import Callable

from momento_wire_types import cachepubsub_pb2 as pubsub_pb
from momento_wire_types import cachepubsub_pb2_grpc as pubsub_grpc

from momento import logs
from momento.auth import CredentialProvider
from momento.config import TopicConfiguration
from momento.errors import convert_error
from momento.errors.exceptions import ClientResourceExhaustedException
from momento.internal._utilities import _validate_cache_name, _validate_topic_name
from momento.internal.aio._scs_grpc_manager import (
    _PubsubGrpcManager,
    _PubsubGrpcStreamManager,
)
from momento.internal.services import Service
from momento.responses import (
    TopicPublish,
    TopicPublishResponse,
    TopicSubscribe,
    TopicSubscribeResponse,
)


class _ScsPubsubClient:
    """Internal pubsub client."""

    def __init__(self, configuration: TopicConfiguration, credential_provider: CredentialProvider):
        endpoint = credential_provider.cache_endpoint
        self._logger = logs.logger
        self._logger.debug("Pubsub client instantiated with endpoint: %s", endpoint)
        self._endpoint = endpoint

        default_deadline: timedelta = configuration.get_transport_strategy().get_grpc_configuration().get_deadline()
        self._default_deadline_seconds = default_deadline.total_seconds()

        # Default to a single channel and scale up if necessary. Each channel can support
        # 100 subscriptions. Issuing more subscribe requests than you have channels to handle
        # will cause a ClientResourceExhaustedException.
        num_channels = 1
        num_subscriptions = configuration.get_max_subscriptions()
        if num_subscriptions > 0:
            num_channels = math.ceil(num_subscriptions / 100.0)
            self._logger.debug(f"creating {num_channels} subscription channels")
        self._stream_managers = [
            _PubsubGrpcStreamManager(configuration, credential_provider) for i in range(0, num_channels)
        ]

        # Default to 4 unary pubsub channels. TODO: Make this configurable.
        self._unary_managers = [_PubsubGrpcManager(configuration, credential_provider) for i in range(0, 4)]
        self._stream_manager_count = 0
        self._unary_manager_count = 0

    @property
    def endpoint(self) -> str:
        return self._endpoint

    async def publish(self, cache_name: str, topic_name: str, value: str | bytes) -> TopicPublishResponse:
        try:
            _validate_cache_name(cache_name)
            _validate_topic_name(topic_name)

            if isinstance(value, str):
                topic_value = pubsub_pb._TopicValue(text=value)
            else:
                topic_value = pubsub_pb._TopicValue(binary=value)

            request = pubsub_pb._PublishRequest(
                cache_name=cache_name,
                topic=topic_name,
                value=topic_value,
            )

            await self._get_unary_stub().Publish(  # type: ignore[misc]
                request,
                timeout=self._default_deadline_seconds,
            )
            return TopicPublish.Success()
        except Exception as e:
            self._log_request_error("publish", e)
            return TopicPublish.Error(convert_error(e, Service.TOPICS))

    async def subscribe(
        self,
        cache_name: str,
        topic_name: str,
        resume_at_topic_sequence_number: int = 0,
        resume_at_topic_sequence_page: int = 0,
    ) -> TopicSubscribeResponse:
        try:
            _validate_cache_name(cache_name)
            _validate_topic_name(topic_name)

            request = pubsub_pb._SubscriptionRequest(
                cache_name=cache_name,
                topic=topic_name,
                resume_at_topic_sequence_number=resume_at_topic_sequence_number,
                sequence_page=resume_at_topic_sequence_page,
            )
            stub, decrement_stream_count = self._get_stream_stub()
            stream = stub.Subscribe(  # type: ignore[misc]
                request,
                timeout=self._default_deadline_seconds,
            )

            # Ping the stream to provide a nice error message if the cache does not exist.
            msg: pubsub_pb._SubscriptionItem = await stream.read()  # type: ignore[misc]
            msg_type: str = msg.WhichOneof("kind")
            if msg_type == "heartbeat":
                # The first message to a new subscription is always a heartbeat.
                pass
            else:
                err = Exception(f"expected a heartbeat message but got '{msg_type}'")
                self._log_request_error("subscribe", err)
                return TopicSubscribe.Error(convert_error(err, Service.TOPICS))
            return TopicSubscribe.SubscriptionAsync(
                cache_name,
                topic_name,
                client_stream=stream,  # type: ignore[misc]
                decrement_stream_count_method=decrement_stream_count,
            )
        except Exception as e:
            self._log_request_error("subscribe", e)
            return TopicSubscribe.Error(convert_error(e, Service.TOPICS))

    def _log_request_error(self, request_type: str, e: Exception) -> None:
        self._logger.warning(f"{request_type} failed with exception: {e}")

    def _get_unary_stub(self) -> pubsub_grpc.PubsubStub:
        # Simply round-robin through the unary managers.
        # Unary requests will eventually complete (unlike long-lived subscriptions),
        # so we do not need the same bookkeeping logic here.
        manager = self._unary_managers[self._unary_manager_count % len(self._unary_managers)]
        self._unary_manager_count += 1
        return manager.async_stub()

    def _get_stream_stub(self) -> tuple[pubsub_grpc.PubsubStub, Callable[[], None]]:
        # Try to get a client with capacity for another subscription by round-robining through the stubs.
        # Allow up to max_stream_capacity attempts to account for large bursts of requests.
        max_stream_capacity = len(self._stream_managers) * 100
        for _ in range(0, max_stream_capacity):
            try:
                manager = self._stream_managers[self._stream_manager_count % len(self._stream_managers)]
                self._stream_manager_count += 1
                return manager.async_stub(), manager.decrement_stream_count
            except ClientResourceExhaustedException:
                # If the stub is at capacity, continue to the next one.
                continue

        # Otherwise return an error if no stubs have capacity.
        raise ClientResourceExhaustedException(
            message="Maximum number of active subscriptions reached",
            service=Service.TOPICS,
        )

    async def close(self) -> None:
        for unary_client in self._unary_managers:
            await unary_client.close()
        for stream_client in self._stream_managers:
            await stream_client.close()

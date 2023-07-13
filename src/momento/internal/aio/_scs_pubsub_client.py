from __future__ import annotations

import math

from momento_wire_types import cachepubsub_pb2 as pubsub_pb
from momento_wire_types import cachepubsub_pb2_grpc as pubsub_grpc

from momento import logs
from momento.auth import CredentialProvider
from momento.config import TopicConfiguration
from momento.errors import convert_error
from momento.internal._utilities import _validate_cache_name, _validate_topic_name
from momento.internal.aio._scs_grpc_manager import (
    _PubsubGrpcManager,
    _PubsubGrpcStreamManager,
)
from momento.responses import (
    TopicPublish,
    TopicPublishResponse,
    TopicSubscribe,
    TopicSubscribeResponse,
)


class _ScsPubsubClient:
    """Internal pubsub client."""

    stream_topic_manager_count = 0

    def __init__(self, configuration: TopicConfiguration, credential_provider: CredentialProvider):
        endpoint = credential_provider.cache_endpoint
        self._logger = logs.logger
        self._logger.debug("Pubsub client instantiated with endpoint: %s", endpoint)
        self._endpoint = endpoint

        num_subscriptions = configuration.get_max_subscriptions()
        # Default to a single channel and scale up if necessary. Each channel can support
        # 100 subscriptions. Issuing more subscribe requests than you have channels to handle
        # will cause the last request to hang indefinitely, so it's important to get this right.
        num_channels = 1
        if num_subscriptions > 0:
            num_channels = math.ceil(num_subscriptions / 100.0)
            self._logger.debug(f"creating {num_channels} subscription channels")

        self._grpc_manager = _PubsubGrpcManager(configuration, credential_provider)
        self._stream_managers = [
            _PubsubGrpcStreamManager(configuration, credential_provider) for i in range(0, num_channels)
        ]

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

            await self._get_stub().Publish(  # type: ignore[misc]
                request,
            )
            return TopicPublish.Success()
        except Exception as e:
            self._log_request_error("publish", e)
            return TopicPublish.Error(convert_error(e))

    async def subscribe(self, cache_name: str, topic_name: str) -> TopicSubscribeResponse:
        try:
            _validate_cache_name(cache_name)
            _validate_topic_name(topic_name)

            request = pubsub_pb._SubscriptionRequest(
                cache_name=cache_name,
                topic=topic_name,
                # TODO: resume_at_topic_sequence_number
            )
            stream = self._get_stream_stub().Subscribe(  # type: ignore[misc]
                request,
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
                return TopicSubscribe.Error(convert_error(err))
            return TopicSubscribe.SubscriptionAsync(cache_name, topic_name, client_stream=stream)  # type: ignore[misc]
        except Exception as e:
            self._log_request_error("subscribe", e)
            return TopicSubscribe.Error(convert_error(e))

    def _log_request_error(self, request_type: str, e: Exception) -> None:
        self._logger.warning(f"{request_type} failed with exception: {e}")

    def _get_stub(self) -> pubsub_grpc.PubsubStub:
        return self._grpc_manager.async_stub()

    def _get_stream_stub(self) -> pubsub_grpc.PubsubStub:
        stub = self._stream_managers[self.stream_topic_manager_count % len(self._stream_managers)].async_stub()
        self.stream_topic_manager_count += 1
        return stub

    async def close(self) -> None:
        await self._grpc_manager.close()
        for stream_client in self._stream_managers:
            await stream_client.close()

from __future__ import annotations

import time

from momento_wire_types import cachepubsub_pb2 as pubsub_pb
from momento_wire_types import cachepubsub_pb2_grpc as pubsub_grpc

from momento import logs
from momento.auth import CredentialProvider
from momento.config import TopicConfiguration
from momento.errors import convert_error

from momento.internal.aio._scs_grpc_manager import _PubsubGrpcManager, _PubsubGrpcStreamManager
from momento.internal.aio._utilities import make_metadata
from momento.internal._utilities import _validate_cache_name
from momento.responses import (
    TopicPublish, TopicPublishResponse, TopicSubscribe, TopicSubscribeResponse
)


class _ScsPubsubClient:
    """Internal pubsub client."""

    def __init__(self, configuration: TopicConfiguration, credential_provider: CredentialProvider):
        endpoint = credential_provider.cache_endpoint
        self._logger = logs.logger
        self._logger.debug("Simple cache data client instantiated with endpoint: %s", endpoint)
        self._endpoint = endpoint

        self._grpc_manager = _PubsubGrpcManager(configuration, credential_provider)
        # TODO: this should be a pool whose size is determined bu max_subscriptions
        self._stream_manager = _PubsubGrpcStreamManager(configuration, credential_provider)

    @property
    def endpoint(self) -> str:
        return self._endpoint

    async def publish(self, cache_name: str, topic_name: str, value: str | bytes) -> TopicPublishResponse:
        try:
            _validate_cache_name(cache_name)
            # TODO: validate topic name
            if isinstance(value, str):
                topic_value = pubsub_pb._TopicValue(text=value)
            else:
                topic_value = pubsub_pb._TopicValue(binary=value)

            request = pubsub_pb._PublishRequest(
                # TODO: so, uh, this is part of the request now?
                cache_name=cache_name,
                topic=topic_name,
                value=topic_value,
            )

            await self._build_stub().Publish(
                request,
                # TODO: so, uh, we just don't use this here?
                metadata=make_metadata(cache_name)
            )
            return TopicPublish.Success()
        except Exception as e:
            # TODO: add this logging
            # self._log_request_error("increment", e)
            return TopicPublish.Error(convert_error(e))

    async def subscribe(self, cache_name: str, topic_name: str) -> TopicSubscribeResponse:
        try:
            _validate_cache_name(cache_name)
            # TODO: validate topic name
            request = pubsub_pb._SubscriptionRequest(
                cache_name=cache_name,
                topic=topic_name,
                # TODO: resume_at_topic_sequence_number
            )
            stream = self._build_stream_stub().Subscribe(
                request,
            )

            # Ping the stream to provide a nice error message if the cache does not exist.
            msg = await stream.read()
            msg_type = msg.WhichOneof("kind")
            if msg_type == "heartbeat":
                # The first message to a new subscription is always a heartbeat.
                pass
            else:
                # TODO: improve this
                return TopicSubscribe.Error(
                    convert_error(Exception(f"expected a heartbeat message but got '{msg_type}'"))
                )
            return TopicSubscribe.Subscription(cache_name, topic_name, client_stream=stream)
        except Exception as e:
            raise e

    def _build_stub(self) -> pubsub_grpc.PubsubStub:
        return self._grpc_manager.async_stub()

    # TODO: pretty sure `async_stub` isn't what we want for the stream
    def _build_stream_stub(self) -> pubsub_grpc.PubsubStub:
        return self._stream_manager.async_stub()

    async def close(self) -> None:
        await self._grpc_manager.close()

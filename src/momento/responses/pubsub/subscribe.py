from abc import ABC
from typing import Optional

from grpc._channel import _MultiThreadedRendezvous
from grpc.aio._interceptor import InterceptedUnaryStreamCall
from momento_wire_types import cachepubsub_pb2

from ... import logs
from ..mixins import ErrorResponseMixin
from ..response import PubsubResponse
from .subscription_item import TopicSubscriptionItem, TopicSubscriptionItemResponse


class TopicSubscribeResponse(PubsubResponse):
    """Parent response type for a topic `publish` request.

    Its subtypes are:
    - `TopicSubscribe.Subscription`
    - `TopicSubscribe.SubscriptionAsync`
    - `TopicSubscribe.Error`
    """


class TopicSubscribe(ABC):
    """Groups all `TopicSubscribeResponse` derived types under a common namespace."""

    class SubscriptionBase(TopicSubscribeResponse):
        """Base class for common logic shared between async and synchronous subscriptions."""

        _logger = logs.logger
        _last_known_sequence_number: Optional[int] = None

        def _process_result(self, result: cachepubsub_pb2._SubscriptionItem) -> Optional[TopicSubscriptionItemResponse]:
            msg_type: str = result.WhichOneof("kind")
            if msg_type == "item":
                self._last_known_sequence_number = result.item.topic_sequence_number
                value = result.item.value
                value_type: str = value.WhichOneof("kind")
                if value_type == "text":
                    return TopicSubscriptionItem.Success(bytes(value.text, "utf-8"))
                elif value_type == "bytes":
                    return TopicSubscriptionItem.Success(value.bytes)
            elif msg_type == "heartbeat":
                self._logger.debug("client stream received heartbeat")
                return None
            elif msg_type == "discontinuity":
                self._logger.debug("client stream received discontinuity")
                return None
            self._logger.debug(f"client stream received unknown type: {msg_type}")
            return None

    class SubscriptionAsync(SubscriptionBase):
        """Provides the async version of a topic subscription."""

        def __init__(self, cache_name: str, topic_name: str, client_stream: InterceptedUnaryStreamCall):
            self._cache_name = cache_name
            self._topic_name = topic_name
            self._client_stream = client_stream  # type: ignore[misc]

        async def item(self) -> TopicSubscriptionItemResponse:
            """Returns the next published item from the subscription."""
            while True:
                try:
                    result: cachepubsub_pb2._SubscriptionItem = await self._client_stream.read()  # type: ignore[misc]
                except Exception as e:
                    self._logger.debug("Error reading from client stream: %s", e)
                    # TODO: attempt reconnect
                    continue
                item = self._process_result(result)
                # if item is null, we've received a heartbeat or discontinuity and should continue
                if item is not None:
                    return item

    class Subscription(SubscriptionBase):
        """Provides the synchronous version of a topic subscription."""

        def __init__(self, cache_name: str, topic_name: str, client_stream: _MultiThreadedRendezvous):
            self._cache_name = cache_name
            self._topic_name = topic_name
            self._client_stream = client_stream  # type: ignore[misc]

        def item(self) -> TopicSubscriptionItemResponse:
            """Returns the next published item from the subscription."""
            while True:
                try:
                    result: cachepubsub_pb2._SubscriptionItem = self._client_stream.next()  # type: ignore[misc]
                except Exception as e:
                    self._logger.debug("Error reading from client stream: %s", e)
                    # TODO: attempt reconnect
                    continue
                item = self._process_result(result)
                # if item is null, we've received a heartbeat or discontinuity and should continue
                if item is not None:
                    return item

    class Error(TopicSubscribeResponse, ErrorResponseMixin):
        """Contains information about an error returned from a request.

        This includes:
        - `error_code`: `MomentoErrorCode` value for the error.
        - `message`: a detailed error message.
        """

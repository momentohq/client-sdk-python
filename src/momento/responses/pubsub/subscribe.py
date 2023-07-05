from abc import ABC
from typing import Any

from .subscription_item import TopicSubscriptionItem, TopicSubscriptionItemResponse
from ..mixins import ErrorResponseMixin
from ..response import PubsubResponse
from ... import logs


class TopicSubscribeResponse(PubsubResponse):
    """Parent response type for a topic `publish` request.

    Its subtypes are:
    - `TopicSubscribe.Subscription`
    - `TopicSubscribe.Error`
    """

class TopicSubscribe(ABC):
    """Groups all `TopicSubscribeResponse` derived types under a common namespace."""

    class Subscription(TopicSubscribeResponse):
        """Indicates the request was successful"""
        def __init__(self, cache_name: str, topic_name: str, client_stream: Any):
            self._logger = logs.logger
            self._cache_name = cache_name
            self._topic_name = topic_name
            self._client_stream = client_stream
            self._last_known_sequence_number = None

        async def item(self) -> TopicSubscriptionItemResponse:
            while True:
                # TODO: assuming that an exception means we need to reconnect?
                try:
                    result = await self._client_stream.read()
                except Exception as e:
                    self._logger.debug("Error reading from client stream: %s", e)
                    # TODO: attempt reconnect
                    continue
                msg_type = result.WhichOneof("kind")
                if msg_type == "item":
                    self._last_known_sequence_number = result.item.topic_sequence_number
                    value = result.item.value
                    value_type = value.WhichOneof("kind")
                    if value_type == "text":
                        return TopicSubscriptionItem.Success(bytes(value.text, 'utf-8'))
                    elif value_type == "bytes":
                        return TopicSubscriptionItem.Success(value.bytes)
                elif msg_type == "heartbeat":
                    self._logger.debug("client stream received heartbeat")
                    continue
                elif type == "discontinuity":
                    self._logger.debug("client stream received discontinuity")
                    continue
                else:
                    self._logger.debug(f"client stream received unknown type: {msg_type}")
                    continue

    class Error(TopicSubscribeResponse, ErrorResponseMixin):
        """Contains information about an error returned from a request.

        This includes:
        - `error_code`: `MomentoErrorCode` value for the error.
        - `message`: a detailed error message.
        """

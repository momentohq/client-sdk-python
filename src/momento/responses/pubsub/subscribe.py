from __future__ import annotations

from abc import ABC
from concurrent.futures._base import CancelledError
from typing import Optional

from grpc._channel import _MultiThreadedRendezvous
from grpc.aio._interceptor import InterceptedUnaryStreamCall
from momento_wire_types import cachepubsub_pb2

from ... import logs
from ...errors import MomentoErrorCode, SdkException
from ..mixins import ErrorResponseMixin
from ..response import PubsubResponse
from .subscription_item import TopicSubscriptionItem, TopicSubscriptionItemResponse


class TopicSubscribeResponse(PubsubResponse):
    """Parent response type for a topic `publish` request.

    Its subtypes are:
    - `TopicSubscribe.Subscription`
    - `TopicSubscribe.SubscriptionAsync`
    - `TopicSubscribe.Error`

    Both the ``Subscription` and the `SubscriptionAsync` response subtypes are
    iterators that yield subscription items.
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

        def __aiter__(self) -> TopicSubscribe.SubscriptionAsync:
            return self

        async def __anext__(self) -> TopicSubscriptionItemResponse:
            """Retrieves the next published item from the subscription.

            The item method returns a response object that is resolved to a type-safe object
            of one of the following:

            - TopicSubscriptionItem.Success
            - TopicSubscriptionItem.Error

            Pattern matching can be used to operate on the appropriate subtype.
            For example, in python 3.10+ if you're receiving a subscription item::

                async for response in subscription:
                    match response:
                        case TopicSubscriptionItem.Success():
                            return response.value_string # value_bytes is also available
                        case TopicSubscriptionItem.Error():
                            ...there was an error retrieving the item...

            or equivalently in earlier versions of python::

                async for response in subscription:
                    if isinstance(response, TopicSubscriptionItem.Success):
                        return response.value_string # value_bytes is also available
                    elif isinstance(response, TopicSubscriptionItem.Error):
                        ...there was an error retrieving the item...
            """
            while True:
                try:
                    result: cachepubsub_pb2._SubscriptionItem = await self._client_stream.read()  # type: ignore[misc]
                    item = self._process_result(result)
                    # if item is null, we've received a heartbeat or discontinuity and should continue
                    if item is not None:
                        return item
                except (CancelledError, StopAsyncIteration) as e:
                    err = SdkException(
                        f"Client subscription has been cancelled by {type(e)}",
                        MomentoErrorCode.CANCELLED_ERROR,
                        message_wrapper="Error reading item from topic subscription",
                    )
                    self._logger.debug(f"Client stream read has been cancelled: {type(e)}")
                    return TopicSubscriptionItem.Error(err)
                except Exception as e:
                    self._logger.debug(f"Error reading from client stream: {type(e)}")
                    # TODO: attempt reconnect
                    continue

    class Subscription(SubscriptionBase):
        """Provides the synchronous version of a topic subscription."""

        def __init__(self, cache_name: str, topic_name: str, client_stream: _MultiThreadedRendezvous):
            self._cache_name = cache_name
            self._topic_name = topic_name
            self._client_stream = client_stream  # type: ignore[misc]

        def __iter__(self) -> TopicSubscribe.Subscription:
            return self

        def __next__(self) -> TopicSubscriptionItemResponse:
            """Retrieves the next published item from the subscription.

            The item method returns a response object that is resolved to a type-safe object
            of one of the following:

            - TopicSubscriptionItem.Success
            - TopicSubscriptionItem.Error

            Pattern matching can be used to operate on the appropriate subtype.
            For example, in python 3.10+ if you're receiving a subscription item::

                for response in subscription:
                    match response:
                        case TopicSubscriptionItem.Success():
                            return response.value_string # value_bytes is also available
                        case TopicSubscriptionItem.Error():
                            ...there was an error retrieving the item...

            or equivalently in earlier versions of python::

                for response in subscription:
                    if isinstance(response, TopicSubscriptionItem.Success):
                        return response.value_string # value_bytes is also available
                    elif isinstance(response, TopicSubscriptionItem.Error):
                        ...there was an error retrieving the item...
            """
            while True:
                try:
                    result: cachepubsub_pb2._SubscriptionItem = self._client_stream.next()  # type: ignore[misc]
                    item = self._process_result(result)
                    # if item is null, we've received a heartbeat or discontinuity and should continue
                    if item is not None:
                        return item
                except Exception as e:
                    self._logger.debug("Error reading from client stream: %s", e)
                    # TODO: attempt reconnect
                    continue

    class Error(TopicSubscribeResponse, ErrorResponseMixin):
        """Contains information about an error returned from a request.

        This includes:
        - `error_code`: `MomentoErrorCode` value for the error.
        - `message`: a detailed error message.
        """

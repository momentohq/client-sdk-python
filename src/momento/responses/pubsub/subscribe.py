from abc import ABC
from typing import Any

from ..mixins import ErrorResponseMixin
from ..response import PubsubResponse

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
            self._cache_name = cache_name
            self._topic_name = topic_name
            self._client_stream = client_stream

        async def item(self):
            while True:
                # TODO: assuming that an exception means we need to reconnect
                try:
                    result = await self._client_stream.read()
                except Exception as e:
                    # TODO: attempt reconnect
                    continue
                msg_type = result.WhichOneof("kind")
                if msg_type == "item":
                    # TODO: handle text vs binary
                    value_type = result.item.value.WhichOneof("kind")
                    # print(f"value type is {value_type}")
                    value = result.item.value
                    # print(f"got value {value} with dir {dir(value)}")
                    # print("returning ", value.bytes)
                    # TODO: does the return type need to expose methods to retrieve both text and bytes here?
                    #  i.e., should this be wrapped in a type with value_string and value_bytes methods?
                    if value_type == "text":
                        return value.text
                    elif value_type == "bytes":
                        return value.bytes
                elif msg_type == "heartbeat":
                    # TODO: add logging for heartbeat
                    continue
                elif type == "discontinuity":
                    # TODO: add logging for discontinuity
                    continue
                else:
                    # TODO: add logging for unknown response
                    continue

    class Error(TopicSubscribeResponse, ErrorResponseMixin):
        """Contains information about an error returned from a request.

        This includes:
        - `error_code`: `MomentoErrorCode` value for the error.
        - `message`: a detailed error message.
        """

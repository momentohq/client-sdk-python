from abc import ABC
from dataclasses import dataclass

from ..mixins import ErrorResponseMixin, ValueStringMixin
from ..response import PubsubResponse


class TopicSubscriptionItemResponse(PubsubResponse):
    """Parent response type for a topic subscription's `item` request.

    Its subtypes are:
    - `TopicSubscriptionItem.Success`
    - `TopicSubscriptionItem.Error`
    """


class TopicSubscriptionItem(ABC):
    """Groups all `TopicSubscriptionItemResponse` derived types under a common namespace."""

    @dataclass
    class Success(TopicSubscriptionItemResponse, ValueStringMixin):
        """Indicates the request was successful."""

        value_bytes: bytes
        """The item returned from the subscription for the specified topic. Use the
        `value_string` property to access the value as a string."""

    class Error(TopicSubscriptionItemResponse, ErrorResponseMixin):
        """Contains information about an error returned from a request.

        This includes:
        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

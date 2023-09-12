from abc import ABC
from dataclasses import dataclass
from warnings import warn

from ..mixins import ErrorResponseMixin, ValueStringMixin
from ..response import PubsubResponse


class TopicSubscriptionItemResponse(PubsubResponse):
    """Parent response type for a topic subscription's `item` request.

    Its subtypes are:
    - `TopicSubscriptionItem.Text`
    - `TopicSubscriptionItem.Binary`
    - `TopicSubscriptionItem.Error`

    The subtype `TopicSubscriptionItem.Success` is deprecated.
    """


class TopicSubscriptionItem(ABC):
    """Groups all `TopicSubscriptionItemResponse` derived types under a common namespace."""

    @dataclass
    class Success(TopicSubscriptionItemResponse, ValueStringMixin):
        """Indicates the request was successful."""

        value_bytes: bytes
        """The item returned from the subscription for the specified topic. Use the
        `value_string` property to access the value as a string."""

        def __post_init__(self) -> None:
            warn("Success is a deprecated response, use Text or Binary instead", DeprecationWarning, stacklevel=2)

    @dataclass
    class Text(TopicSubscriptionItemResponse):
        """Indicates the request was successful and value will be returned as a string."""

        value: str

    @dataclass
    class Binary(TopicSubscriptionItemResponse):
        """Indicates the request was successful and value will be returned as bytes."""

        value: bytes

    class Error(TopicSubscriptionItemResponse, ErrorResponseMixin):
        """Contains information about an error returned from a request.

        This includes:
        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

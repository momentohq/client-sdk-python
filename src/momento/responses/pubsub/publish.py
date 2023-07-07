from abc import ABC

from ..mixins import ErrorResponseMixin
from ..response import PubsubResponse


class TopicPublishResponse(PubsubResponse):
    """Parent response type for a topic `publish` request.

    Its subtypes are:
    - `TopicPublish.Success`
    - `TopicPublish.Error`
    """


class TopicPublish(ABC):
    """Groups all `TopicPublishResponse` derived types under a common namespace."""

    class Success(TopicPublishResponse):
        """Indicates the request was successful."""

    class Error(TopicPublishResponse, ErrorResponseMixin):
        """Contains information about an error returned from a request.

        This includes:
        - `error_code`: `MomentoErrorCode` value for the error.
        - `message`: a detailed error message.
        """

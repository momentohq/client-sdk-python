from __future__ import annotations

from abc import ABC
from dataclasses import dataclass

from ...mixins import ErrorResponseMixin
from ..response import VectorIndexResponse


class CountItemsResponse(VectorIndexResponse):
    """Parent response type for a `count_items` request.

    Its subtypes are:
    - `CountItems.Success`
    - `CountItems.Error`

    See `PreviewVectorIndexClient` for how to work with responses.
    """


class CountItems(ABC):
    """Groups all `CountItemsResponse` derived types under a common namespace."""

    @dataclass
    class Success(CountItemsResponse):
        """Contains the result of a `count_items` request."""

        item_count: int
        """The number of items in the index."""

    class Error(CountItemsResponse, ErrorResponseMixin):
        """Contains information about an error returned from a request.

        This includes:
        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

from __future__ import annotations

from abc import ABC
from dataclasses import dataclass

from momento_wire_types import controlclient_pb2 as ctrl_pb

from momento.errors.exceptions import UnknownException
from momento.requests.vector_index import SimilarityMetric

from ...mixins import ErrorResponseMixin
from ...response import ControlResponse


class ListIndexesResponse(ControlResponse):
    """Parent response type for a list indexes request.

    The response object is resolved to a type-safe object of one of
    the following subtypes:
    - `ListIndexes.Success`
    - `ListIndexes.Error`

    Pattern matching can be used to operate on the appropriate subtype.
    For example, in python 3.10+::

        match response:
            case ListIndexes.Success():
                ...
            case ListIndexes.Error():
                ...
            case _:
                # Shouldn't happen

    or equivalently in earlier versions of python::

        if isinstance(response, ListIndexes.Success):
            ...
        elif isinstance(response, ListIndexes.Error):
            ...
        else:
            # Shouldn't happen
    """


@dataclass
class IndexInfo:
    """Contains a Momento index's info."""

    name: str
    num_dimensions: int
    similarity_metric: SimilarityMetric

    @staticmethod
    def from_grpc_response(grpc_index_info: ctrl_pb._ListIndexesResponse._Index) -> "IndexInfo":
        metric_type: str = grpc_index_info.similarity_metric.WhichOneof("similarity_metric")
        similarity_metric: SimilarityMetric

        if metric_type == "cosine_similarity":
            similarity_metric = SimilarityMetric.COSINE_SIMILARITY
        elif metric_type == "euclidean_similarity":
            similarity_metric = SimilarityMetric.EUCLIDEAN_SIMILARITY
        elif metric_type == "inner_product":
            similarity_metric = SimilarityMetric.INNER_PRODUCT
        else:
            raise UnknownException(f"Unknown similarity metric: {metric_type}")

        return IndexInfo(
            name=grpc_index_info.index_name,
            num_dimensions=grpc_index_info.num_dimensions,
            similarity_metric=similarity_metric,
        )


class ListIndexes(ABC):
    """Groups all `ListIndexesResponse` derived types under a common namespace."""

    @dataclass
    class Success(ListIndexesResponse):
        """Indicates the request was successful."""

        indexes: list[IndexInfo]
        """The list of indexes available to the user."""

        @staticmethod
        def from_grpc_response(grpc_list_index_response: ctrl_pb._ListIndexesResponse) -> ListIndexes.Success:
            """Initializes ListIndexResponse to handle list index response.

            Args:
                grpc_list_index_response: Protobuf based response returned by Scs.
            """
            return ListIndexes.Success(
                indexes=[IndexInfo.from_grpc_response(index) for index in grpc_list_index_response.indexes]  # type: ignore[misc]  # noqa: E501
            )

    class Error(ListIndexesResponse, ErrorResponseMixin):
        """Contains information about an error returned from a request.

        This includes:
        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """

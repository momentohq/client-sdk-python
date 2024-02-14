from __future__ import annotations

from types import TracebackType
from typing import Optional, Type

from momento import logs
from momento.auth import CredentialProvider
from momento.config import VectorIndexConfiguration

try:
    from momento.internal._utilities import _validate_request_timeout
    from momento.internal.synchronous._vector_index_control_client import (
        _VectorIndexControlClient,
    )
    from momento.internal.synchronous._vector_index_data_client import _VectorIndexDataClient
except ImportError as e:
    if e.name == "cygrpc":
        import sys

        print(
            "There is an issue on M1 macs between GRPC native packaging and Python wheel tags. "
            "See https://github.com/grpc/grpc/issues/28387",
            file=sys.stderr,
        )
        print("-".join("" for _ in range(99)), file=sys.stderr)
        print("    TO WORK AROUND:", file=sys.stderr)
        print("    * Install Rosetta 2", file=sys.stderr)
        print(
            "    * Install Python from python.org (you might need to do this if you're using an arm-only build)",
            file=sys.stderr,
        )
        print("    * re-run with:", file=sys.stderr)
        print("arch -x86_64 {} {}".format(sys.executable, *sys.argv), file=sys.stderr)
        print("-".join("" for _ in range(99)), file=sys.stderr)
    raise e

from momento.requests.vector_index import AllMetadata, FilterExpression, Item, SimilarityMetric
from momento.responses.vector_index import (
    CountItemsResponse,
    CreateIndexResponse,
    DeleteIndexResponse,
    DeleteItemBatchResponse,
    GetItemBatchResponse,
    GetItemMetadataBatchResponse,
    ListIndexesResponse,
    SearchAndFetchVectorsResponse,
    SearchResponse,
    UpsertItemBatchResponse,
)


class PreviewVectorIndexClient:
    """Synchronous Vector Index Client.

    Vector and control methods return a response object unique to each request.
    The response object is resolved to a type-safe object of one of several
    sub-types. See the documentation for each response type for details.

    Pattern matching can be used to operate on the appropriate subtype.
    For example, in python 3.10+ if you're deleting a key::

        response = client.create_index(index_name, num_dimensions)
        match response:
            case CreateIndex.Success():
                ...the index was created...
            case CreateIndex.IndexAlreadyExists():
                ... the index already exists...
            case CreateIndex.Error():
                ...there was an error trying to delete the key...

    or equivalently in earlier versions of python::

        response = client.create_index(index_name, num_dimensions)
        if isinstance(response, CreateIndex.Success):
            ...
        case isinstance(response, CreateIndex.IndexAlreadyExists):
            ... the index already exists...
        elif isinstance(response, CreateIndex.Error):
            ...
        else:
            raise Exception("This should never happen")
    """

    def __init__(self, configuration: VectorIndexConfiguration, credential_provider: CredentialProvider):
        """Instantiate a client.

        Args:
            configuration (VectorIndexConfiguration): An object holding configuration settings for communication
                with the server.
            credential_provider (CredentialProvider): An object holding the auth token and endpoint information.

        Raises:
            IllegalArgumentException: If method arguments fail validations.
        Example::

            from momento import CredentialProvider, PreviewVectorIndexClient, VectorIndexConfigurations

            configuration = VectorIndexConfigurations.Laptop.latest()
            credential_provider = CredentialProvider.from_environment_variable("MOMENTO_API_KEY")
            client = PreviewVectorIndexClient(configuration, credential_provider)
        """
        _validate_request_timeout(configuration.get_transport_strategy().get_grpc_configuration().get_deadline())
        self._logger = logs.logger
        self._next_client_index = 0
        self._control_client = _VectorIndexControlClient(configuration, credential_provider)
        self._data_client = _VectorIndexDataClient(configuration, credential_provider)

    def __enter__(self) -> PreviewVectorIndexClient:
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        self._control_client.close()
        self._data_client.close()

    def count_items(self, index_name: str) -> CountItemsResponse:
        """Gets the number of items in a vector index.

        Note that if the vector index does not exist, a `NOT_FOUND` error will be returned.

        Args:
            index_name (str): Name of the index to count the items in.

        Returns:
            CountItemsResponse: The result of a count items operation.
        """
        return self._data_client.count_items(index_name)

    def create_index(
        self,
        index_name: str,
        num_dimensions: int,
        similarity_metric: SimilarityMetric = SimilarityMetric.COSINE_SIMILARITY,
    ) -> CreateIndexResponse:
        """Creates a vector index if it doesn't exist.

        Remark on the choice of similarity metric:
        - Cosine similarity is appropriate for most embedding models as they tend to be optimized
            for this metric.
        - If the vectors are unit normalized, cosine similarity is equivalent to inner product.
            If your vectors are already unit normalized, you can use inner product to improve
            performance.
        - Euclidean similarity, the sum of squared differences, is appropriate for datasets where
            this metric is meaningful. For example, if the vectors represent images, and the
            embedding model is trained to optimize the euclidean distance between images, then
            euclidean similarity is appropriate.

        Args:
            index_name (str): Name of the index to be created.
            num_dimensions (int): Number of dimensions of the vectors to be indexed.
            similarity_metric (SimilarityMetric): The similarity metric to use when comparing
                vectors in the index. Defaults to SimilarityMetric.COSINE_SIMILARITY.

        Returns:
            CreateIndexResponse: The result of a create index operation.
        """
        return self._control_client.create_index(index_name, num_dimensions, similarity_metric)

    def delete_index(self, index_name: str) -> DeleteIndexResponse:
        """Deletes a vector index and all of the items within it.

        Args:
            index_name (str): Name of the index to be deleted.

        Returns:
            DeleteIndexResponse: The result of a delete index operation.
        """
        return self._control_client.delete_index(index_name)

    def list_indexes(self) -> ListIndexesResponse:
        """Lists all vector indexes.

        Returns:
            ListIndexesResponse: The result of a list indexes operation.
        """
        return self._control_client.list_indexes()

    def upsert_item_batch(self, index_name: str, items: list[Item]) -> UpsertItemBatchResponse:
        """Upserts a batch of items into a vector index.

        If an item with the same ID already exists in the index, it will be replaced.
        Otherwise, it will be added to the index.

        Args:
            index_name (str): Name of the index to add the items into.
            items (list[Item]): The items to be added into the index.

        Returns:
            AddItemBatchResponse: The result of an add item batch operation.
        """
        return self._data_client.upsert_item_batch(index_name, items)

    def delete_item_batch(self, index_name: str, filter: FilterExpression | list[str]) -> DeleteItemBatchResponse:
        """Deletes a batch of items from a vector index.

        Deletes any and all items with the given IDs from the index.

        Args:
            index_name (str): Name of the index to delete the items from.
            filter (FilterExpression | list[str]): A filter expression to match
                items to be deleted, or list of item IDs to be deleted.

        Returns:
            DeleteItemBatchResponse: The result of a delete item batch operation.
        """
        return self._data_client.delete_item_batch(index_name, filter)

    def search(
        self,
        index_name: str,
        query_vector: list[float],
        top_k: int = 10,
        metadata_fields: Optional[list[str]] | AllMetadata = None,
        score_threshold: Optional[float] = None,
        filter: Optional[FilterExpression] = None,
    ) -> SearchResponse:
        """Searches for the most similar vectors to the query vector in the index.

        Ranks the results using the similarity metric specified when the index was created.

        Args:
            index_name (str): Name of the index to search in.
            query_vector (list[float]): The vector to search for.
            top_k (int): The number of results to return. Defaults to 10.
            metadata_fields (Optional[list[str]] | AllMetadata): A list of metadata fields
                to return with each result. If not provided, no metadata is returned.
                If the special value `ALL_METADATA` is provided, all metadata is returned.
                Defaults to None.
            score_threshold (Optional[float]): A score threshold to filter results by.
                For cosine similarity and inner product, scores lower than the threshold
                are excluded. For euclidean similarity, scores higher than the threshold
                are excluded. The threshold is exclusive. Defaults to None, ie no threshold.
            filter (Optional[FilterExpression]): A filter expression to filter
                results by. Defaults to None, ie no filter.

        Returns:
            SearchResponse: The result of a search operation.
        """
        return self._data_client.search(index_name, query_vector, top_k, metadata_fields, score_threshold, filter)

    def search_and_fetch_vectors(
        self,
        index_name: str,
        query_vector: list[float],
        top_k: int = 10,
        metadata_fields: Optional[list[str]] | AllMetadata = None,
        score_threshold: Optional[float] = None,
        filter: Optional[FilterExpression] = None,
    ) -> SearchAndFetchVectorsResponse:
        """Searches for the most similar vectors to the query vector in the index.

        Ranks the results using the similarity metric specified when the index was created.
        Also returns the vectors associated with each result.

        Args:
            index_name (str): Name of the index to search in.
            query_vector (list[float]): The vector to search for.
            top_k (int): The number of results to return. Defaults to 10.
            metadata_fields (Optional[list[str]] | AllMetadata): A list of metadata fields
                to return with each result. If not provided, no metadata is returned.
                If the special value `ALL_METADATA` is provided, all metadata is returned.
                Defaults to None.
            score_threshold (Optional[float]): A score threshold to filter results by.
                For cosine similarity and inner product, scores lower than the threshold
                are excluded. For euclidean similarity, scores higher than the threshold
                are excluded. The threshold is exclusive. Defaults to None, ie no threshold.
            filter (Optional[FilterExpression]): A filter expression to filter
                results by. Defaults to None, ie no filter.

        Returns:
            SearchResponse: The result of a search operation.
        """
        return self._data_client.search_and_fetch_vectors(
            index_name, query_vector, top_k, metadata_fields, score_threshold, filter
        )

    def get_item_batch(self, index_name: str, filter: list[str]) -> GetItemBatchResponse:
        """Gets a batch of items from a vector index by ID.

        Args:
            index_name (str): Name of the index to get the item from.
            filter (list[str]): The IDs of the items to be retrieved from the index.

        Returns:
            GetItemBatchResponse: The result of a get item batch operation.
        """
        return self._data_client.get_item_batch(index_name, filter)

    def get_item_metadata_batch(self, index_name: str, filter: list[str]) -> GetItemMetadataBatchResponse:
        """Gets metadata for a batch of items from a vector index by ID.

        Args:
            index_name (str): Name of the index to get the items from.
            filter (list[str]): The IDs of the item metadata to be retrieved from the index.

        Returns:
            GetItemMetadataBatchResponse: The result of a get item metadata batch operation.
        """
        return self._data_client.get_item_metadata_batch(index_name, filter)

    # TODO: repr

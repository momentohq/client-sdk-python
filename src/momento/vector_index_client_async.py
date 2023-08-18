from __future__ import annotations

from types import TracebackType
from typing import Optional, Type

from momento import logs
from momento.auth import CredentialProvider
from momento.config import VectorIndexConfiguration

try:
    from momento.internal._utilities import _validate_request_timeout
    from momento.internal.aio._vector_index_control_client import (
        _VectorIndexControlClient,
    )
    from momento.internal.aio._vector_index_data_client import _VectorIndexDataClient
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

from momento.requests.vector_index import Item
from momento.responses.vector_index import (
    AddItemBatchResponse,
    CreateIndexResponse,
    DeleteIndexResponse,
    DeleteItemBatchResponse,
    ListIndexesResponse,
    SearchResponse,
)


class PreviewVectorIndexClientAsync:
    """Async Vector Index Client.

    Vector and control methods return a response object unique to each request.
    The response object is resolved to a type-safe object of one of several
    sub-types. See the documentation for each response type for details.

    Pattern matching can be used to operate on the appropriate subtype.
    For example, in python 3.10+ if you're deleting a key::

        response = await client.create_index(index_name, num_dimensions)
        match response:
            case CreateIndex.Success():
                ...the index was created...
            case CreateIndex.IndexAlreadyExists():
                ... the index already exists...
            case CreateIndex.Error():
                ...there was an error trying to delete the key...

    or equivalently in earlier versions of python::

        response = await client.create_index(index_name, num_dimensions)
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

            from momento import CredentialProvider, PreviewVectorIndexClientAsync, VectorIndexConfigurations

            configuration = VectorIndexConfigurations.Laptop.latest()
            credential_provider = CredentialProvider.from_environment_variable("MOMENTO_AUTH_TOKEN")
            client = PreviewVectorIndexClientAsync(configuration, credential_provider)
        """
        _validate_request_timeout(configuration.get_transport_strategy().get_grpc_configuration().get_deadline())
        self._logger = logs.logger
        self._next_client_index = 0
        self._control_client = _VectorIndexControlClient(configuration, credential_provider)
        self._data_client = _VectorIndexDataClient(configuration, credential_provider)

    async def __aenter__(self) -> PreviewVectorIndexClientAsync:
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        await self._control_client.close()
        await self._data_client.close()

    async def create_index(self, index_name: str, num_dimensions: int) -> CreateIndexResponse:
        """Creates a vector index if it doesn't exist.

        Args:
            index_name (str): Name of the index to be created.
            num_dimensions (int): Number of dimensions of the vectors to be indexed.

        Returns:
            CreateIndexResponse: The result of a create index operation.
        """
        return await self._control_client.create_index(index_name, num_dimensions)

    async def delete_index(self, index_name: str) -> DeleteIndexResponse:
        """Deletes a vector index and all of the items within it.

        Args:
            index_name (str): Name of the index to be deleted.

        Returns:
            DeleteIndexResponse: The result of a delete index operation.
        """
        return await self._control_client.delete_index(index_name)

    async def list_indexes(self) -> ListIndexesResponse:
        """Lists all vector indexes.

        Returns:
            ListIndexesResponse: The result of a list indexes operation.
        """
        return await self._control_client.list_indexes()

    async def add_item_batch(self, index_name: str, items: list[Item]) -> AddItemBatchResponse:
        """Adds a batch of items into a vector index.

        Adds an item into the index regardless if the ID already exists.
        On duplicate ID, a separate entry is created with the same ID.

        To deduplicate, first call `delete_item_batch` to remove all items
        with the same ID, then call `add_item_batch` to add the new items.

        Args:
            index_name (str): Name of the index to add the items into.
            items (list[Item]): The items to be added into the index.

        Returns:
            AddItemBatchResponse: The result of an add item batch operation.
        """
        return await self._data_client.add_item_batch(index_name, items)

    async def delete_item_batch(self, index_name: str, ids: list[str]) -> DeleteItemBatchResponse:
        """Deletes a batch of items from a vector index.

        Deletes any and all items with the given IDs from the index.

        Args:
            index_name (str): Name of the index to delete the items from.
            ids (list[str]): The IDs of the items to be deleted from the index.

        Returns:
            DeleteItemBatchResponse: The result of a delete item batch operation.
        """
        return await self._data_client.delete_item_batch(index_name, ids)

    async def search(
        self, index_name: str, query_vector: list[float], top_k: int = 10, metadata_fields: Optional[list[str]] = None
    ) -> SearchResponse:
        """Searches for the most similar vectors to the query vector in the index.

        Ranks the vectors in the index by maximum inner product to the query vector.

        If the index and query vectors are unit normalized, this is equivalent to
        ranking by cosine similarity. Hence to perform a cosine similarity search,
        the index vectors should be unit normalized prior to indexing, and the query
        vector should be unit normalized prior to searching.

        Args:
            index_name (str): Name of the index to search in.
            query_vector (list[float]): The vector to search for.
            top_k (int): The number of results to return. Defaults to 10.
            metadata_fields (Optional[list[str]]): A list of metadata fields to return with each result.
                If not provided, no metadata is returned. Defaults to None.

        Returns:
            SearchResponse: The result of a search operation.
        """
        return await self._data_client.search(index_name, query_vector, top_k, metadata_fields)

    # TODO: repr

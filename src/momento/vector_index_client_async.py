from __future__ import annotations

from types import TracebackType
from typing import Optional, Type

from momento import logs
from momento.auth import CredentialProvider
from momento.config import Configuration

try:
    from momento.internal._utilities import _validate_request_timeout
    from momento.internal.aio._scs_control_client import _ScsControlClient
    from momento.internal.aio._vector_index_client import _VectorIndexClient
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

from momento.requests import Item
from momento.responses import (
    CreateIndexResponse,
    DeleteIndexResponse,
    ListIndexesResponse,
    VectorIndexUpsertItemBatchResponse,
)


class VectorIndexClientAsync:
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

    def __init__(self, configuration: Configuration, credential_provider: CredentialProvider):
        """Instantiate a client.

        Args:
            configuration (Configuration): An object holding configuration settings for communication with the server.
            credential_provider (CredentialProvider): An object holding the auth token and endpoint information.

        Raises:
            IllegalArgumentException: If method arguments fail validations.
        Example::

            from datetime import timedelta
            from momento import CredentialProvider, VectorIndexClientAsync, VectorIndexConfigurations

            configuration = VectorIndexConfigurations.Laptop.latest()
            credential_provider = CredentialProvider.from_environment_variable("MOMENTO_AUTH_TOKEN")
            client = VectorIndexClientAsync(configuration, credential_provider)
        """
        _validate_request_timeout(configuration.get_transport_strategy().get_grpc_configuration().get_deadline())
        self._logger = logs.logger
        self._next_client_index = 0
        self._control_client = _ScsControlClient(configuration, credential_provider)
        self._data_client = _VectorIndexClient(configuration, credential_provider)

    async def __aenter__(self) -> VectorIndexClientAsync:
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

    async def upsert_item_batch(self, index_name: str, items: list[Item]) -> VectorIndexUpsertItemBatchResponse:
        """Upserts a batch of items into a vector index.

        Inserts an item if the ID does not exist. If the ID does exist, the item is replaced
        with the new item.

        Args:
            index_name (str): Name of the index to upsert the items into.
            items (list[Item]): The items to be upserted into the index.

        Returns:
            UpsertItemBatchResponse: The result of an upsert item batch operation.
        """
        return await self._data_client.upsert_item_batch(index_name, items)

    # TODO: repr

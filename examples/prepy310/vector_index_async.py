import asyncio
import logging
from time import sleep

from momento import (
    CredentialProvider,
    PreviewVectorIndexClientAsync,
    VectorIndexConfigurations,
)
from momento.config import VectorIndexConfiguration
from momento.requests.vector_index import ALL_METADATA, Item, SimilarityMetric
from momento.responses.vector_index import (
    CreateIndex,
    DeleteIndex,
    DeleteItemBatch,
    ListIndexes,
    Search,
    UpsertItemBatch,
)

from example_utils.example_logging import initialize_logging

_logger = logging.getLogger("vector-example")

VECTOR_INDEX_CONFIGURATION: VectorIndexConfiguration = VectorIndexConfigurations.Default.latest()
VECTOR_AUTH_PROVIDER = CredentialProvider.from_environment_variable("MOMENTO_API_KEY")


def _print_start_banner() -> None:
    _logger.info("******************************************************************")
    _logger.info("*                      Momento Example Start                     *")
    _logger.info("******************************************************************\n")


def _print_end_banner() -> None:
    _logger.info("******************************************************************")
    _logger.info("*                       Momento Example End                      *")
    _logger.info("******************************************************************\n")


async def create_index(
    client: PreviewVectorIndexClientAsync,
    index_name: str,
    num_dimensions: int,
    similarity_metric: SimilarityMetric = SimilarityMetric.COSINE_SIMILARITY,
) -> None:
    _logger.info(f"Creating index with name {index_name!r}")
    create_index_response = await client.create_index(index_name, num_dimensions, similarity_metric)
    if isinstance(create_index_response, CreateIndex.Success):
        _logger.info(f"Index with name {index_name!r}  successfully created!")
    elif isinstance(create_index_response, CreateIndex.IndexAlreadyExists):
        _logger.info(f"Index with name {index_name!r} already exists")
    elif isinstance(create_index_response, CreateIndex.Error):
        _logger.error(f"Error while creating index {create_index_response.message}")
    else:
        _logger.error("Unreachable")
    _logger.info("")


async def list_indexes(client: PreviewVectorIndexClientAsync) -> None:
    _logger.info("Listing indexes:")
    list_indexes_response = await client.list_indexes()
    if isinstance(list_indexes_response, ListIndexes.Success):
        for index in list_indexes_response.indexes:
            _logger.info(f"- {index!r}")
    elif isinstance(list_indexes_response, ListIndexes.Error):
        _logger.error(f"Error while listing indexes {list_indexes_response.message}")
    else:
        _logger.error("Unreachable")
    _logger.info("")


async def upsert_items(client: PreviewVectorIndexClientAsync, index_name: str) -> None:
    items = [
        Item(id="test_item_1", vector=[1.0, 2.0], metadata={"key1": "value1"}),
        Item(id="test_item_2", vector=[3.0, 4.0], metadata={"key2": "value2"}),
        Item(id="test_item_3", vector=[5.0, 6.0], metadata={"key1": "value3", "key3": "value3"}),
    ]
    _logger.info(f"Adding items {items}")
    upsert_response = await client.upsert_item_batch(
        index_name,
        items=items,
    )
    if isinstance(upsert_response, UpsertItemBatch.Success):
        _logger.info("Successfully added items")
    elif isinstance(upsert_response, UpsertItemBatch.Error):
        _logger.error(f"Error while adding items to index {index_name!r}: {upsert_response.message}")
    else:
        _logger.error("Unreachable")
    _logger.info("")


async def search(client: PreviewVectorIndexClientAsync, index_name: str) -> None:
    query_vector = [1.0, 2.0]
    top_k = 3
    _logger.info(f"Searching index {index_name} with query_vector {query_vector} and top {top_k} elements")
    search_response = await client.search(
        index_name, query_vector=query_vector, top_k=top_k, metadata_fields=ALL_METADATA
    )
    if isinstance(search_response, Search.Success):
        _logger.info(f"Search succeeded with {len(search_response.hits)} matches:")
        _logger.info(search_response.hits)
    elif isinstance(search_response, Search.Error):
        _logger.error(f"Error while searching on index {index_name}: {search_response.message}")
    else:
        _logger.error("Unreachable")
    _logger.info("")


async def delete_items(client: PreviewVectorIndexClientAsync, index_name: str) -> None:
    item_ids_to_delete = ["test_item_1", "test_item_3"]
    _logger.info(f"Deleting items: {item_ids_to_delete}")
    delete_response = await client.delete_item_batch(index_name, ids=item_ids_to_delete)
    if isinstance(delete_response, DeleteItemBatch.Success):
        _logger.info("Successfully deleted items")
    elif isinstance(delete_response, DeleteItemBatch.Error):
        _logger.error(f"Error while deleting items {delete_response.message}")
    else:
        _logger.error("Unreachable")
    _logger.info("")


async def delete_index(client: PreviewVectorIndexClientAsync, index_name: str) -> None:
    _logger.info("Deleting index " + index_name)
    delete_response = await client.delete_index(index_name)

    if isinstance(delete_response, DeleteIndex.Success):
        _logger.info(f"Index {index_name} deleted successfully!")
    elif isinstance(delete_response, DeleteIndex.Error):
        _logger.error(f"Failed to delete index {index_name} with error {delete_response.message}")
    else:
        _logger.error("Unreachable")
    _logger.info("")


async def main() -> None:
    initialize_logging()
    _print_start_banner()
    async with PreviewVectorIndexClientAsync(VECTOR_INDEX_CONFIGURATION, VECTOR_AUTH_PROVIDER) as client:
        index_name = "hello_momento_index"

        await create_index(client, index_name, num_dimensions=2)
        await list_indexes(client)
        await upsert_items(client, index_name)
        sleep(2)
        await search(client, index_name)
        await delete_items(client, index_name)
        sleep(2)
        _logger.info("Deleted two items; search will return 1 hit now")
        await search(client, index_name)
        await delete_index(client, index_name)
    _print_end_banner()


if __name__ == "__main__":
    asyncio.run(main())

import logging
from time import sleep

from momento import (
    CredentialProvider,
    PreviewVectorIndexClient,
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


def create_index(
    client: PreviewVectorIndexClient,
    index_name: str,
    num_dimensions: int,
    similarity_metric: SimilarityMetric = SimilarityMetric.COSINE_SIMILARITY,
) -> None:
    _logger.info(f"Creating index with name {index_name!r}")
    create_index_response = client.create_index(index_name, num_dimensions, similarity_metric)
    match create_index_response:
        case CreateIndex.Success():
            _logger.info(f"Index with name {index_name!r}  successfully created!")
        case CreateIndex.IndexAlreadyExists():
            _logger.info(f"Index with name {index_name!r} already exists")
        case CreateIndex.Error() as create_index_error:
            _logger.error(f"Error while creating index {create_index_error.message}")
        case _:
            _logger.error("Unreachable")
    _logger.info("")


def list_indexes(client: PreviewVectorIndexClient) -> None:
    _logger.info("Listing indexes:")
    list_indexes_response = client.list_indexes()
    match list_indexes_response:
        case ListIndexes.Success() as success:
            for index in success.indexes:
                _logger.info(f"- {index!r}")
        case ListIndexes.Error() as list_indexes_error:
            _logger.error(f"Error while listing indexes {list_indexes_error.message}")
        case _:
            _logger.error("Unreachable")
    _logger.info("")


def upsert_items(client: PreviewVectorIndexClient, index_name: str) -> None:
    items = [
        Item(id="test_item_1", vector=[1.0, 2.0], metadata={"key1": "value1"}),
        Item(id="test_item_2", vector=[3.0, 4.0], metadata={"key2": "value2"}),
        Item(id="test_item_3", vector=[5.0, 6.0], metadata={"key1": "value3", "key3": "value3"}),
    ]
    _logger.info(f"Adding items {items}")
    upsert_response = client.upsert_item_batch(
        index_name,
        items=items,
    )
    match upsert_response:
        case UpsertItemBatch.Success():
            _logger.info("Successfully added items")
        case UpsertItemBatch.Error() as upsert_error:
            _logger.error(f"Error while adding items to index {index_name!r}: {upsert_error.message}")
        case _:
            _logger.error("Unreachable")
    _logger.info("")


def search(client: PreviewVectorIndexClient, index_name: str) -> None:
    query_vector = [1.0, 2.0]
    top_k = 3
    _logger.info(f"Searching index {index_name} with query_vector {query_vector} and top {top_k} elements")
    search_response = client.search(index_name, query_vector=query_vector, top_k=top_k, metadata_fields=ALL_METADATA)
    match search_response:
        case Search.Success() as success:
            _logger.info(f"Search succeeded with {len(success.hits)} matches:")
            _logger.info(success.hits)
        case Search.Error() as search_error:
            _logger.error(f"Error while searching on index {index_name}: {search_error.message}")
        case _:
            _logger.error("Unreachable")
    _logger.info("")


def delete_items(client: PreviewVectorIndexClient, index_name: str) -> None:
    item_ids_to_delete = ["test_item_1", "test_item_3"]
    _logger.info(f"Deleting items: {item_ids_to_delete}")
    delete_response = client.delete_item_batch(index_name, filter=item_ids_to_delete)
    match delete_response:
        case DeleteItemBatch.Success():
            _logger.info("Successfully deleted items")
        case DeleteItemBatch.Error() as delete_error:
            _logger.error(f"Error while deleting items {delete_error.message}")
        case _:
            _logger.error("Unreachable")
    _logger.info("")


def delete_index(client: PreviewVectorIndexClient, index_name: str) -> None:
    _logger.info("Deleting index " + index_name)
    delete_response = client.delete_index(index_name)

    match delete_response:
        case DeleteIndex.Success():
            _logger.info(f"Index {index_name} deleted successfully!")
        case DeleteIndex.Error() as delete_error:
            _logger.error(f"Failed to delete index {index_name} with error {delete_error.message}")
        case _:
            _logger.error("Unreachable")
    _logger.info("")


if __name__ == "__main__":
    initialize_logging()
    _print_start_banner()
    with PreviewVectorIndexClient(VECTOR_INDEX_CONFIGURATION, VECTOR_AUTH_PROVIDER) as client:
        index_name = "hello_momento_index"

        create_index(client, index_name, num_dimensions=2)
        list_indexes(client)
        upsert_items(client, index_name)
        sleep(2)
        search(client, index_name)
        delete_items(client, index_name)
        sleep(2)
        _logger.info("Deleted two items; search will return 1 hit now")
        search(client, index_name)
        delete_index(client, index_name)
    _print_end_banner()

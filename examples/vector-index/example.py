from time import sleep

from momento import (
    CredentialProvider,
    PreviewVectorIndexClient,
    VectorIndexConfigurations,
)
from momento.config import VectorIndexConfiguration
from momento.requests.vector_index import Item
from momento.responses.vector_index import (
    CreateIndex,
    DeleteIndex,
    DeleteItemBatch,
    ListIndexes,
    Search,
    UpsertItemBatch,
)

VECTOR_INDEX_CONFIGURATION: VectorIndexConfiguration = VectorIndexConfigurations.Default.latest()
VECTOR_AUTH_PROVIDER = CredentialProvider.from_environment_variable("MOMENTO_AUTH_TOKEN")


def _print_start_banner() -> None:
    print("******************************************************************")
    print("*                      Momento Example Start                     *")
    print("******************************************************************\n")


def create_index(index_name: str) -> None:
    print("Creating index with name " + index_name)
    create_index_response = _client.create_index(index_name, num_dimensions=2)
    if isinstance(create_index_response, CreateIndex.Success):
        print("Index with name " + index_name + " successfully created!")
    elif isinstance(create_index_response, CreateIndex.IndexAlreadyExists):
        print("Index with name " + index_name + " already exists")
    elif isinstance(create_index_response, CreateIndex.Error):
        raise (Exception("Error while creating index " + create_index_response.message))
    print("******************************************************************\n")


def list_indexes():
    print("Listing indexes")
    list_indexes_response = _client.list_indexes()
    if isinstance(list_indexes_response, ListIndexes.Success):
        for index in list_indexes_response.index_names:
            print("Account has an index with name " + index)
    elif isinstance(list_indexes_response, ListIndexes.Error):
        print(Exception("Error while listing indexes " + list_indexes_response.message))
    print("******************************************************************\n")


def upsert_items(index_name):
    items = [
        Item(id="test_item_1", vector=[1.0, 2.0], metadata={"key1": "value1"}),
        Item(id="test_item_2", vector=[3.0, 4.0], metadata={"key2": "value2"}),
        Item(id="test_item_3", vector=[5.0, 6.0], metadata={"key1": "value3", "key3": "value3"}),
    ]
    print("Adding items " + str(items))
    add_response = _client.upsert_item_batch(
        index_name,
        items=items,
    )
    if isinstance(add_response, UpsertItemBatch.Success):
        print("Successfully added items")
    elif isinstance(add_response, UpsertItemBatch.Error):
        raise (Exception("Error while adding items to index " + index_name + " " + add_response.message))
    print("******************************************************************\n")


def search(index_name):
    query_vector = [1.0, 2.0]
    top_k = 3
    print(
        "Searching index "
        + index_name
        + " with query_vector "
        + str(query_vector)
        + " and top "
        + str(top_k)
        + " elements"
    )
    search_response = _client.search(index_name, query_vector=query_vector, top_k=top_k)
    if isinstance(search_response, Search.Success):
        print("Search succeeded with " + str(len(search_response.hits)) + " matches")
    elif isinstance(search_response, Search.Error):
        raise (Exception("Error while searching on index " + index_name + " " + search_response.message))
    print("******************************************************************\n")


def delete_items(index_name):
    delete_response = _client.delete_item_batch(index_name, ids=["test_item_1", "test_item_3"])
    if isinstance(delete_response, DeleteItemBatch.Success):
        print("Successfully deleted items")
    elif isinstance(delete_response, DeleteItemBatch.Error):
        raise (Exception("Error while deleting items " + delete_response.message))


def delete_index(index_name):
    print("Deleting index " + index_name)
    del_response = _client.delete_index(index_name)

    if isinstance(del_response, DeleteIndex.Success):
        print("Index " + index_name + " deleted successfully!")
    elif isinstance(del_response, DeleteIndex.Error):
        raise (Exception("Failed to delete index " + index_name + " with error " + del_response.message))


if __name__ == "__main__":
    _print_start_banner()
    with PreviewVectorIndexClient(VECTOR_INDEX_CONFIGURATION, VECTOR_AUTH_PROVIDER) as _client:
        index_name = "hello_momento_index"

        create_index(index_name)
        list_indexes()
        upsert_items(index_name)
        sleep(2)
        search(index_name)
        delete_items(index_name)
        sleep(2)
        print("\nDeleted two items; search will return 1 hit now")
        search(index_name)
        delete_index(index_name)

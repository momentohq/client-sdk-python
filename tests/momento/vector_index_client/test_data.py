from momento import PreviewVectorIndexClient
from momento.errors import MomentoErrorCode
from momento.requests.vector_index import Item
from momento.responses.vector_index import (
    AddItemBatch,
    CreateIndex,
    DeleteItemBatch,
    Search,
    SearchHit,
)
from tests.conftest import TUniqueVectorIndexName
from tests.utils import sleep


def test_create_index_add_item_search_happy_path(
    vector_index_client: PreviewVectorIndexClient,
    unique_vector_index_name: TUniqueVectorIndexName,
) -> None:
    index_name = unique_vector_index_name(vector_index_client)
    create_response = vector_index_client.create_index(index_name, num_dimensions=2)
    assert isinstance(create_response, CreateIndex.Success)

    add_response = vector_index_client.add_item_batch(index_name, items=[Item(id="test_item", vector=[1.0, 2.0])])
    assert isinstance(add_response, AddItemBatch.Success)

    sleep(2)

    search_response = vector_index_client.search(index_name, query_vector=[1.0, 2.0], top_k=1)
    assert isinstance(search_response, Search.Success)
    assert len(search_response.hits) == 1
    assert search_response.hits[0].id == "test_item"
    assert search_response.hits[0].distance == 5.0


def test_create_index_add_multiple_items_search_happy_path(
    vector_index_client: PreviewVectorIndexClient,
    unique_vector_index_name: TUniqueVectorIndexName,
) -> None:
    index_name = unique_vector_index_name(vector_index_client)
    create_response = vector_index_client.create_index(index_name, num_dimensions=2)
    assert isinstance(create_response, CreateIndex.Success)

    add_response = vector_index_client.add_item_batch(
        index_name,
        items=[
            Item(id="test_item_1", vector=[1.0, 2.0]),
            Item(id="test_item_2", vector=[3.0, 4.0]),
            Item(id="test_item_3", vector=[5.0, 6.0]),
        ],
    )
    assert isinstance(add_response, AddItemBatch.Success)

    sleep(2)

    search_response = vector_index_client.search(index_name, query_vector=[1.0, 2.0], top_k=3)
    assert isinstance(search_response, Search.Success)
    assert len(search_response.hits) == 3

    assert search_response.hits == [
        SearchHit(id="test_item_3", distance=17.0),
        SearchHit(id="test_item_2", distance=11.0),
        SearchHit(id="test_item_1", distance=5.0),
    ]


def test_create_index_add_multiple_items_search_with_top_k_happy_path(
    vector_index_client: PreviewVectorIndexClient,
    unique_vector_index_name: TUniqueVectorIndexName,
) -> None:
    index_name = unique_vector_index_name(vector_index_client)
    create_response = vector_index_client.create_index(index_name, num_dimensions=2)
    assert isinstance(create_response, CreateIndex.Success)

    add_response = vector_index_client.add_item_batch(
        index_name,
        items=[
            Item(id="test_item_1", vector=[1.0, 2.0]),
            Item(id="test_item_2", vector=[3.0, 4.0]),
            Item(id="test_item_3", vector=[5.0, 6.0]),
        ],
    )
    assert isinstance(add_response, AddItemBatch.Success)

    sleep(2)

    search_response = vector_index_client.search(index_name, query_vector=[1.0, 2.0], top_k=2)
    assert isinstance(search_response, Search.Success)
    assert len(search_response.hits) == 2

    assert search_response.hits == [
        SearchHit(id="test_item_3", distance=17.0),
        SearchHit(id="test_item_2", distance=11.0),
    ]


def test_add_and_search_with_metadata_happy_path(
    vector_index_client: PreviewVectorIndexClient,
    unique_vector_index_name: TUniqueVectorIndexName,
) -> None:
    index_name = unique_vector_index_name(vector_index_client)
    create_response = vector_index_client.create_index(index_name, num_dimensions=2)
    assert isinstance(create_response, CreateIndex.Success)

    add_response = vector_index_client.add_item_batch(
        index_name,
        items=[
            Item(id="test_item_1", vector=[1.0, 2.0], metadata={"key1": "value1"}),
            Item(id="test_item_2", vector=[3.0, 4.0], metadata={"key2": "value2"}),
            Item(id="test_item_3", vector=[5.0, 6.0], metadata={"key1": "value3", "key3": "value3"}),
        ],
    )
    assert isinstance(add_response, AddItemBatch.Success)

    sleep(2)

    search_response = vector_index_client.search(index_name, query_vector=[1.0, 2.0], top_k=3)
    assert isinstance(search_response, Search.Success)
    assert len(search_response.hits) == 3

    assert search_response.hits == [
        SearchHit(id="test_item_3", distance=17.0),
        SearchHit(id="test_item_2", distance=11.0),
        SearchHit(id="test_item_1", distance=5.0),
    ]

    search_response = vector_index_client.search(index_name, query_vector=[1.0, 2.0], top_k=3, metadata_fields=["key1"])
    assert isinstance(search_response, Search.Success)
    assert len(search_response.hits) == 3

    assert search_response.hits == [
        SearchHit(id="test_item_3", distance=17.0, metadata={"key1": "value3"}),
        SearchHit(id="test_item_2", distance=11.0, metadata={}),
        SearchHit(id="test_item_1", distance=5.0, metadata={"key1": "value1"}),
    ]

    search_response = vector_index_client.search(
        index_name, query_vector=[1.0, 2.0], top_k=3, metadata_fields=["key1", "key2", "key3", "key4"]
    )
    assert isinstance(search_response, Search.Success)
    assert len(search_response.hits) == 3

    assert search_response.hits == [
        SearchHit(id="test_item_3", distance=17.0, metadata={"key1": "value3", "key3": "value3"}),
        SearchHit(id="test_item_2", distance=11.0, metadata={"key2": "value2"}),
        SearchHit(id="test_item_1", distance=5.0, metadata={"key1": "value1"}),
    ]


# TODO: test adding data of different dimension than the index


def test_add_validates_index_name(vector_index_client: PreviewVectorIndexClient) -> None:
    response = vector_index_client.add_item_batch(index_name="", items=[Item(id="test_item", vector=[1.0, 2.0])])
    assert isinstance(response, AddItemBatch.Error)
    assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR


def test_search_validates_index_name(vector_index_client: PreviewVectorIndexClient) -> None:
    response = vector_index_client.search(index_name="", query_vector=[1.0, 2.0])
    assert isinstance(response, Search.Error)
    assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR


def test_search_validates_top_k(vector_index_client: PreviewVectorIndexClient) -> None:
    response = vector_index_client.search(index_name="test_index", query_vector=[1.0, 2.0], top_k=0)
    assert isinstance(response, Search.Error)
    assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
    assert response.inner_exception.message == "Top k must be a positive integer."


def test_delete_validates_index_name(vector_index_client: PreviewVectorIndexClient) -> None:
    response = vector_index_client.delete_item_batch(index_name="", ids=[])
    assert isinstance(response, DeleteItemBatch.Error)
    assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR


def test_delete_deletes_ids(
    vector_index_client: PreviewVectorIndexClient,
    unique_vector_index_name: TUniqueVectorIndexName,
) -> None:
    index_name = unique_vector_index_name(vector_index_client)
    create_response = vector_index_client.create_index(index_name, num_dimensions=2)
    assert isinstance(create_response, CreateIndex.Success)

    add_response = vector_index_client.add_item_batch(
        index_name,
        items=[
            Item(id="test_item_1", vector=[1.0, 2.0]),
            Item(id="test_item_2", vector=[3.0, 4.0]),
            Item(id="test_item_3", vector=[5.0, 6.0]),
            Item(id="test_item_3", vector=[7.0, 8.0]),
        ],
    )
    assert isinstance(add_response, AddItemBatch.Success)

    sleep(2)

    search_response = vector_index_client.search(index_name, query_vector=[1.0, 2.0], top_k=10)
    assert isinstance(search_response, Search.Success)
    assert len(search_response.hits) == 3

    assert search_response.hits == [
        SearchHit(id="test_item_3", distance=23.0),
        SearchHit(id="test_item_2", distance=11.0),
        SearchHit(id="test_item_1", distance=5.0),
    ]

    delete_response = vector_index_client.delete_item_batch(index_name, ids=["test_item_1", "test_item_3"])
    assert isinstance(delete_response, DeleteItemBatch.Success)

    sleep(2)

    search_response = vector_index_client.search(index_name, query_vector=[1.0, 2.0], top_k=10)
    assert isinstance(search_response, Search.Success)
    assert len(search_response.hits) == 1

    assert search_response.hits == [
        SearchHit(id="test_item_2", distance=11.0),
    ]
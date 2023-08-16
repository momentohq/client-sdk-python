from momento import VectorIndexClient
from momento.errors import MomentoErrorCode
from momento.requests.item import Item
from momento.responses import CreateIndex, VectorIndexSearch, VectorIndexUpsertItemBatch
from momento.responses.vector_index.search import SearchHit
from tests.conftest import TUniqueVectorIndexName
from tests.utils import sleep


def test_create_index_upsert_item_search_happy_path(
    vector_index_client: VectorIndexClient, unique_vector_index_name: TUniqueVectorIndexName
) -> None:
    index_name = unique_vector_index_name(vector_index_client)
    create_response = vector_index_client.create_index(index_name, num_dimensions=2)
    assert isinstance(create_response, CreateIndex.Success)

    upsert_response = vector_index_client.upsert_item_batch(index_name, items=[Item(id="test_item", vector=[1.0, 2.0])])
    assert isinstance(upsert_response, VectorIndexUpsertItemBatch.Success)

    sleep(2)

    search_response = vector_index_client.search(index_name, query_vector=[1.0, 2.0], top_k=1)
    assert isinstance(search_response, VectorIndexSearch.Success)
    assert len(search_response.hits) == 1
    assert search_response.hits[0].id == "test_item"
    assert search_response.hits[0].distance == 5.0


def test_create_index_upsert_multiple_items_search_happy_path(
    vector_index_client: VectorIndexClient, unique_vector_index_name: TUniqueVectorIndexName
) -> None:
    index_name = unique_vector_index_name(vector_index_client)
    create_response = vector_index_client.create_index(index_name, num_dimensions=2)
    assert isinstance(create_response, CreateIndex.Success)

    upsert_response = vector_index_client.upsert_item_batch(
        index_name,
        items=[
            Item(id="test_item_1", vector=[1.0, 2.0]),
            Item(id="test_item_2", vector=[3.0, 4.0]),
            Item(id="test_item_3", vector=[5.0, 6.0]),
        ],
    )
    assert isinstance(upsert_response, VectorIndexUpsertItemBatch.Success)

    sleep(2)

    search_response = vector_index_client.search(index_name, query_vector=[1.0, 2.0], top_k=3)
    assert isinstance(search_response, VectorIndexSearch.Success)
    assert len(search_response.hits) == 3

    assert search_response.hits == [
        SearchHit(id="test_item_3", distance=17.0),
        SearchHit(id="test_item_2", distance=11.0),
        SearchHit(id="test_item_1", distance=5.0),
    ]


def test_upsert_and_search_with_metadata_happy_path(
    vector_index_client: VectorIndexClient, unique_vector_index_name: TUniqueVectorIndexName
) -> None:
    index_name = unique_vector_index_name(vector_index_client)
    create_response = vector_index_client.create_index(index_name, num_dimensions=2)
    assert isinstance(create_response, CreateIndex.Success)

    upsert_response = vector_index_client.upsert_item_batch(
        index_name,
        items=[
            Item(id="test_item_1", vector=[1.0, 2.0], metadata={"key1": "value1"}),
            Item(id="test_item_2", vector=[3.0, 4.0], metadata={"key2": "value2"}),
            Item(id="test_item_3", vector=[5.0, 6.0], metadata={"key1": "value3", "key3": "value3"}),
        ],
    )
    assert isinstance(upsert_response, VectorIndexUpsertItemBatch.Success)

    sleep(2)

    search_response = vector_index_client.search(index_name, query_vector=[1.0, 2.0], top_k=3)
    assert isinstance(search_response, VectorIndexSearch.Success)
    assert len(search_response.hits) == 3

    assert search_response.hits == [
        SearchHit(id="test_item_3", distance=17.0),
        SearchHit(id="test_item_2", distance=11.0),
        SearchHit(id="test_item_1", distance=5.0),
    ]

    search_response = vector_index_client.search(index_name, query_vector=[1.0, 2.0], top_k=3, metadata_fields=["key1"])
    assert isinstance(search_response, VectorIndexSearch.Success)
    assert len(search_response.hits) == 3

    assert search_response.hits == [
        SearchHit(id="test_item_3", distance=17.0, metadata={"key1": "value3"}),
        SearchHit(id="test_item_2", distance=11.0, metadata={}),
        SearchHit(id="test_item_1", distance=5.0, metadata={"key1": "value1"}),
    ]

    search_response = vector_index_client.search(
        index_name, query_vector=[1.0, 2.0], top_k=3, metadata_fields=["key1", "key2", "key3", "key4"]
    )
    assert isinstance(search_response, VectorIndexSearch.Success)
    assert len(search_response.hits) == 3

    assert search_response.hits == [
        SearchHit(id="test_item_3", distance=17.0, metadata={"key1": "value3", "key3": "value3"}),
        SearchHit(id="test_item_2", distance=11.0, metadata={"key2": "value2"}),
        SearchHit(id="test_item_1", distance=5.0, metadata={"key1": "value1"}),
    ]


def test_upsert_validates_index_name(vector_index_client: VectorIndexClient) -> None:
    response = vector_index_client.upsert_item_batch(index_name="", items=[Item(id="test_item", vector=[1.0, 2.0])])
    assert isinstance(response, VectorIndexUpsertItemBatch.Error)
    assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR


def test_search_validates_index_name(vector_index_client: VectorIndexClient) -> None:
    response = vector_index_client.search(index_name="", query_vector=[1.0, 2.0])
    assert isinstance(response, VectorIndexSearch.Error)
    assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR


def test_search_validates_top_k(vector_index_client: VectorIndexClient) -> None:
    response = vector_index_client.search(index_name="test_index", query_vector=[1.0, 2.0], top_k=0)
    assert isinstance(response, VectorIndexSearch.Error)
    assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
    assert response.inner_exception.message == "Top k must be a positive integer."

from __future__ import annotations

from typing import Optional, cast

import pytest
from momento import PreviewVectorIndexClient
from momento.common_data.vector_index.item import Metadata
from momento.errors import MomentoErrorCode
from momento.requests.vector_index import ALL_METADATA, Field, FilterExpression, Item, SimilarityMetric, filters
from momento.responses.vector_index import (
    CountItems,
    CreateIndex,
    DeleteItemBatch,
    GetItemBatch,
    GetItemMetadataBatch,
    Search,
    SearchAndFetchVectors,
    SearchHit,
    UpsertItemBatch,
)

from tests.conftest import TUniqueVectorIndexName
from tests.utils import sleep, uuid_str, when_fetching_vectors_apply_vectors_to_hits


def test_create_index_with_inner_product_upsert_item_search_happy_path(
    vector_index_client: PreviewVectorIndexClient,
    unique_vector_index_name: TUniqueVectorIndexName,
) -> None:
    index_name = unique_vector_index_name(vector_index_client)
    create_response = vector_index_client.create_index(
        index_name, num_dimensions=2, similarity_metric=SimilarityMetric.INNER_PRODUCT
    )
    assert isinstance(create_response, CreateIndex.Success)

    upsert_response = vector_index_client.upsert_item_batch(index_name, items=[Item(id="test_item", vector=[1.0, 2.0])])
    assert isinstance(upsert_response, UpsertItemBatch.Success)

    sleep(2)

    search_response = vector_index_client.search(index_name, query_vector=[1.0, 2.0], top_k=1)
    assert isinstance(search_response, Search.Success)
    assert len(search_response.hits) == 1
    assert search_response.hits[0].id == "test_item"
    assert search_response.hits[0].score == 5.0


@pytest.mark.parametrize("similarity_metric", [SimilarityMetric.COSINE_SIMILARITY, None])
def test_create_index_with_cosine_similarity_upsert_item_search_happy_path(
    vector_index_client: PreviewVectorIndexClient,
    unique_vector_index_name: TUniqueVectorIndexName,
    similarity_metric: Optional[SimilarityMetric],
) -> None:
    index_name = unique_vector_index_name(vector_index_client)
    num_dimensions = 2
    if similarity_metric is not None:
        create_response = vector_index_client.create_index(
            index_name, num_dimensions=num_dimensions, similarity_metric=similarity_metric
        )
    else:
        create_response = vector_index_client.create_index(index_name, num_dimensions=num_dimensions)
    assert isinstance(create_response, CreateIndex.Success)

    upsert_response = vector_index_client.upsert_item_batch(
        index_name,
        items=[
            Item(id="test_item_1", vector=[1.0, 1.0]),
            Item(id="test_item_2", vector=[-1.0, 1.0]),
            Item(id="test_item_3", vector=[-1.0, -1.0]),
        ],
    )
    assert isinstance(upsert_response, UpsertItemBatch.Success)

    sleep(2)

    search_response = vector_index_client.search(index_name, query_vector=[2.0, 2.0], top_k=3)
    assert isinstance(search_response, Search.Success)
    assert search_response.hits == [
        SearchHit(id="test_item_1", score=1.0),
        SearchHit(id="test_item_2", score=0.0),
        SearchHit(id="test_item_3", score=-1.0),
    ]


def test_create_index_with_euclidean_similarity_upsert_item_search_happy_path(
    vector_index_client: PreviewVectorIndexClient,
    unique_vector_index_name: TUniqueVectorIndexName,
) -> None:
    index_name = unique_vector_index_name(vector_index_client)
    create_response = vector_index_client.create_index(
        index_name, num_dimensions=2, similarity_metric=SimilarityMetric.EUCLIDEAN_SIMILARITY
    )
    assert isinstance(create_response, CreateIndex.Success)

    upsert_response = vector_index_client.upsert_item_batch(
        index_name,
        items=[
            Item(id="test_item_1", vector=[1.0, 1.0]),
            Item(id="test_item_2", vector=[-1.0, 1.0]),
            Item(id="test_item_3", vector=[-1.0, -1.0]),
        ],
    )
    assert isinstance(upsert_response, UpsertItemBatch.Success)

    sleep(2)

    search_response = vector_index_client.search(index_name, query_vector=[1.0, 1.0], top_k=3)
    assert isinstance(search_response, Search.Success)
    assert search_response.hits == [
        SearchHit(id="test_item_1", score=0.0),
        SearchHit(id="test_item_2", score=4.0),
        SearchHit(id="test_item_3", score=8.0),
    ]


def test_create_index_upsert_multiple_items_search_happy_path(
    vector_index_client: PreviewVectorIndexClient,
    unique_vector_index_name: TUniqueVectorIndexName,
) -> None:
    index_name = unique_vector_index_name(vector_index_client)
    create_response = vector_index_client.create_index(
        index_name, num_dimensions=2, similarity_metric=SimilarityMetric.INNER_PRODUCT
    )
    assert isinstance(create_response, CreateIndex.Success)

    upsert_response = vector_index_client.upsert_item_batch(
        index_name,
        items=[
            Item(id="test_item_1", vector=[1.0, 2.0]),
            Item(id="test_item_2", vector=[3.0, 4.0]),
            Item(id="test_item_3", vector=[5.0, 6.0]),
        ],
    )
    assert isinstance(upsert_response, UpsertItemBatch.Success)

    sleep(2)

    search_response = vector_index_client.search(index_name, query_vector=[1.0, 2.0], top_k=3)
    assert isinstance(search_response, Search.Success)
    assert len(search_response.hits) == 3

    assert search_response.hits == [
        SearchHit(id="test_item_3", score=17.0),
        SearchHit(id="test_item_2", score=11.0),
        SearchHit(id="test_item_1", score=5.0),
    ]


# A note on the parameterized search tests:
# The search tests are parameterized to test both the search and search_and_fetch_vectors methods.
# We pass both the name of the specific search method and the response type.
# The tests will run for both search methods, and the response type will be used to assert the type of the response.
# The vectors are attached to the hits if the response type is SearchAndFetchVectors.Success.
@pytest.mark.parametrize(
    ["search_method_name", "response"],
    [("search", Search.Success), ("search_and_fetch_vectors", SearchAndFetchVectors.Success)],
)
def test_create_index_upsert_multiple_items_search_with_top_k_happy_path(
    vector_index_client: PreviewVectorIndexClient,
    unique_vector_index_name: TUniqueVectorIndexName,
    search_method_name: str,
    response: type[Search.Success] | type[SearchAndFetchVectors.Success],
) -> None:
    index_name = unique_vector_index_name(vector_index_client)
    create_response = vector_index_client.create_index(
        index_name, num_dimensions=2, similarity_metric=SimilarityMetric.INNER_PRODUCT
    )
    assert isinstance(create_response, CreateIndex.Success)

    items = [
        Item(id="test_item_1", vector=[1.0, 2.0]),
        Item(id="test_item_2", vector=[3.0, 4.0]),
        Item(id="test_item_3", vector=[5.0, 6.0]),
    ]
    upsert_response = vector_index_client.upsert_item_batch(
        index_name,
        items=items,
    )
    assert isinstance(upsert_response, UpsertItemBatch.Success)

    sleep(2)

    search = getattr(vector_index_client, search_method_name)
    search_response = search(index_name, query_vector=[1.0, 2.0], top_k=2)
    assert isinstance(search_response, response)
    assert len(search_response.hits) == 2

    hits = [SearchHit(id="test_item_3", score=17.0), SearchHit(id="test_item_2", score=11.0)]
    hits = when_fetching_vectors_apply_vectors_to_hits(search_response, hits, items)
    assert search_response.hits == hits


@pytest.mark.parametrize(
    ["search_method_name", "response"],
    [("search", Search.Success), ("search_and_fetch_vectors", SearchAndFetchVectors.Success)],
)
def test_upsert_and_search_with_metadata_happy_path(
    vector_index_client: PreviewVectorIndexClient,
    unique_vector_index_name: TUniqueVectorIndexName,
    search_method_name: str,
    response: type[Search.Success] | type[SearchAndFetchVectors.Success],
) -> None:
    index_name = unique_vector_index_name(vector_index_client)
    create_response = vector_index_client.create_index(
        index_name, num_dimensions=2, similarity_metric=SimilarityMetric.INNER_PRODUCT
    )
    assert isinstance(create_response, CreateIndex.Success)

    items = [
        Item(id="test_item_1", vector=[1.0, 2.0], metadata={"key1": "value1"}),
        Item(id="test_item_2", vector=[3.0, 4.0], metadata={"key2": "value2"}),
        Item(id="test_item_3", vector=[5.0, 6.0], metadata={"key1": "value3", "key3": "value3"}),
    ]
    upsert_response = vector_index_client.upsert_item_batch(
        index_name,
        items=items,
    )
    assert isinstance(upsert_response, UpsertItemBatch.Success)

    sleep(2)

    search = getattr(vector_index_client, search_method_name)
    search_response = search(index_name, query_vector=[1.0, 2.0], top_k=3)
    assert isinstance(search_response, response)
    assert len(search_response.hits) == 3

    hits = [
        SearchHit(id="test_item_3", score=17.0),
        SearchHit(id="test_item_2", score=11.0),
        SearchHit(id="test_item_1", score=5.0),
    ]
    hits = when_fetching_vectors_apply_vectors_to_hits(search_response, hits, items)
    assert search_response.hits == hits

    search_response = search(index_name, query_vector=[1.0, 2.0], top_k=3, metadata_fields=["key1"])
    assert isinstance(search_response, response)
    assert len(search_response.hits) == 3

    hits = [
        SearchHit(id="test_item_3", score=17.0, metadata={"key1": "value3"}),
        SearchHit(id="test_item_2", score=11.0, metadata={}),
        SearchHit(id="test_item_1", score=5.0, metadata={"key1": "value1"}),
    ]
    hits = when_fetching_vectors_apply_vectors_to_hits(search_response, hits, items)
    assert search_response.hits == hits

    search_response = search(
        index_name, query_vector=[1.0, 2.0], top_k=3, metadata_fields=["key1", "key2", "key3", "key4"]
    )
    assert isinstance(search_response, response)
    assert len(search_response.hits) == 3

    hits = [
        SearchHit(id="test_item_3", score=17.0, metadata={"key1": "value3", "key3": "value3"}),
        SearchHit(id="test_item_2", score=11.0, metadata={"key2": "value2"}),
        SearchHit(id="test_item_1", score=5.0, metadata={"key1": "value1"}),
    ]
    hits = when_fetching_vectors_apply_vectors_to_hits(search_response, hits, items)
    assert search_response.hits == hits


def test_upsert_with_bad_metadata(vector_index_client: PreviewVectorIndexClient) -> None:
    response = vector_index_client.upsert_item_batch(
        index_name="test_index",
        items=[Item(id="test_item", vector=[1.0, 2.0], metadata={"key": {"subkey": "subvalue"}})],  # type: ignore
    )
    assert isinstance(response, UpsertItemBatch.Error)
    assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
    assert (
        response.message
        == "Invalid argument passed to Momento client: Metadata values must be either str, int, float, bool, or list[str]. Field 'key' has a value of type <class 'dict'> with value {'subkey': 'subvalue'}."  # noqa: E501 W503
    )


@pytest.mark.parametrize(
    ["search_method_name", "response"],
    [("search", Search.Success), ("search_and_fetch_vectors", SearchAndFetchVectors.Success)],
)
def test_upsert_and_search_with_all_metadata_happy_path(
    vector_index_client: PreviewVectorIndexClient,
    unique_vector_index_name: TUniqueVectorIndexName,
    search_method_name: str,
    response: type[Search.Success] | type[SearchAndFetchVectors.Success],
) -> None:
    index_name = unique_vector_index_name(vector_index_client)
    create_response = vector_index_client.create_index(
        index_name, num_dimensions=2, similarity_metric=SimilarityMetric.INNER_PRODUCT
    )
    assert isinstance(create_response, CreateIndex.Success)

    items = [
        Item(id="test_item_1", vector=[1.0, 2.0], metadata={"key1": "value1"}),
        Item(id="test_item_2", vector=[3.0, 4.0], metadata={"key2": "value2"}),
        Item(id="test_item_3", vector=[5.0, 6.0], metadata={"key1": "value3", "key3": "value3"}),
    ]
    upsert_response = vector_index_client.upsert_item_batch(
        index_name,
        items=items,
    )
    assert isinstance(upsert_response, UpsertItemBatch.Success)

    sleep(2)

    search = getattr(vector_index_client, search_method_name)
    search_response = search(index_name, query_vector=[1.0, 2.0], top_k=3)
    assert isinstance(search_response, response)
    assert len(search_response.hits) == 3

    hits = [
        SearchHit(id="test_item_3", score=17.0),
        SearchHit(id="test_item_2", score=11.0),
        SearchHit(id="test_item_1", score=5.0),
    ]
    hits = when_fetching_vectors_apply_vectors_to_hits(search_response, hits, items)
    assert search_response.hits == hits

    search_response = search(index_name, query_vector=[1.0, 2.0], top_k=3, metadata_fields=ALL_METADATA)
    assert isinstance(search_response, response)
    assert len(search_response.hits) == 3

    hits = [
        SearchHit(id="test_item_3", score=17.0, metadata={"key1": "value3", "key3": "value3"}),
        SearchHit(id="test_item_2", score=11.0, metadata={"key2": "value2"}),
        SearchHit(id="test_item_1", score=5.0, metadata={"key1": "value1"}),
    ]
    hits = when_fetching_vectors_apply_vectors_to_hits(search_response, hits, items)
    assert search_response.hits == hits


@pytest.mark.parametrize(
    ["search_method_name", "response"],
    [
        ("search", Search.Success),
        (
            "search_and_fetch_vectors",
            SearchAndFetchVectors.Success,
        ),
    ],
)
def test_upsert_and_search_with_diverse_metadata_happy_path(
    vector_index_client: PreviewVectorIndexClient,
    unique_vector_index_name: TUniqueVectorIndexName,
    search_method_name: str,
    response: type[Search.Success] | type[SearchAndFetchVectors.Success],
) -> None:
    index_name = unique_vector_index_name(vector_index_client)
    create_response = vector_index_client.create_index(
        index_name, num_dimensions=2, similarity_metric=SimilarityMetric.INNER_PRODUCT
    )
    assert isinstance(create_response, CreateIndex.Success)

    metadata: Metadata = {
        "string": "value",
        "bool": True,
        "int": 1,
        "float": 3.14,
        "list": ["a", "b", "c"],
        "empty_list": [],
    }
    items = [
        Item(id="test_item_1", vector=[1.0, 2.0], metadata=metadata),
    ]
    upsert_response = vector_index_client.upsert_item_batch(index_name, items=items)
    assert isinstance(upsert_response, UpsertItemBatch.Success)

    sleep(2)

    search = getattr(vector_index_client, search_method_name)
    search_response = search(index_name, query_vector=[1.0, 2.0], top_k=1, metadata_fields=ALL_METADATA)
    assert isinstance(search_response, response)
    assert len(search_response.hits) == 1

    hits = [SearchHit(id="test_item_1", metadata=metadata, score=5.0)]
    hits = when_fetching_vectors_apply_vectors_to_hits(search_response, hits, items)
    assert search_response.hits == hits


def test_upsert_replaces_existing_items(
    vector_index_client: PreviewVectorIndexClient,
    unique_vector_index_name: TUniqueVectorIndexName,
) -> None:
    index_name = unique_vector_index_name(vector_index_client)
    create_response = vector_index_client.create_index(
        index_name, num_dimensions=2, similarity_metric=SimilarityMetric.INNER_PRODUCT
    )
    assert isinstance(create_response, CreateIndex.Success)

    upsert_response = vector_index_client.upsert_item_batch(
        index_name,
        items=[
            Item(id="test_item_1", vector=[1.0, 2.0], metadata={"key1": "value1"}),
            Item(id="test_item_2", vector=[3.0, 4.0], metadata={"key2": "value2"}),
            Item(id="test_item_3", vector=[5.0, 6.0], metadata={"key1": "value3", "key3": "value3"}),
        ],
    )
    assert isinstance(upsert_response, UpsertItemBatch.Success)

    sleep(2)

    upsert_response = vector_index_client.upsert_item_batch(
        index_name,
        items=[
            Item(id="test_item_1", vector=[2.0, 4.0], metadata={"key4": "value4"}),
        ],
    )
    assert isinstance(upsert_response, UpsertItemBatch.Success)

    sleep(2)

    search_response = vector_index_client.search(
        index_name, query_vector=[1.0, 2.0], top_k=5, metadata_fields=["key1", "key2", "key3", "key4"]
    )
    assert isinstance(search_response, Search.Success)
    assert len(search_response.hits) == 3

    assert search_response.hits == [
        SearchHit(id="test_item_3", score=17.0, metadata={"key1": "value3", "key3": "value3"}),
        SearchHit(id="test_item_2", score=11.0, metadata={"key2": "value2"}),
        SearchHit(id="test_item_1", score=10.0, metadata={"key4": "value4"}),
    ]


def test_create_index_upsert_item_dimensions_different_than_num_dimensions_error(
    vector_index_client: PreviewVectorIndexClient,
    unique_vector_index_name: TUniqueVectorIndexName,
) -> None:
    index_name = unique_vector_index_name(vector_index_client)
    create_response = vector_index_client.create_index(
        index_name, num_dimensions=2, similarity_metric=SimilarityMetric.INNER_PRODUCT
    )
    assert isinstance(create_response, CreateIndex.Success)

    # upserting 3 dimensions
    upsert_response = vector_index_client.upsert_item_batch(
        index_name, items=[Item(id="test_item", vector=[1.0, 2.0, 3.0])]
    )
    assert isinstance(upsert_response, UpsertItemBatch.Error)

    expected_inner_ex_message = "invalid parameter: vector, vector dimension has to match the index's dimension"
    expected_message = f"Invalid argument passed to Momento client: {expected_inner_ex_message}"
    assert upsert_response.message == expected_message
    assert upsert_response.inner_exception.message == expected_inner_ex_message


@pytest.mark.parametrize(
    ["search_method_name", "error_type"],
    [("search", Search.Error), ("search_and_fetch_vectors", SearchAndFetchVectors.Error)],
)
def test_create_index_upsert_multiple_items_search_with_top_k_query_vector_dimensions_incorrect(
    vector_index_client: PreviewVectorIndexClient,
    unique_vector_index_name: TUniqueVectorIndexName,
    search_method_name: str,
    error_type: type[Search.Error] | type[SearchAndFetchVectors.Error],
) -> None:
    index_name = unique_vector_index_name(vector_index_client)
    create_response = vector_index_client.create_index(
        index_name, num_dimensions=2, similarity_metric=SimilarityMetric.INNER_PRODUCT
    )
    assert isinstance(create_response, CreateIndex.Success)

    upsert_response = vector_index_client.upsert_item_batch(
        index_name,
        items=[
            Item(id="test_item_1", vector=[1.0, 2.0]),
            Item(id="test_item_2", vector=[3.0, 4.0]),
            Item(id="test_item_3", vector=[5.0, 6.0]),
        ],
    )
    assert isinstance(upsert_response, UpsertItemBatch.Success)

    search = getattr(vector_index_client, search_method_name)
    search_response = search(index_name, query_vector=[1.0, 2.0, 3.0], top_k=2)
    assert isinstance(search_response, error_type)

    expected_inner_ex_message = "invalid parameter: query_vector, query vector dimension must match the index dimension"
    expected_resp_message = f"Invalid argument passed to Momento client: {expected_inner_ex_message}"

    assert search_response.inner_exception.message == expected_inner_ex_message
    assert search_response.message == expected_resp_message


def test_upsert_validates_index_name(vector_index_client: PreviewVectorIndexClient) -> None:
    response = vector_index_client.upsert_item_batch(index_name="", items=[Item(id="test_item", vector=[1.0, 2.0])])
    assert isinstance(response, UpsertItemBatch.Error)
    assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR


@pytest.mark.parametrize(
    ["search_method_name", "error_type"],
    [("search", Search.Error), ("search_and_fetch_vectors", SearchAndFetchVectors.Error)],
)
def test_search_validates_index_name(
    vector_index_client: PreviewVectorIndexClient,
    search_method_name: str,
    error_type: type[Search.Error] | type[SearchAndFetchVectors.Error],
) -> None:
    search = getattr(vector_index_client, search_method_name)
    response = search(index_name="", query_vector=[1.0, 2.0])
    assert isinstance(response, error_type)
    assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR


@pytest.mark.parametrize(
    ["search_method_name", "error_type"],
    [("search", Search.Error), ("search_and_fetch_vectors", SearchAndFetchVectors.Error)],
)
def test_search_validates_top_k(
    vector_index_client: PreviewVectorIndexClient,
    search_method_name: str,
    error_type: type[Search.Error] | type[SearchAndFetchVectors.Error],
) -> None:
    search = getattr(vector_index_client, search_method_name)
    response = search(index_name="test_index", query_vector=[1.0, 2.0], top_k=0)
    assert isinstance(response, error_type)
    assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
    assert response.inner_exception.message == "Top k must be a positive integer."


@pytest.mark.parametrize(
    ["similarity_metric", "distances", "thresholds", "search_method_name", "response"],
    [
        # Distances are the distance to the same 3 data vectors from the same query vector.
        # Thresholds are those that should:
        # 1. exclude lowest two matches
        # 2. keep all matches
        # 3. exclude all matches
        (SimilarityMetric.COSINE_SIMILARITY, [1.0, 0.0, -1.0], [0.5, -1.01, 1.0], "search", Search.Success),
        (
            SimilarityMetric.COSINE_SIMILARITY,
            [1.0, 0.0, -1.0],
            [0.5, -1.01, 1.0],
            "search_and_fetch_vectors",
            SearchAndFetchVectors.Success,
        ),
        (SimilarityMetric.INNER_PRODUCT, [4.0, 0.0, -4.0], [0.0, -4.01, 4.0], "search", Search.Success),
        (
            SimilarityMetric.INNER_PRODUCT,
            [4.0, 0.0, -4.0],
            [0.0, -4.01, 4.0],
            "search_and_fetch_vectors",
            SearchAndFetchVectors.Success,
        ),
        (SimilarityMetric.EUCLIDEAN_SIMILARITY, [2, 10, 18], [3, 20, -0.01], "search", Search.Success),
        (
            SimilarityMetric.EUCLIDEAN_SIMILARITY,
            [2, 10, 18],
            [3, 20, -0.01],
            "search_and_fetch_vectors",
            SearchAndFetchVectors.Success,
        ),
    ],
)
def test_search_score_threshold_happy_path(
    vector_index_client: PreviewVectorIndexClient,
    unique_vector_index_name: TUniqueVectorIndexName,
    similarity_metric: SimilarityMetric,
    distances: list[float],
    thresholds: list[float],
    search_method_name: str,
    response: type[Search.Success] | type[SearchAndFetchVectors.Success],
) -> None:
    index_name = unique_vector_index_name(vector_index_client)
    num_dimensions = 2
    create_response = vector_index_client.create_index(
        index_name, num_dimensions=num_dimensions, similarity_metric=similarity_metric
    )
    assert isinstance(create_response, CreateIndex.Success)

    items = [
        Item(id="test_item_1", vector=[1.0, 1.0]),
        Item(id="test_item_2", vector=[-1.0, 1.0]),
        Item(id="test_item_3", vector=[-1.0, -1.0]),
    ]
    upsert_response = vector_index_client.upsert_item_batch(
        index_name,
        items=items,
    )
    assert isinstance(upsert_response, UpsertItemBatch.Success)

    sleep(2)

    query_vector = [2.0, 2.0]
    search_hits = [SearchHit(id=f"test_item_{i+1}", score=distance) for i, distance in enumerate(distances)]

    search = getattr(vector_index_client, search_method_name)
    search_response = search(index_name, query_vector=query_vector, top_k=3, score_threshold=thresholds[0])
    assert isinstance(search_response, response)
    assert search_response.hits == when_fetching_vectors_apply_vectors_to_hits(search_response, [search_hits[0]], items)

    search_response2 = search(index_name, query_vector=query_vector, top_k=3, score_threshold=thresholds[1])
    assert isinstance(search_response2, response)
    assert search_response2.hits == when_fetching_vectors_apply_vectors_to_hits(search_response, search_hits, items)

    search_response3 = search(index_name, query_vector=query_vector, top_k=3, score_threshold=thresholds[2])
    assert isinstance(search_response3, response)
    assert search_response3.hits == []


def test_search_with_filter_expression(
    vector_index_client: PreviewVectorIndexClient,
    unique_vector_index_name: TUniqueVectorIndexName,
) -> None:
    index_name = unique_vector_index_name(vector_index_client)
    num_dimensions = 2
    create_response = vector_index_client.create_index(
        index_name, num_dimensions=num_dimensions, similarity_metric=SimilarityMetric.INNER_PRODUCT
    )
    assert isinstance(create_response, CreateIndex.Success)
    items = [
        Item(
            id="test_item_1",
            vector=[1.0, 1.0],
            metadata={"str": "value1", "int": 0, "float": 0.0, "bool": True, "tags": ["a", "b", "c"]},
        ),
        Item(
            id="test_item_2",
            vector=[-1.0, 1.0],
            metadata={"str": "value2", "int": 5, "float": 5.0, "bool": False, "tags": ["a", "b"]},
        ),
        Item(
            id="test_item_3",
            vector=[-1.0, -1.0],
            metadata={"str": "value3", "int": 10, "float": 10.0, "bool": True, "tags": ["a", "d"]},
        ),
    ]
    upsert_response = vector_index_client.upsert_item_batch(
        index_name,
        items=items,
    )
    assert isinstance(upsert_response, UpsertItemBatch.Success)
    sleep(2)

    # Writing the test cases here instead of as a parameterized test because:
    # 1. The search data is the same across tests, so no need to reindex each time.
    # 2. It is 10x faster to run the tests this way.
    for filter, expected_ids, test_case_name in [
        (Field("str") == "value1", ["test_item_1"], "string equality"),
        (Field("str") != "value1", ["test_item_2", "test_item_3"], "string inequality"),
        (Field("int") == 0, ["test_item_1"], "int equality"),
        (Field("float") == 0.0, ["test_item_1"], "float equality"),
        (Field("bool"), ["test_item_1", "test_item_3"], "bool equality"),
        (~Field("bool"), ["test_item_2"], "bool inequality"),
        (Field("int") > 5, ["test_item_3"], "int greater than"),
        (Field("int") >= 5, ["test_item_2", "test_item_3"], "int greater than or equal to"),
        (Field("float") > 5.0, ["test_item_3"], "float greater than"),
        (Field("float") >= 5.0, ["test_item_2", "test_item_3"], "float greater than or equal to"),
        (Field("int") < 5, ["test_item_1"], "int less than"),
        (Field("int") <= 5, ["test_item_1", "test_item_2"], "int less than or equal to"),
        (Field("float") < 5.0, ["test_item_1"], "float less than"),
        (Field("float") <= 5.0, ["test_item_1", "test_item_2"], "float less than or equal to"),
        (Field("tags").list_contains("a"), ["test_item_1", "test_item_2", "test_item_3"], "list contains a"),
        (Field("tags").list_contains("b"), ["test_item_1", "test_item_2"], "list contains b"),
        (Field("tags").list_contains("m"), [], "list contains m"),
        ((Field("tags").list_contains("b")) & (Field("int") > 1), ["test_item_2"], "list contains b and int > 1"),
        (
            (Field("tags").list_contains("b")) | (Field("int") > 1),
            ["test_item_1", "test_item_2", "test_item_3"],
            "list contains b or int > 1",
        ),
        (filters.IdInSet({}), [], "id in empty set"),
        (filters.IdInSet({"not there"}), [], "id in set not there"),
        (filters.IdInSet({"test_item_1", "test_item_3"}), ["test_item_1", "test_item_3"], "id in set"),
    ]:
        filter = cast(FilterExpression, filter)
        search_response = vector_index_client.search(index_name, query_vector=[2.0, 2.0], filter=filter)
        assert isinstance(
            search_response, Search.Success
        ), f"Expected search {test_case_name!r} to succeed but got {search_response!r}"
        assert (
            [hit.id for hit in search_response.hits] == expected_ids
        ), f"Expected search {test_case_name!r} to return {expected_ids!r} but got {search_response.hits!r}"

        search_and_fetch_vectors_response = vector_index_client.search_and_fetch_vectors(
            index_name, query_vector=[2.0, 2.0], filter=filter
        )
        assert isinstance(
            search_and_fetch_vectors_response, SearchAndFetchVectors.Success
        ), f"Expected search {test_case_name!r} to succeed but got {search_and_fetch_vectors_response!r}"
        assert [hit.id for hit in search_and_fetch_vectors_response.hits] == expected_ids, (
            f"Expected search {test_case_name!r} to return {expected_ids!r} "
            f"but got {search_and_fetch_vectors_response.hits!r}"
        )


def test_delete_validates_index_name(vector_index_client: PreviewVectorIndexClient) -> None:
    response = vector_index_client.delete_item_batch(index_name="", ids=[])
    assert isinstance(response, DeleteItemBatch.Error)
    assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR


def test_delete_deletes_ids(
    vector_index_client: PreviewVectorIndexClient,
    unique_vector_index_name: TUniqueVectorIndexName,
) -> None:
    index_name = unique_vector_index_name(vector_index_client)
    create_response = vector_index_client.create_index(
        index_name, num_dimensions=2, similarity_metric=SimilarityMetric.INNER_PRODUCT
    )
    assert isinstance(create_response, CreateIndex.Success)

    upsert_response = vector_index_client.upsert_item_batch(
        index_name,
        items=[
            Item(id="test_item_1", vector=[1.0, 2.0]),
            Item(id="test_item_2", vector=[3.0, 4.0]),
            Item(id="test_item_3", vector=[5.0, 6.0]),
            Item(id="test_item_3", vector=[7.0, 8.0]),
        ],
    )
    assert isinstance(upsert_response, UpsertItemBatch.Success)

    sleep(2)

    search_response = vector_index_client.search(index_name, query_vector=[1.0, 2.0], top_k=10)
    assert isinstance(search_response, Search.Success)
    assert len(search_response.hits) == 3

    assert search_response.hits == [
        SearchHit(id="test_item_3", score=23.0),
        SearchHit(id="test_item_2", score=11.0),
        SearchHit(id="test_item_1", score=5.0),
    ]

    delete_response = vector_index_client.delete_item_batch(index_name, ids=["test_item_1", "test_item_3"])
    assert isinstance(delete_response, DeleteItemBatch.Success)

    sleep(2)

    search_response = vector_index_client.search(index_name, query_vector=[1.0, 2.0], top_k=10)
    assert isinstance(search_response, Search.Success)
    assert len(search_response.hits) == 1

    assert search_response.hits == [
        SearchHit(id="test_item_2", score=11.0),
    ]


@pytest.mark.parametrize(
    [
        "get_item_method_name",
        "ids",
        "expected_get_item_response",
        "expected_get_item_values",
    ],
    [
        ("get_item_batch", [], GetItemBatch.Success, {}),
        ("get_item_metadata_batch", [], GetItemMetadataBatch.Success, {}),
        ("get_item_batch", ["missing_id"], GetItemBatch.Success, {}),
        (
            "get_item_metadata_batch",
            ["test_item_1"],
            GetItemMetadataBatch.Success,
            {"test_item_1": {"key1": "value1"}},
        ),
        ("get_item_metadata_batch", ["missing_id"], GetItemMetadataBatch.Success, {}),
        (
            "get_item_batch",
            ["test_item_1"],
            GetItemBatch.Success,
            {
                "test_item_1": Item(id="test_item_1", vector=[1.0, 1.0], metadata={"key1": "value1"}),
            },
        ),
        (
            "get_item_batch",
            ["test_item_1", "missing_id", "test_item_2"],
            GetItemBatch.Success,
            {
                "test_item_1": Item(id="test_item_1", vector=[1.0, 1.0], metadata={"key1": "value1"}),
                "test_item_2": Item(id="test_item_2", vector=[-1.0, 1.0], metadata={}),
            },
        ),
        (
            "get_item_metadata_batch",
            ["test_item_1", "missing_id", "test_item_2"],
            GetItemMetadataBatch.Success,
            {
                "test_item_1": {"key1": "value1"},
                "test_item_2": {},
            },
        ),
    ],
)
def test_get_items_by_id(
    vector_index_client: PreviewVectorIndexClient,
    unique_vector_index_name: TUniqueVectorIndexName,
    get_item_method_name: str,
    ids: list[str],
    expected_get_item_response: type[GetItemMetadataBatch.Success] | type[GetItemBatch.Success],
    expected_get_item_values: dict[str, Metadata] | dict[str, Item],
) -> None:
    index_name = unique_vector_index_name(vector_index_client)
    create_response = vector_index_client.create_index(index_name, num_dimensions=2)
    assert isinstance(create_response, CreateIndex.Success)

    items = [
        Item(id="test_item_1", vector=[1.0, 1.0], metadata={"key1": "value1"}),
        Item(id="test_item_2", vector=[-1.0, 1.0]),
        Item(id="test_item_3", vector=[-1.0, -1.0]),
    ]
    upsert_response = vector_index_client.upsert_item_batch(
        index_name,
        items=items,
    )
    assert isinstance(upsert_response, UpsertItemBatch.Success)

    sleep(2)

    get_item = getattr(vector_index_client, get_item_method_name)
    get_item_response = get_item(index_name, ids)
    assert isinstance(get_item_response, expected_get_item_response)
    assert get_item_response.values == expected_get_item_values


def test_count_items_on_missing_index(
    vector_index_client: PreviewVectorIndexClient,
) -> None:
    response = vector_index_client.count_items(index_name=uuid_str())
    assert isinstance(response, CountItems.Error)
    assert response.error_code == MomentoErrorCode.NOT_FOUND_ERROR


def test_count_items_on_empty_index(
    vector_index_client: PreviewVectorIndexClient,
    unique_vector_index_name: TUniqueVectorIndexName,
) -> None:
    index_name = unique_vector_index_name(vector_index_client)

    create_response = vector_index_client.create_index(index_name, num_dimensions=2)
    assert isinstance(create_response, CreateIndex.Success)

    count_response = vector_index_client.count_items(index_name)
    assert isinstance(count_response, CountItems.Success)
    assert count_response.item_count == 0


def test_count_items_with_items(
    vector_index_client: PreviewVectorIndexClient,
    unique_vector_index_name: TUniqueVectorIndexName,
) -> None:
    num_items = 10
    index_name = unique_vector_index_name(vector_index_client)

    create_response = vector_index_client.create_index(index_name, num_dimensions=2)
    assert isinstance(create_response, CreateIndex.Success)

    items = [Item(id=f"test_item_{i}", vector=[i, i]) for i in range(num_items)]  # type: list[Item]
    upsert_response = vector_index_client.upsert_item_batch(
        index_name,
        items=items,
    )
    assert isinstance(upsert_response, UpsertItemBatch.Success)

    sleep(2)

    count_response = vector_index_client.count_items(index_name)
    assert isinstance(count_response, CountItems.Success)
    assert count_response.item_count == num_items

    num_items_to_delete = 5
    delete_response = vector_index_client.delete_item_batch(
        index_name, ids=[item.id for item in items[:num_items_to_delete]]
    )
    assert isinstance(delete_response, DeleteItemBatch.Success)

    sleep(2)

    count_response = vector_index_client.count_items(index_name)
    assert isinstance(count_response, CountItems.Success)
    assert count_response.item_count == num_items - num_items_to_delete

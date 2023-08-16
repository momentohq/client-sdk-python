from momento import PreviewVectorIndexClient
from momento.errors import MomentoErrorCode
from momento.responses.vector_index import CreateIndex, DeleteIndex, ListIndexes
from tests.conftest import TUniqueVectorIndexName
from tests.utils import unique_test_vector_index_name


def test_create_index_list_indexes_and_delete_index(
    vector_index_client: PreviewVectorIndexClient,
    unique_vector_index_name: TUniqueVectorIndexName,
    vector_index_dimensions: int,
) -> None:
    new_index_name = unique_vector_index_name(vector_index_client)

    create_index_response = vector_index_client.create_index(new_index_name, num_dimensions=vector_index_dimensions)
    assert isinstance(create_index_response, CreateIndex.Success)

    list_indexes_response = vector_index_client.list_indexes()
    assert isinstance(list_indexes_response, ListIndexes.Success)
    assert any(index_info.name == new_index_name for index_info in list_indexes_response.indexes)

    delete_index_response = vector_index_client.delete_index(new_index_name)
    assert isinstance(delete_index_response, DeleteIndex.Success)


def test_create_index_already_exists_when_creating_existing_index(
    vector_index_client: PreviewVectorIndexClient, vector_index_name: str, vector_index_dimensions: int
) -> None:
    response = vector_index_client.create_index(vector_index_name, num_dimensions=vector_index_dimensions)
    assert isinstance(response, CreateIndex.IndexAlreadyExists)


def test_create_index_returns_error_for_bad_name(
    vector_index_client: PreviewVectorIndexClient,
) -> None:
    for bad_name, reason in [("", "not be empty"), (None, "be a string"), (1, "be a string")]:
        response = vector_index_client.create_index(bad_name, num_dimensions=1)  # type: ignore[arg-type]
        assert isinstance(response, CreateIndex.Error)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
        assert response.inner_exception.message == f"Vector index name must {reason}"


def test_create_index_returns_error_for_bad_num_dimensions(
    vector_index_client: PreviewVectorIndexClient,
    unique_vector_index_name: TUniqueVectorIndexName,
) -> None:
    for bad_num_dimensions in [0, 1.1]:
        response = vector_index_client.create_index(
            unique_vector_index_name(vector_index_client), num_dimensions=bad_num_dimensions  # type: ignore[arg-type]
        )
        assert isinstance(response, CreateIndex.Error)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
        assert response.inner_exception.message == "Number of dimensions must be a positive integer."


# TODO: Add test for bad token when creating index
# async def test_create_cache_throws_authentication_exception_for_bad_token(
#     bad_token_credential_provider: CredentialProvider,
#     configuration: Configuration,
#     default_ttl_seconds: timedelta,
#     unique_cache_name_async: TUniqueCacheNameAsync,
# ) -> None:
#     async with CacheClientAsync(configuration, bad_token_credential_provider, default_ttl_seconds) as client_async:
#         new_cache_name = unique_cache_name_async(client_async)
#         response = await client_async.create_cache(new_cache_name)
#         assert isinstance(response, CreateCache.Error)
#         assert response.error_code == errors.MomentoErrorCode.AUTHENTICATION_ERROR


# Delete index
def test_delete_cache_succeeds(vector_index_client: PreviewVectorIndexClient, vector_index_dimensions: int) -> None:
    index_name = unique_test_vector_index_name()

    response = vector_index_client.create_index(index_name, vector_index_dimensions)
    assert isinstance(response, CreateIndex.Success)

    delete_response = vector_index_client.delete_index(index_name)
    assert isinstance(delete_response, DeleteIndex.Success)

    delete_response = vector_index_client.delete_index(index_name)
    assert isinstance(delete_response, DeleteIndex.Error)
    assert delete_response.error_code == MomentoErrorCode.NOT_FOUND_ERROR


def test_delete_index_returns_not_found_error_when_deleting_unknown_index(
    vector_index_client: PreviewVectorIndexClient,
) -> None:
    index_name = unique_test_vector_index_name()
    response = vector_index_client.delete_index(index_name)
    assert isinstance(response, DeleteIndex.Error)
    assert response.error_code == MomentoErrorCode.NOT_FOUND_ERROR


def test_delete_index_returns_error_for_bad_name(
    vector_index_client: PreviewVectorIndexClient,
) -> None:
    for bad_name, reason in [("", "not be empty"), (None, "be a string"), (1, "be a string")]:
        response = vector_index_client.delete_index(bad_name)  # type: ignore[arg-type]
        assert isinstance(response, DeleteIndex.Error)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
        assert response.inner_exception.message == f"Vector index name must {reason}"


# TODO: Add test for bad token when creating index
# async def test_delete_cache_throws_authentication_exception_for_bad_token(
#     bad_token_credential_provider: CredentialProvider, configuration: Configuration, default_ttl_seconds: timedelta
# ) -> None:
#     async with CacheClientAsync(configuration, bad_token_credential_provider, default_ttl_seconds) as client_async:
#         response = await client_async.delete_cache(uuid_str())
#         assert isinstance(response, DeleteCache.Error)
#         assert response.error_code == MomentoErrorCode.AUTHENTICATION_ERROR


# List indexes
def test_list_indexes_succeeds(vector_index_client: PreviewVectorIndexClient) -> None:
    index_name = unique_test_vector_index_name()

    initial_response = vector_index_client.list_indexes()
    assert isinstance(initial_response, ListIndexes.Success)

    index_names = [index_info.name for index_info in initial_response.indexes]
    assert index_name not in index_names

    try:
        response = vector_index_client.create_index(index_name, num_dimensions=1)
        assert isinstance(response, CreateIndex.Success)

        list_cache_resp = vector_index_client.list_indexes()
        assert isinstance(list_cache_resp, ListIndexes.Success)

        index_names = [index_info.name for index_info in list_cache_resp.indexes]
        assert index_name in index_names
    finally:
        delete_response = vector_index_client.delete_index(index_name)
        assert isinstance(delete_response, DeleteIndex.Success)


# TODO: Add test for bad token when creating index
# async def test_list_caches_throws_authentication_exception_for_bad_token(
#     bad_token_credential_provider: CredentialProvider, configuration: Configuration, default_ttl_seconds: timedelta
# ) -> None:
#     async with CacheClientAsync(configuration, bad_token_credential_provider, default_ttl_seconds) as client_async:
#         response = await client_async.list_caches()
#         assert isinstance(response, ListCaches.Error)
#         assert response.error_code == MomentoErrorCode.AUTHENTICATION_ERROR

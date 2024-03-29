import asyncio
from datetime import timedelta

from momento import (
    CacheClientAsync,
    Configurations,
    CredentialProvider,
    PreviewVectorIndexClientAsync,
    TopicClientAsync,
    TopicConfigurations,
    VectorIndexConfigurations,
)
from momento.requests.vector_index import ALL_METADATA, Field, Item
from momento.requests.vector_index import filters as F
from momento.responses import (
    CacheDelete,
    CacheGet,
    CacheSet,
    CreateCache,
    DeleteCache,
    ListCaches,
    TopicPublish,
    TopicSubscribe,
    TopicSubscriptionItem,
)
from momento.responses.vector_index import (
    CountItems,
    CreateIndex,
    DeleteIndex,
    DeleteItemBatch,
    GetItemBatch,
    GetItemMetadataBatch,
    ListIndexes,
    Search,
    SearchAndFetchVectors,
    UpsertItemBatch,
)


def example_API_CredentialProviderFromEnvVar():
    CredentialProvider.from_environment_variable("MOMENTO_API_KEY")


# end example


async def example_API_InstantiateCacheClient():
    await CacheClientAsync.create(
        Configurations.Laptop.latest(),
        CredentialProvider.from_environment_variable("MOMENTO_API_KEY"),
        timedelta(seconds=60),
    )


# end example


async def example_API_CreateCache(cache_client: CacheClientAsync):
    create_cache_response = await cache_client.create_cache("test-cache")
    match create_cache_response:
        case CreateCache.Success():
            print("Cache 'test-cache' created")
        case CreateCache.CacheAlreadyExists():
            print("Cache 'test-cache' already exists.")
        case CreateCache.Error() as error:
            print(f"An error occurred while attempting to create cache 'test-cache': {error.message}")


# end example


async def example_API_DeleteCache(cache_client: CacheClientAsync):
    delete_cache_response = await cache_client.delete_cache("test-cache")
    match delete_cache_response:
        case DeleteCache.Success():
            print("Cache 'test-cache' deleted")
        case DeleteCache.Error() as error:
            raise Exception(f"An error occurred while attempting to delete 'test-cache': {error.message}")


# end example


async def example_API_ListCaches(cache_client: CacheClientAsync):
    print("Listing caches:")
    list_caches_response = await cache_client.list_caches()
    match list_caches_response:
        case ListCaches.Success() as success:
            for cache_info in success.caches:
                print(f"- {cache_info.name!r}")
        case ListCaches.Error() as error:
            raise Exception(f"An error occurred while attempting to list caches: {error.message}")


# end example


async def example_API_Set(cache_client: CacheClientAsync):
    set_response = await cache_client.set("test-cache", "test-key", "test-value")
    match set_response:
        case CacheSet.Success():
            print("Key 'test-key' stored successfully")
        case CacheSet.Error() as error:
            raise Exception(
                f"An error occurred while attempting to store key 'test-key' in cache 'test-cache': {error.message}"
            )


# end example


async def example_API_Get(cache_client: CacheClientAsync):
    get_response = await cache_client.get("test-cache", "test-key")
    match get_response:
        case CacheGet.Hit() as hit:
            print(f"Retrieved value for key 'test-key': {hit.value_string}")
        case CacheGet.Miss():
            print("Key 'test-key' was not found in cache 'test-cache'")
        case CacheGet.Error() as error:
            raise Exception(
                f"An error occurred while attempting to get key 'test-key' from cache 'test-cache': {error.message}"
            )


# end example


async def example_API_Delete(cache_client: CacheClientAsync):
    delete_response = await cache_client.delete("test-cache", "test-key")
    match delete_response:
        case CacheDelete.Success():
            print("Key 'test-key' deleted successfully")
        case CacheDelete.Error() as error:
            raise Exception(
                f"An error occurred while attempting to delete key 'test-key' from cache 'test-cache': {error.message}"
            )


# end example


async def example_API_InstantiateTopicClient():
    TopicClientAsync(
        TopicConfigurations.Default.latest(), CredentialProvider.from_environment_variable("MOMENTO_API_KEY")
    )


# end example


async def example_API_TopicSubscribe(topic_client: TopicClientAsync):
    response = await topic_client.subscribe("cache", "my_topic")
    match response:
        case TopicSubscribe.Error() as error:
            print(f"Error subscribing to topic: {error.message}")
        case TopicSubscribe.SubscriptionAsync() as subscription:
            await topic_client.publish("cache", "my_topic", "my_value")
            async for item in subscription:
                match item:
                    case TopicSubscriptionItem.Text():
                        print(f"Received message as string: {item.value}")
                        return
                    case TopicSubscriptionItem.Binary():
                        print(f"Received message as bytes: {item.value!r}")
                        return
                    case TopicSubscriptionItem.Error():
                        print(f"Error with received message: {item.inner_exception.message}")
                        return


# end example


async def example_API_TopicPublish(topic_client: TopicClientAsync):
    response = await topic_client.publish("cache", "my_topic", "my_value")
    match response:
        case TopicPublish.Success():
            print("Successfully published a message")
        case TopicPublish.Error() as error:
            print(f"Error publishing a message: {error.message}")


# end example


async def example_API_InstantiateVectorClient():
    PreviewVectorIndexClientAsync(
        VectorIndexConfigurations.Default.latest(), CredentialProvider.from_environment_variable("MOMENTO_API_KEY")
    )


# end example


async def example_API_CreateIndex(vector_client: PreviewVectorIndexClientAsync):
    response = await vector_client.create_index("test-index", 2)
    match response:
        case CreateIndex.Success():
            print("Index 'test-index' created")
        case CreateIndex.IndexAlreadyExists():
            print("Index 'test-index' already exists")
        case CreateIndex.Error() as error:
            print(f"Error creating index 'test-index': {error.message}")


# end example


async def example_API_ListIndexes(vector_client: PreviewVectorIndexClientAsync):
    response = await vector_client.list_indexes()
    match response:
        case ListIndexes.Success() as success:
            print(f"Indexes:\n{success.indexes}")
        case CreateIndex.Error() as error:
            print(f"Error listing indexes: {error.message}")


# end example


async def example_API_DeleteIndex(vector_client: PreviewVectorIndexClientAsync):
    response = await vector_client.delete_index("test-index")
    match response:
        case DeleteIndex.Success():
            print("Index 'test-index' deleted")
        case DeleteIndex.Error() as error:
            print(f"Error deleting index 'test-index': {error.message}")


# end example


async def example_API_CountItems(vector_client: PreviewVectorIndexClientAsync):
    response = await vector_client.count_items("test-index")
    match response:
        case CountItems.Success() as success:
            print(f"Found {success.item_count} items")
        case CountItems.Error() as error:
            print(f"Error counting items in index 'test-index': {error.message}")


# end example


async def example_API_UpsertItemBatch(vector_client: PreviewVectorIndexClientAsync):
    response = await vector_client.upsert_item_batch(
        "test-index",
        [
            Item(id="example_item_1", vector=[1.0, 2.0], metadata={"key1": "value1"}),
            Item(id="example_item_2", vector=[3.0, 4.0], metadata={"key2": "value2"}),
        ],
    )
    match response:
        case UpsertItemBatch.Success():
            print("Successfully added items to index 'test-index'")
        case UpsertItemBatch.Error() as error:
            print(f"Error adding items to index 'test-index': {error.message}")


# end example


async def example_API_DeleteItemBatch(vector_client: PreviewVectorIndexClientAsync):
    response = await vector_client.delete_item_batch("test-index", ["example_item_1", "example_item_2"])
    match response:
        case DeleteItemBatch.Success():
            print("Successfully deleted items from index 'test-index'")
        case DeleteItemBatch.Error() as error:
            print(f"Error deleting items from index 'test-index': {error.message}")


# end example


async def example_API_GetItemBatch(vector_client: PreviewVectorIndexClientAsync):
    response = await vector_client.get_item_batch("test-index", ["example_item_1", "example_item_2"])
    match response:
        case GetItemBatch.Success() as success:
            print(f"Found {len(success.values)} items")
        case GetItemBatch.Error() as error:
            print(f"Error getting items from index 'test-index': {error.message}")


# end example


async def example_API_GetItemMetadataBatch(vector_client: PreviewVectorIndexClientAsync):
    response = await vector_client.get_item_metadata_batch("test-index", ["example_item_1", "example_item_2"])
    match response:
        case GetItemMetadataBatch.Success() as success:
            print(f"Found metadata for {len(success.values)} items")
        case GetItemMetadataBatch.Error() as error:
            print(f"Error getting item metadata from index 'test-index': {error.message}")


# end example


async def example_API_Search(vector_client: PreviewVectorIndexClientAsync):
    response = await vector_client.search("test-index", [1.0, 2.0], top_k=3, metadata_fields=ALL_METADATA)
    match response:
        case Search.Success() as success:
            print(f"Found {len(success.hits)} matches")
        case Search.Error() as error:
            print(f"Error searching index 'test-index': {error.message}")


# end example


async def example_API_SearchAndFetchVectors(vector_client: PreviewVectorIndexClientAsync):
    response = await vector_client.search_and_fetch_vectors(
        "test-index", [1.0, 2.0], top_k=3, metadata_fields=ALL_METADATA
    )
    match response:
        case SearchAndFetchVectors.Success() as success:
            print(f"Found {len(success.hits)} matches")
        case SearchAndFetchVectors.Error() as error:
            print(f"Error searching index 'test-index': {error.message}")


# end example


def example_API_FilterExpressionOverview() -> None:
    # For convenience, the filter expressions classes can accessed with filters module:
    # from momento.requests.vector_index import filters as F
    #
    # You can use the Field class to create a more idiomatic filter expression by using the
    # overloaded comparison operators:
    # from momento.requests.vector_index import Field
    #
    # Below we demonstrate both approaches to creating filter expressions.

    # Is the movie titled "The Matrix"?
    F.Equals("movie_title", "The Matrix")
    Field("movie_title") == "The Matrix"

    # Is the movie not titled "The Matrix"?
    F.Not(F.Equals("movie_title", "The Matrix"))
    Field("movie_title") != "The Matrix"

    # Was the movie released in 1999?
    F.Equals("year", 1999)
    Field("year") == 1999

    # Did the movie gross 463.5 million dollars?
    F.Equals("gross_revenue_millions", 463.5)
    Field("gross_revenue_millions") == 463.5

    # Was the movie in theaters?
    F.Equals("in_theaters", True)
    Field("in_theaters")

    # Was the movie released after 1990?
    F.GreaterThan("year", 1990)
    Field("year") > 1990

    # Was the movie released in or after 2020?
    F.GreaterThanOrEqual("year", 2020)
    Field("year") >= 2020

    # Was the movie released before 2000?
    F.LessThan("year", 2000)
    Field("year") < 2000

    # Was the movie released in or before 2000?
    F.LessThanOrEqual("year", 2000)
    Field("year") <= 2000

    # Was "Keanu Reeves" one of the actors?
    F.ListContains("actors", "Keanu Reeves")
    Field("actors").list_contains("Keanu Reeves")

    # Is the ID one of the following?
    F.IdInSet(["tt0133093", "tt0234215", "tt0242653"])

    # Was the movie directed by "Lana Wachowski" and released after 2000?
    F.And(F.ListContains("directors", "Lana Wachowski"), F.GreaterThan("year", 2000))
    Field("directors").list_contains("Lana Wachowski") & (Field("year") > 2000)

    # Was the movie directed by "Lana Wachowski" or released after 2000?
    F.Or(F.ListContains("directors", "Lana Wachowski"), F.GreaterThan("year", 2000))
    Field("directors").list_contains("Lana Wachowski") | (Field("year") > 2000)

    # Was "Keanu Reeves" not one of the actors?
    F.Not(F.ListContains("actors", "Keanu Reeves"))


# end example


async def main():
    example_API_CredentialProviderFromEnvVar()

    await example_API_InstantiateCacheClient()
    cache_client = await CacheClientAsync.create(
        Configurations.Laptop.latest(),
        CredentialProvider.from_environment_variable("MOMENTO_API_KEY"),
        timedelta(seconds=60),
    )

    await example_API_CreateCache(cache_client)
    await example_API_DeleteCache(cache_client)
    await example_API_CreateCache(cache_client)
    await example_API_ListCaches(cache_client)

    await example_API_Set(cache_client)
    await example_API_Get(cache_client)
    await example_API_Delete(cache_client)

    await example_API_DeleteCache(cache_client)

    topic_client = TopicClientAsync(
        TopicConfigurations.Default.latest(), CredentialProvider.from_environment_variable("MOMENTO_API_KEY")
    )
    await example_API_InstantiateTopicClient()
    await example_API_TopicPublish(topic_client)
    await example_API_TopicSubscribe(topic_client)
    await topic_client.close()

    vector_client = PreviewVectorIndexClientAsync(
        VectorIndexConfigurations.Default.latest(), CredentialProvider.from_environment_variable("MOMENTO_API_KEY")
    )
    await example_API_InstantiateVectorClient()
    await example_API_CreateIndex(vector_client)
    await example_API_ListIndexes(vector_client)
    await example_API_CountItems(vector_client)
    await example_API_UpsertItemBatch(vector_client)
    await example_API_GetItemBatch(vector_client)
    await example_API_GetItemMetadataBatch(vector_client)
    await example_API_Search(vector_client)
    await example_API_SearchAndFetchVectors(vector_client)
    example_API_FilterExpressionOverview()
    await example_API_DeleteItemBatch(vector_client)
    await example_API_DeleteIndex(vector_client)


if __name__ == "__main__":
    asyncio.run(main())

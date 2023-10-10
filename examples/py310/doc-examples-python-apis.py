import asyncio
from datetime import timedelta

from momento import (
    CacheClientAsync,
    Configurations,
    CredentialProvider,
    TopicClientAsync,
    TopicConfigurations, PreviewVectorIndexClientAsync, VectorIndexConfigurations,
)
from momento.requests.vector_index import Item, ALL_METADATA
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
from momento.responses.vector_index import CreateIndex, ListIndexes, DeleteIndex, UpsertItemBatch, DeleteItemBatch, \
    Search


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
            print(f"Indexes:\n{success.index_names}")
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

async def example_API_UpsertItemBatch(vector_client: PreviewVectorIndexClientAsync):
    response = await vector_client.upsert_item_batch('test-index', [
        Item(id="example_item_1", vector=[1.0, 2.0], metadata={"key1": "value1"}),
        Item(id="example_item_2", vector=[3.0, 4.0], metadata={"key2": "value2"}),
    ])
    match response:
        case UpsertItemBatch.Success():
            print("Successfully added items to index 'test-index'")
        case UpsertItemBatch.Error() as error:
            print(f"Error adding items to index 'test-index': {error.message}")


# end example

async def example_API_DeleteItemBatch(vector_client: PreviewVectorIndexClientAsync):
    response = await vector_client.delete_item_batch('test-index', ['example_item_1', 'example_item_2'])
    match response:
        case DeleteItemBatch.Success():
            print("Successfully deleted items from index 'test-index'")
        case DeleteItemBatch.Error() as error:
            print(f"Error deleting items from index 'test-index': {error.message}")


# end example

async def example_API_Search(vector_client: PreviewVectorIndexClientAsync):
    response = await vector_client.search('test-index', [1.0, 2.0], top_k=3, metadata_fields=ALL_METADATA)
    match response:
        case Search.Success() as success:
            print(f"Found {len(success.hits)} matches")
        case Search.Error() as error:
            print(f"Error searching index 'test-index': {error.message}")


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

    vector_client = PreviewVectorIndexClientAsync(VectorIndexConfigurations.Default.latest(),
                                                  CredentialProvider.from_environment_variable("MOMENTO_API_KEY"))
    await example_API_InstantiateVectorClient()
    await example_API_CreateIndex(vector_client)
    await example_API_ListIndexes(vector_client)
    await example_API_UpsertItemBatch(vector_client)
    await example_API_Search(vector_client)
    await example_API_DeleteItemBatch(vector_client)
    await example_API_DeleteIndex(vector_client)


if __name__ == "__main__":
    asyncio.run(main())

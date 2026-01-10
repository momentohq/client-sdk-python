import asyncio
from datetime import timedelta

from momento import (
    CacheClientAsync,
    Configurations,
    CredentialProvider,
    TopicClientAsync,
    TopicConfigurations,
)
from momento.auth.access_control.disposable_token_scope import DisposableTokenProps
from momento.auth.access_control.disposable_token_scopes import DisposableTokenScopes
from momento.auth_client_async import AuthClientAsync
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
    GenerateDisposableToken,
)
from momento.utilities import ExpiresIn


def retrieve_api_key_from_your_secrets_manager() -> str:
    # this is not a valid API key but conforms to the syntax requirements.
    return "eyJhcGlfa2V5IjogImV5SjBlWEFpT2lKS1YxUWlMQ0poYkdjaU9pSklVekkxTmlKOS5leUpwYzNNaU9pSlBibXhwYm1VZ1NsZFVJRUoxYVd4a1pYSWlMQ0pwWVhRaU9qRTJOemd6TURVNE1USXNJbVY0Y0NJNk5EZzJOVFV4TlRReE1pd2lZWFZrSWpvaUlpd2ljM1ZpSWpvaWFuSnZZMnRsZEVCbGVHRnRjR3hsTG1OdmJTSjkuOEl5OHE4NExzci1EM1lDb19IUDRkLXhqSGRUOFVDSXV2QVljeGhGTXl6OCIsICJlbmRwb2ludCI6ICJ0ZXN0Lm1vbWVudG9ocS5jb20ifQo="


def retrieve_api_key_v2_from_your_secrets_manager() -> str:
    # this is not a valid API key but conforms to the syntax requirements.
    return "eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJ0IjoiZyIsImp0aSI6InNvbWUtaWQifQ.GMr9nA6HE0ttB6llXct_2Sg5-fOKGFbJCdACZFgNbN1fhT6OPg_hVc8ThGzBrWC_RlsBpLA1nzqK3SOJDXYxAw"


def example_API_CredentialProviderFromEnvVar():
    CredentialProvider.from_environment_variable("V1_API_KEY")


# end example


def example_API_CredentialProviderFromEnvVarV2():
    CredentialProvider.from_environment_variables_v2(
        api_key_env_var="MOMENTO_API_KEY", endpoint_env_var="MOMENTO_ENDPOINT"
    )


# end example


def example_API_CredentialProviderFromEnvVarV2Default():
    CredentialProvider.from_environment_variables_v2()


# end example


def example_API_CredentialProviderFromApiKeyV2():
    api_key = retrieve_api_key_v2_from_your_secrets_manager()
    endpoint = "cell-4-us-west-2-1.prod.a.momentohq.com"
    CredentialProvider.from_api_key_v2(api_key, endpoint)


# end example


def example_API_CredentialProviderFromDisposableToken():
    auth_token = retrieve_api_key_from_your_secrets_manager()
    CredentialProvider.from_disposable_token(auth_token)


# end example


async def example_API_InstantiateCacheClient():
    await CacheClientAsync.create(
        Configurations.Laptop.latest(),
        CredentialProvider.from_environment_variables_v2(),
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
    TopicClientAsync(TopicConfigurations.Default.latest(), CredentialProvider.from_environment_variables_v2())


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


async def example_API_InstantiateAuthClient():
    AuthClientAsync(
        Configurations.Laptop.latest(),
        CredentialProvider.from_environment_variable("V1_API_KEY"),
    )


# end example


async def example_API_GenerateDisposableToken(auth_client: AuthClientAsync):
    response = await auth_client.generate_disposable_token(
        DisposableTokenScopes.topic_publish_subscribe("a-cache", "a-topic"),
        ExpiresIn.minutes(5),
        DisposableTokenProps(token_id="a-token-id"),
    )
    match response:
        case GenerateDisposableToken.Success():
            print("Successfully generated a disposable token")
        case GenerateDisposableToken.Error() as error:
            print(f"Error generating a disposable token: {error.message}")


# end example


async def main():
    example_API_CredentialProviderFromEnvVar()
    example_API_CredentialProviderFromEnvVarV2()
    example_API_CredentialProviderFromEnvVarV2Default()
    example_API_CredentialProviderFromApiKeyV2()
    example_API_CredentialProviderFromDisposableToken()

    await example_API_InstantiateCacheClient()
    cache_client = await CacheClientAsync.create(
        Configurations.Laptop.latest(),
        CredentialProvider.from_environment_variables_v2(),
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
        TopicConfigurations.Default.latest(), CredentialProvider.from_environment_variables_v2()
    )
    await example_API_InstantiateTopicClient()
    await example_API_TopicPublish(topic_client)
    await example_API_TopicSubscribe(topic_client)
    await topic_client.close()

    auth_client = AuthClientAsync(
        Configurations.Laptop.latest(),
        CredentialProvider.from_environment_variable("V1_API_KEY"),
    )
    await example_API_InstantiateAuthClient()
    await example_API_GenerateDisposableToken(auth_client)
    await auth_client.close()


if __name__ == "__main__":
    asyncio.run(main())

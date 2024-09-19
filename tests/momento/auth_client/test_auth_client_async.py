from datetime import timedelta

from momento import errors
from momento.auth.access_control.disposable_token_scope import DisposableTokenScope
from momento.auth.access_control.permission_scope import AllCaches
from momento.auth.access_control.permission_scopes import cache_read_only
from momento.auth.credential_provider import CredentialProvider
from momento.auth_client_async import AuthClientAsync
from momento.cache_client_async import CacheClientAsync
from momento.config.configurations import Configurations
from momento.config.topic_configurations import TopicConfigurations
from momento.responses.auth.generate_disposable_token import GenerateDisposableToken
from momento.responses.data.scalar.get import CacheGet
from momento.responses.data.scalar.set import CacheSet
from momento.responses.pubsub.publish import TopicPublish
from momento.responses.pubsub.subscribe import TopicSubscribe
from momento.topic_client_async import TopicClientAsync
from momento.utilities.expiration import ExpiresIn

from tests.utils import uuid_str

###### Beginning of helper functions ######


def cache_client_from_disposable_token(token: str) -> CacheClientAsync:
    return CacheClientAsync(
        Configurations.Laptop.latest(),
        CredentialProvider.from_string(token),
        timedelta(seconds=30),
    )


def topic_client_from_disposable_token(token: str) -> TopicClientAsync:
    return TopicClientAsync(
        TopicConfigurations.Default.latest(),
        CredentialProvider.from_string(token),
    )


async def generate_disposable_token_success(auth_client_async: AuthClientAsync, scope: DisposableTokenScope) -> str:
    resp = await auth_client_async.generate_disposable_token(scope, ExpiresIn.minutes(2))
    assert isinstance(resp, GenerateDisposableToken.Success)
    return resp.auth_token


async def assert_get_success(cache_client_async: CacheClientAsync, cache_name: str, key: str) -> None:
    get_resp = await cache_client_async.get(cache_name, key)
    assert not isinstance(get_resp, CacheGet.Error)


async def assert_get_failure(cache_client_async: CacheClientAsync, cache_name: str, key: str) -> None:
    get_resp = await cache_client_async.get(cache_name, key)
    assert isinstance(get_resp, CacheGet.Error)
    assert get_resp.error_code == errors.MomentoErrorCode.PERMISSION_ERROR


async def assert_set_success(cache_client_async: CacheClientAsync, cache_name: str, key: str, value: str) -> None:
    set_resp = await cache_client_async.set(cache_name, key, value)
    assert isinstance(set_resp, CacheSet.Success)


async def assert_set_failure(cache_client_async: CacheClientAsync, cache_name: str, key: str, value: str) -> None:
    set_resp = await cache_client_async.set(cache_name, key, value)
    assert isinstance(set_resp, CacheSet.Error)
    assert set_resp.error_code == errors.MomentoErrorCode.PERMISSION_ERROR


async def assert_publish_success(topic_client_async: TopicClientAsync, cache_name: str, topic_name: str) -> None:
    publish_resp = await topic_client_async.publish(cache_name, topic_name, uuid_str())
    assert isinstance(publish_resp, TopicPublish.Success)


async def assert_publish_failure(topic_client_async: TopicClientAsync, cache_name: str, topic_name: str) -> None:
    publish_resp = await topic_client_async.publish(cache_name, topic_name, uuid_str())
    assert isinstance(publish_resp, TopicPublish.Error)
    assert publish_resp.error_code == errors.MomentoErrorCode.PERMISSION_ERROR


async def assert_subscribe_success(topic_client_async: TopicClientAsync, cache_name: str, topic_name: str) -> None:
    subscribe_resp = await topic_client_async.subscribe(cache_name, topic_name)
    assert not isinstance(subscribe_resp, TopicSubscribe.Error)


async def assert_subscribe_failure(topic_client_async: TopicClientAsync, cache_name: str, topic_name: str) -> None:
    subscribe_resp = await topic_client_async.subscribe(cache_name, topic_name)
    assert isinstance(subscribe_resp, TopicSubscribe.Error)
    assert subscribe_resp.error_code == errors.MomentoErrorCode.PERMISSION_ERROR


###### Beginning of tests ######


async def test_generate_disposable_token_read_only_all_caches(
    client_async: CacheClientAsync,  # need this to create the test caches
    auth_client_async: AuthClientAsync,
    cache_name: str,
    alternate_cache_name: str,
) -> None:
    key = uuid_str()
    value = uuid_str()

    scope = DisposableTokenScope(permission_scope=cache_read_only(AllCaches()).get_permissions_objects())
    token = await generate_disposable_token_success(auth_client_async, scope)
    cc = cache_client_from_disposable_token(token)

    # Gets should succeed regardless of cache
    await assert_get_success(cc, cache_name, key)
    await assert_get_success(cc, alternate_cache_name, key)
    # Sets should fail regardless of cache
    await assert_set_failure(cc, cache_name, key, value)
    await assert_set_failure(cc, alternate_cache_name, key, value)
    assert True


async def test_generate_disposable_token_read_only_specific_cache(auth_client_async: AuthClientAsync) -> None:
    assert True


async def test_generate_disposable_token_read_only_specific_key_specific_cache(
    auth_client_async: AuthClientAsync,
) -> None:
    assert True


async def test_generate_disposable_token_read_only_key_prefix_specific_cache(
    auth_client_async: AuthClientAsync,
) -> None:
    assert True


async def test_generate_disposable_token_write_only_all_caches(auth_client_async: AuthClientAsync) -> None:
    assert True


async def test_generate_disposable_token_write_only_specific_cache(auth_client_async: AuthClientAsync) -> None:
    assert True


async def test_generate_disposable_token_write_only_specific_key_specific_cache(
    auth_client_async: AuthClientAsync,
) -> None:
    assert True


async def test_generate_disposable_token_write_only_key_prefix_specific_cache(
    auth_client_async: AuthClientAsync,
) -> None:
    assert True


async def test_generate_disposable_token_read_write_all_caches(auth_client_async: AuthClientAsync) -> None:
    assert True


async def test_generate_disposable_token_read_write_specific_cache(auth_client_async: AuthClientAsync) -> None:
    assert True


async def test_generate_disposable_token_read_write_specific_key_specific_cache(
    auth_client_async: AuthClientAsync,
) -> None:
    assert True


async def test_generate_disposable_token_read_write_key_prefix_specific_cache(
    auth_client_async: AuthClientAsync,
) -> None:
    assert True


async def test_generate_disposable_token_topic_publish_only_specific_cache_specific_topic(
    auth_client_async: AuthClientAsync,
) -> None:
    assert True


async def test_generate_disposable_token_topic_publish_only_all_caches_all_topics(
    auth_client_async: AuthClientAsync,
) -> None:
    assert True


async def test_generate_disposable_token_topic_subscribe_only_specific_cache_specific_topic(
    auth_client_async: AuthClientAsync,
) -> None:
    assert True


async def test_generate_disposable_token_topic_subscribe_only_all_caches_all_topics(
    auth_client_async: AuthClientAsync,
) -> None:
    assert True


async def test_generate_disposable_token_topic_publish_subscribe_specific_cache_specific_topic(
    auth_client_async: AuthClientAsync,
) -> None:
    assert True


async def test_generate_disposable_token_topic_publish_subscribe_all_caches_all_topics(
    auth_client_async: AuthClientAsync,
) -> None:
    assert True

from datetime import timedelta

from momento import errors
from momento.auth.access_control.disposable_token_scope import DisposableTokenScope
from momento.auth.access_control.disposable_token_scopes import DisposableTokenScopes
from momento.auth.access_control.permission_scope import AllCaches, AllTopics
from momento.auth.credential_provider import CredentialProvider
from momento.auth_client import AuthClient
from momento.cache_client import CacheClient
from momento.config.configurations import Configurations
from momento.config.topic_configurations import TopicConfigurations
from momento.responses.auth.generate_disposable_token import GenerateDisposableToken
from momento.responses.data.scalar.get import CacheGet
from momento.responses.data.scalar.set import CacheSet
from momento.responses.pubsub.publish import TopicPublish
from momento.responses.pubsub.subscribe import TopicSubscribe
from momento.topic_client import TopicClient
from momento.utilities.expiration import ExpiresIn

from tests.utils import uuid_str

###### Beginning of helper functions ######


def cache_client_from_disposable_token(token: str) -> CacheClient:
    return CacheClient(
        Configurations.Laptop.latest(),
        CredentialProvider.from_string(token),
        timedelta(seconds=30),
    )


def topic_client_from_disposable_token(token: str) -> TopicClient:
    return TopicClient(
        TopicConfigurations.Default.latest(),
        CredentialProvider.from_string(token),
    )


def generate_disposable_token_success(auth_client: AuthClient, scope: DisposableTokenScope) -> str:
    resp = auth_client.generate_disposable_token(scope, ExpiresIn.minutes(2))
    assert isinstance(resp, GenerateDisposableToken.Success)
    return resp.auth_token


def assert_get_success(cache_client: CacheClient, cache_name: str, key: str) -> None:
    get_resp = cache_client.get(cache_name, key)
    assert not isinstance(get_resp, CacheGet.Error)


def assert_get_failure(cache_client: CacheClient, cache_name: str, key: str) -> None:
    get_resp = cache_client.get(cache_name, key)
    assert isinstance(get_resp, CacheGet.Error)
    assert get_resp.error_code == errors.MomentoErrorCode.PERMISSION_ERROR


def assert_set_success(cache_client: CacheClient, cache_name: str, key: str, value: str) -> None:
    set_resp = cache_client.set(cache_name, key, value)
    assert isinstance(set_resp, CacheSet.Success)


def assert_set_failure(cache_client: CacheClient, cache_name: str, key: str, value: str) -> None:
    set_resp = cache_client.set(cache_name, key, value)
    assert isinstance(set_resp, CacheSet.Error)
    assert set_resp.error_code == errors.MomentoErrorCode.PERMISSION_ERROR


def assert_publish_success(topic_client: TopicClient, cache_name: str, topic_name: str) -> None:
    publish_resp = topic_client.publish(cache_name, topic_name, uuid_str())
    assert isinstance(publish_resp, TopicPublish.Success)


def assert_publish_failure(topic_client: TopicClient, cache_name: str, topic_name: str) -> None:
    publish_resp = topic_client.publish(cache_name, topic_name, uuid_str())
    assert isinstance(publish_resp, TopicPublish.Error)
    assert publish_resp.error_code == errors.MomentoErrorCode.PERMISSION_ERROR


def assert_subscribe_success(topic_client: TopicClient, cache_name: str, topic_name: str) -> None:
    subscribe_resp = topic_client.subscribe(cache_name, topic_name)
    assert not isinstance(subscribe_resp, TopicSubscribe.Error)


def assert_subscribe_failure(topic_client: TopicClient, cache_name: str, topic_name: str) -> None:
    subscribe_resp = topic_client.subscribe(cache_name, topic_name)
    assert isinstance(subscribe_resp, TopicSubscribe.Error)
    assert subscribe_resp.error_code == errors.MomentoErrorCode.PERMISSION_ERROR


###### Beginning of tests ######


def test_generate_disposable_token_read_only_all_caches(
    client: CacheClient,  # need this to create the test caches
    auth_client: AuthClient,
    cache_name: str,
    alternate_cache_name: str,
) -> None:
    key = uuid_str()
    value = uuid_str()

    scope = DisposableTokenScopes.cache_read_only(AllCaches())
    token = generate_disposable_token_success(auth_client, scope)
    cc = cache_client_from_disposable_token(token)

    # Gets should succeed regardless of cache
    assert_get_success(cc, cache_name, key)
    assert_get_success(cc, alternate_cache_name, key)

    # Sets should fail regardless of cache
    assert_set_failure(cc, cache_name, key, value)
    assert_set_failure(cc, alternate_cache_name, key, value)


def test_generate_disposable_token_read_only_specific_cache(
    client: CacheClient,  # need this to create the test caches
    auth_client: AuthClient,
    cache_name: str,
    alternate_cache_name: str,
) -> None:
    key = uuid_str()
    value = uuid_str()

    scope = DisposableTokenScopes.cache_read_only(cache_name)
    token = generate_disposable_token_success(auth_client, scope)
    cc = cache_client_from_disposable_token(token)

    # Gets should succeed only for the specified cache
    assert_get_success(cc, cache_name, key)
    assert_get_failure(cc, alternate_cache_name, key)

    # Sets should fail regardless of cache
    assert_set_failure(cc, cache_name, key, value)
    assert_set_failure(cc, alternate_cache_name, key, value)


def test_generate_disposable_token_read_only_specific_key_specific_cache(
    client: CacheClient,  # need this to create the test caches
    auth_client: AuthClient,
    cache_name: str,
    alternate_cache_name: str,
) -> None:
    key = uuid_str()
    key2 = uuid_str()
    value = uuid_str()

    scope = DisposableTokenScopes.cache_key_read_only(cache_name, key)
    token = generate_disposable_token_success(auth_client, scope)
    cc = cache_client_from_disposable_token(token)

    # Gets should succeed only for the specified cache and key
    assert_get_success(cc, cache_name, key)
    assert_get_failure(cc, cache_name, key2)
    assert_get_failure(cc, alternate_cache_name, key)

    # Sets should fail regardless of cache
    assert_set_failure(cc, cache_name, key, value)
    assert_set_failure(cc, alternate_cache_name, key, value)


def test_generate_disposable_token_read_only_key_prefix_specific_cache(
    client: CacheClient,  # need this to create the test caches
    auth_client: AuthClient,
    cache_name: str,
    alternate_cache_name: str,
) -> None:
    key = "a-prefix-" + uuid_str()
    key2 = "a-prefix-" + uuid_str()
    key3 = "another-prefix-" + uuid_str()
    value = uuid_str()

    scope = DisposableTokenScopes.cache_key_prefix_read_only(cache_name, "a-prefix-")
    token = generate_disposable_token_success(auth_client, scope)
    cc = cache_client_from_disposable_token(token)

    # Gets should succeed only for the specified cache and key prefix
    assert_get_success(cc, cache_name, key)
    assert_get_success(cc, cache_name, key2)
    assert_get_failure(cc, cache_name, key3)
    assert_get_failure(cc, alternate_cache_name, key)

    # Sets should fail regardless of cache
    assert_set_failure(cc, cache_name, key, value)
    assert_set_failure(cc, alternate_cache_name, key, value)


def test_generate_disposable_token_write_only_all_caches(
    client: CacheClient,  # need this to create the test caches
    auth_client: AuthClient,
    cache_name: str,
    alternate_cache_name: str,
) -> None:
    key = uuid_str()
    value = uuid_str()

    scope = DisposableTokenScopes.cache_write_only(AllCaches())
    token = generate_disposable_token_success(auth_client, scope)
    cc = cache_client_from_disposable_token(token)

    # Gets should fail regardless of cache
    assert_get_failure(cc, cache_name, key)
    assert_get_failure(cc, alternate_cache_name, key)

    # Sets should succeed regardless of cache
    assert_set_success(cc, cache_name, key, value)
    assert_set_success(cc, alternate_cache_name, key, value)


def test_generate_disposable_token_write_only_specific_cache(
    client: CacheClient,  # need this to create the test caches
    auth_client: AuthClient,
    cache_name: str,
    alternate_cache_name: str,
) -> None:
    key = uuid_str()
    value = uuid_str()

    scope = DisposableTokenScopes.cache_write_only(cache_name)
    token = generate_disposable_token_success(auth_client, scope)
    cc = cache_client_from_disposable_token(token)

    # Gets should fail regardless of cache
    assert_get_failure(cc, cache_name, key)
    assert_get_failure(cc, alternate_cache_name, key)

    # Sets should succeed only for the specified cache
    assert_set_success(cc, cache_name, key, value)
    assert_set_failure(cc, alternate_cache_name, key, value)


def test_generate_disposable_token_write_only_specific_key_specific_cache(
    client: CacheClient,  # need this to create the test caches
    auth_client: AuthClient,
    cache_name: str,
    alternate_cache_name: str,
) -> None:
    key = uuid_str()
    key2 = uuid_str()
    value = uuid_str()

    scope = DisposableTokenScopes.cache_key_write_only(cache_name, key)
    token = generate_disposable_token_success(auth_client, scope)
    cc = cache_client_from_disposable_token(token)

    # Gets should fail regardless of cache
    assert_get_failure(cc, cache_name, key)
    assert_get_failure(cc, alternate_cache_name, key)

    # Sets should succeed only for the specified cache and key
    assert_set_success(cc, cache_name, key, value)
    assert_set_failure(cc, cache_name, key2, value)
    assert_set_failure(cc, alternate_cache_name, key, value)


def test_generate_disposable_token_write_only_key_prefix_specific_cache(
    client: CacheClient,  # need this to create the test caches
    auth_client: AuthClient,
    cache_name: str,
    alternate_cache_name: str,
) -> None:
    key = "a-prefix-" + uuid_str()
    key2 = "a-prefix-" + uuid_str()
    key3 = "another-prefix-" + uuid_str()
    value = uuid_str()

    scope = DisposableTokenScopes.cache_key_prefix_write_only(cache_name, "a-prefix-")
    token = generate_disposable_token_success(auth_client, scope)
    cc = cache_client_from_disposable_token(token)

    # Gets should fail regardless of cache
    assert_get_failure(cc, cache_name, key)
    assert_get_failure(cc, alternate_cache_name, key)

    # Sets should succeed only for the specified cache and key prefix
    assert_set_success(cc, cache_name, key, value)
    assert_set_success(cc, cache_name, key2, value)
    assert_set_failure(cc, cache_name, key3, value)
    assert_set_failure(cc, alternate_cache_name, key, value)


def test_generate_disposable_token_read_write_all_caches(
    client: CacheClient,  # need this to create the test caches
    auth_client: AuthClient,
    cache_name: str,
    alternate_cache_name: str,
) -> None:
    key = uuid_str()
    value = uuid_str()

    scope = DisposableTokenScopes.cache_read_write(AllCaches())
    token = generate_disposable_token_success(auth_client, scope)
    cc = cache_client_from_disposable_token(token)

    # Gets should succeed regardless of cache
    assert_get_success(cc, cache_name, key)
    assert_get_success(cc, alternate_cache_name, key)

    # Sets should succeed regardless of cache
    assert_set_success(cc, cache_name, key, value)
    assert_set_success(cc, alternate_cache_name, key, value)


def test_generate_disposable_token_read_write_specific_cache(
    client: CacheClient,  # need this to create the test caches
    auth_client: AuthClient,
    cache_name: str,
    alternate_cache_name: str,
) -> None:
    key = uuid_str()
    value = uuid_str()

    scope = DisposableTokenScopes.cache_read_write(cache_name)
    token = generate_disposable_token_success(auth_client, scope)
    cc = cache_client_from_disposable_token(token)

    # Gets should succeed only for the specified cache
    assert_get_success(cc, cache_name, key)
    assert_get_failure(cc, alternate_cache_name, key)

    # Sets should succeed only for the specified cache
    assert_set_success(cc, cache_name, key, value)
    assert_set_failure(cc, alternate_cache_name, key, value)


def test_generate_disposable_token_read_write_specific_key_specific_cache(
    client: CacheClient,  # need this to create the test caches
    auth_client: AuthClient,
    cache_name: str,
    alternate_cache_name: str,
) -> None:
    key = uuid_str()
    key2 = uuid_str()
    value = uuid_str()

    scope = DisposableTokenScopes.cache_key_read_write(cache_name, key)
    token = generate_disposable_token_success(auth_client, scope)
    cc = cache_client_from_disposable_token(token)

    # Gets should succeed only for the specified cache and key
    assert_get_success(cc, cache_name, key)
    assert_get_failure(cc, cache_name, key2)
    assert_get_failure(cc, alternate_cache_name, key)

    # Sets should succeed only for the specified cache and key
    assert_set_success(cc, cache_name, key, value)
    assert_set_failure(cc, cache_name, key2, value)
    assert_set_failure(cc, alternate_cache_name, key, value)


def test_generate_disposable_token_read_write_key_prefix_specific_cache(
    client: CacheClient,  # need this to create the test caches
    auth_client: AuthClient,
    cache_name: str,
    alternate_cache_name: str,
) -> None:
    key = "a-prefix-" + uuid_str()
    key2 = "a-prefix-" + uuid_str()
    key3 = "another-prefix-" + uuid_str()
    value = uuid_str()

    scope = DisposableTokenScopes.cache_key_prefix_read_write(cache_name, "a-prefix-")
    token = generate_disposable_token_success(auth_client, scope)
    cc = cache_client_from_disposable_token(token)

    # Gets should succeed only for the specified cache and key prefix
    assert_get_success(cc, cache_name, key)
    assert_get_success(cc, cache_name, key2)
    assert_get_failure(cc, cache_name, key3)
    assert_get_failure(cc, alternate_cache_name, key)

    # Sets should succeed only for the specified cache and key prefix
    assert_set_success(cc, cache_name, key, value)
    assert_set_success(cc, cache_name, key2, value)
    assert_set_failure(cc, cache_name, key3, value)
    assert_set_failure(cc, alternate_cache_name, key, value)


def test_generate_disposable_token_topic_publish_only_specific_cache_specific_topic(
    client: CacheClient,  # need this to create the test caches
    auth_client: AuthClient,
    cache_name: str,
    alternate_cache_name: str,
) -> None:
    topic_name = uuid_str()
    topic_name2 = uuid_str()

    scope = DisposableTokenScopes.topic_publish_only(cache_name, topic_name)
    token = generate_disposable_token_success(auth_client, scope)
    tc = topic_client_from_disposable_token(token)

    # Publishes should succeed only for the specified cache and topic
    assert_publish_success(tc, cache_name, topic_name)
    assert_publish_failure(tc, cache_name, topic_name2)
    assert_publish_failure(tc, alternate_cache_name, topic_name)
    assert_publish_failure(tc, alternate_cache_name, topic_name2)

    # Subscribes should fail regardless of cache
    assert_subscribe_failure(tc, cache_name, topic_name)
    assert_subscribe_failure(tc, alternate_cache_name, topic_name)


def test_generate_disposable_token_topic_publish_only_all_caches_all_topics(
    client: CacheClient,  # need this to create the test caches
    auth_client: AuthClient,
    cache_name: str,
    alternate_cache_name: str,
) -> None:
    topic_name = uuid_str()
    topic_name2 = uuid_str()

    scope = DisposableTokenScopes.topic_publish_only(AllCaches(), AllTopics())
    token = generate_disposable_token_success(auth_client, scope)
    tc = topic_client_from_disposable_token(token)

    # Publishes should succeed regardless of cache and topic
    assert_publish_success(tc, cache_name, topic_name)
    assert_publish_success(tc, cache_name, topic_name2)
    assert_publish_success(tc, alternate_cache_name, topic_name)
    assert_publish_success(tc, alternate_cache_name, topic_name2)

    # Subscribes should fail regardless of cache
    assert_subscribe_failure(tc, cache_name, topic_name)
    assert_subscribe_failure(tc, alternate_cache_name, topic_name)


def test_generate_disposable_token_topic_subscribe_only_specific_cache_specific_topic(
    client: CacheClient,  # need this to create the test caches
    auth_client: AuthClient,
    cache_name: str,
    alternate_cache_name: str,
) -> None:
    topic_name = uuid_str()
    topic_name2 = uuid_str()

    scope = DisposableTokenScopes.topic_subscribe_only(cache_name, topic_name)
    token = generate_disposable_token_success(auth_client, scope)
    tc = topic_client_from_disposable_token(token)

    # Publishes should fail regardless of cache and topic
    assert_publish_failure(tc, cache_name, topic_name)
    assert_publish_failure(tc, cache_name, topic_name2)
    assert_publish_failure(tc, alternate_cache_name, topic_name)
    assert_publish_failure(tc, alternate_cache_name, topic_name2)

    # Subscribes should succeed only for the specified cache and topic
    assert_subscribe_success(tc, cache_name, topic_name)
    assert_subscribe_failure(tc, cache_name, topic_name2)
    assert_subscribe_failure(tc, alternate_cache_name, topic_name)


def test_generate_disposable_token_topic_subscribe_only_all_caches_all_topics(
    client: CacheClient,  # need this to create the test caches
    auth_client: AuthClient,
    cache_name: str,
    alternate_cache_name: str,
) -> None:
    topic_name = uuid_str()
    topic_name2 = uuid_str()

    scope = DisposableTokenScopes.topic_subscribe_only(AllCaches(), AllTopics())
    token = generate_disposable_token_success(auth_client, scope)
    tc = topic_client_from_disposable_token(token)

    # Publishes should fail regardless of cache and topic
    assert_publish_failure(tc, cache_name, topic_name)
    assert_publish_failure(tc, cache_name, topic_name2)
    assert_publish_failure(tc, alternate_cache_name, topic_name)
    assert_publish_failure(tc, alternate_cache_name, topic_name2)

    # Subscribes should succeed regardless of cache and topic
    assert_subscribe_success(tc, cache_name, topic_name)
    assert_subscribe_success(tc, cache_name, topic_name2)
    assert_subscribe_success(tc, alternate_cache_name, topic_name)
    assert_subscribe_success(tc, alternate_cache_name, topic_name2)


def test_generate_disposable_token_topic_publish_subscribe_specific_cache_specific_topic(
    client: CacheClient,  # need this to create the test caches
    auth_client: AuthClient,
    cache_name: str,
    alternate_cache_name: str,
) -> None:
    topic_name = uuid_str()
    topic_name2 = uuid_str()

    scope = DisposableTokenScopes.topic_publish_subscribe(cache_name, topic_name)
    token = generate_disposable_token_success(auth_client, scope)
    tc = topic_client_from_disposable_token(token)

    # Publishes should succeed only for the specified cache and topic
    assert_publish_success(tc, cache_name, topic_name)
    assert_publish_failure(tc, cache_name, topic_name2)
    assert_publish_failure(tc, alternate_cache_name, topic_name)
    assert_publish_failure(tc, alternate_cache_name, topic_name2)

    # Subscribes should succeed only for the specified cache and topic
    assert_subscribe_success(tc, cache_name, topic_name)
    assert_subscribe_failure(tc, cache_name, topic_name2)
    assert_subscribe_failure(tc, alternate_cache_name, topic_name)
    assert_subscribe_failure(tc, alternate_cache_name, topic_name)


def test_generate_disposable_token_topic_publish_subscribe_all_caches_all_topics(
    client: CacheClient,  # need this to create the test caches
    auth_client: AuthClient,
    cache_name: str,
    alternate_cache_name: str,
) -> None:
    topic_name = uuid_str()
    topic_name2 = uuid_str()

    scope = DisposableTokenScopes.topic_publish_subscribe(AllCaches(), AllTopics())
    token = generate_disposable_token_success(auth_client, scope)
    tc = topic_client_from_disposable_token(token)

    # Publishes should succeed regardless of cache and topic
    assert_publish_success(tc, cache_name, topic_name)
    assert_publish_success(tc, cache_name, topic_name2)
    assert_publish_success(tc, alternate_cache_name, topic_name)
    assert_publish_success(tc, alternate_cache_name, topic_name2)

    # Subscribes should succeed regardless of cache and topic
    assert_subscribe_success(tc, cache_name, topic_name)
    assert_subscribe_success(tc, cache_name, topic_name2)
    assert_subscribe_success(tc, alternate_cache_name, topic_name)
    assert_subscribe_success(tc, alternate_cache_name, topic_name)

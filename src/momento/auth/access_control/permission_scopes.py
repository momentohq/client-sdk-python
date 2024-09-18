from typing import Union

from momento.auth.access_control.permission_scope import (
    AllCaches,
    AllTopics,
    CacheName,
    CachePermission,
    CacheRole,
    CacheSelector,
    Permission,
    TopicName,
    TopicPermission,
    TopicRole,
    TopicSelector,
)


def cache_read_write(cache: Union[AllCaches, CacheName, str]) -> Permission:
    return CachePermission(
        cache_selector=CacheSelector(cache=cache),
        role=CacheRole.READ_WRITE,
    )


def cache_read_only(cache: Union[AllCaches, CacheName, str]) -> Permission:
    return CachePermission(
        cache_selector=CacheSelector(cache=cache),
        role=CacheRole.READ_ONLY,
    )


def cache_write_only(cache: Union[AllCaches, CacheName, str]) -> Permission:
    return CachePermission(
        cache_selector=CacheSelector(cache=cache),
        role=CacheRole.WRITE_ONLY,
    )


def topic_publish_subscribe(
    cache: Union[AllCaches, CacheName, str], topic: Union[TopicName, AllTopics, str]
) -> Permission:
    return TopicPermission(
        cache_selector=CacheSelector(cache=cache),
        role=TopicRole.PUBLISH_SUBSCRIBE,
        topic_selector=TopicSelector(topic=topic),
    )


def topic_subscribe_only(
    cache: Union[AllCaches, CacheName, str], topic: Union[TopicName, AllTopics, str]
) -> Permission:
    return TopicPermission(
        cache_selector=CacheSelector(cache=cache),
        role=TopicRole.SUBSCRIBE_ONLY,
        topic_selector=TopicSelector(topic=topic),
    )


def topic_publish_only(cache: Union[AllCaches, CacheName, str], topic: Union[TopicName, AllTopics, str]) -> Permission:
    return TopicPermission(
        cache_selector=CacheSelector(cache=cache),
        role=TopicRole.PUBLISH_ONLY,
        topic_selector=TopicSelector(topic=topic),
    )

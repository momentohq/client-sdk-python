"""Permission Scopes.

Convenience methods for creating permission scopes.
"""
from typing import Union

from momento.auth.access_control.permission_scope import (
    AllCaches,
    AllTopics,
    CacheName,
    CachePermission,
    CacheRole,
    CacheSelector,
    Permissions,
    PermissionScope,
    TopicName,
    TopicPermission,
    TopicRole,
    TopicSelector,
)


class PermissionScopes:
    @staticmethod
    def cache_read_write(cache: Union[AllCaches, CacheName, str]) -> PermissionScope:
        scope = Permissions(
            permissions=[
                CachePermission(
                    cache_selector=CacheSelector(cache=cache),
                    role=CacheRole.READ_WRITE,
                )
            ]
        )
        return PermissionScope(permission_scope=scope)

    @staticmethod
    def cache_read_only(cache: Union[AllCaches, CacheName, str]) -> PermissionScope:
        scope = Permissions(
            permissions=[
                CachePermission(
                    cache_selector=CacheSelector(cache=cache),
                    role=CacheRole.READ_ONLY,
                )
            ]
        )
        return PermissionScope(permission_scope=scope)

    @staticmethod
    def cache_write_only(cache: Union[AllCaches, CacheName, str]) -> PermissionScope:
        scope = Permissions(
            permissions=[
                CachePermission(
                    cache_selector=CacheSelector(cache=cache),
                    role=CacheRole.WRITE_ONLY,
                )
            ]
        )
        return PermissionScope(permission_scope=scope)

    @staticmethod
    def topic_publish_subscribe(
        cache: Union[AllCaches, CacheName, str], topic: Union[TopicName, AllTopics, str]
    ) -> PermissionScope:
        scope = Permissions(
            permissions=[
                TopicPermission(
                    cache_selector=CacheSelector(cache=cache),
                    role=TopicRole.PUBLISH_SUBSCRIBE,
                    topic_selector=TopicSelector(topic=topic),
                )
            ]
        )
        return PermissionScope(permission_scope=scope)

    @staticmethod
    def topic_subscribe_only(
        cache: Union[AllCaches, CacheName, str], topic: Union[TopicName, AllTopics, str]
    ) -> PermissionScope:
        scope = Permissions(
            permissions=[
                TopicPermission(
                    cache_selector=CacheSelector(cache=cache),
                    role=TopicRole.SUBSCRIBE_ONLY,
                    topic_selector=TopicSelector(topic=topic),
                )
            ]
        )
        return PermissionScope(permission_scope=scope)

    @staticmethod
    def topic_publish_only(
        cache: Union[AllCaches, CacheName, str], topic: Union[TopicName, AllTopics, str]
    ) -> PermissionScope:
        scope = Permissions(
            permissions=[
                TopicPermission(
                    cache_selector=CacheSelector(cache=cache),
                    role=TopicRole.PUBLISH_ONLY,
                    topic_selector=TopicSelector(topic=topic),
                )
            ]
        )
        return PermissionScope(permission_scope=scope)
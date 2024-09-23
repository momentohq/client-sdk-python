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
        """Create permissions for read-write access to specified cache(s).

        Args:
            cache (Union[AllCaches, CacheName, str]): The cache(s) to grant permission to.

        Returns:
            DisposableTokenScope: A set of permissions to grant to a disposable token.
        """
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
        """Create permissions for read-only access to specified cache(s).

        Args:
            cache (Union[AllCaches, CacheName, str]): The cache(s) to grant permission to.

        Returns:
            DisposableTokenScope: A set of permissions to grant to a disposable token.
        """
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
        """Create permissions for write-only access to specified cache(s).

        Args:
            cache (Union[AllCaches, CacheName, str]): The cache(s) to grant permission to.

        Returns:
            DisposableTokenScope: A set of permissions to grant to a disposable token.
        """
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
        """Create permissions for publish-subscribe access to specified topic(s) and cache(s).

        Args:
            cache (Union[AllCaches, CacheName, str]): The cache(s) to grant permission to.
            topic (Union[TopicName, AllTopics, str]): The topic(s) to grant permission to.

        Returns:
            DisposableTokenScope: A set of permissions to grant to a disposable token.
        """
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
        """Create permissions for subscribe-only access to specified topic(s) and cache(s).

        Args:
            cache (Union[AllCaches, CacheName, str]): The cache(s) to grant permission to.
            topic (Union[TopicName, AllTopics, str]): The topic(s) to grant permission to.

        Returns:
            DisposableTokenScope: A set of permissions to grant to a disposable token.
        """
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
        """Create permissions for publish-only access to specified topic(s) and cache(s).

        Args:
            cache (Union[AllCaches, CacheName, str]): The cache(s) to grant permission to.
            topic (Union[TopicName, AllTopics, str]): The topic(s) to grant permission to.

        Returns:
            DisposableTokenScope: A set of permissions to grant to a disposable token.
        """
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

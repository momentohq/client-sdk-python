"""Disposable Token Scopes.

Convenience methods for creating disposable token scopes.
"""
from typing import Union

from momento.auth.access_control.disposable_token_scope import (
    CacheItemKey,
    CacheItemKeyPrefix,
    CacheItemSelector,
    DisposableTokenCachePermission,
    DisposableTokenCachePermissions,
    DisposableTokenScope,
)
from momento.auth.access_control.permission_scope import (
    AllCaches,
    AllTopics,
    CacheName,
    CachePermission,
    CacheRole,
    CacheSelector,
    Permissions,
    TopicName,
    TopicPermission,
    TopicRole,
    TopicSelector,
)


class DisposableTokenScopes:
    """Disposable Token Scopes.

    Convenience methods for creating permission scopes for disposable tokens.
    """

    @staticmethod
    def cache_key_read_write(
        cache: Union[AllCaches, CacheName, str], key: Union[CacheItemKey, str]
    ) -> DisposableTokenScope:
        """Create permissions for read-write access to a specific key in specified cache(s).

        Args:
            cache (Union[AllCaches, CacheName, str]): The cache(s) to grant permission to.
            key (Union[CacheItemKey, str]): The key to grant permission to.

        Returns:
            DisposableTokenScope: A set of permissions to grant to a disposable token.
        """
        _key = key if isinstance(key, CacheItemKey) else CacheItemKey(key)
        scope = DisposableTokenCachePermissions(
            [
                DisposableTokenCachePermission(
                    CacheSelector(cache),
                    CacheRole.READ_WRITE,
                    CacheItemSelector(_key),
                )
            ]
        )
        return DisposableTokenScope(scope)

    @staticmethod
    def cache_key_prefix_read_write(
        cache: Union[AllCaches, CacheName, str], key_prefix: Union[CacheItemKeyPrefix, str]
    ) -> DisposableTokenScope:
        """Create permissions for read-write access to keys that match a specific key prefix in specified cache(s).

        Args:
            cache (Union[AllCaches, CacheName, str]): The cache(s) to grant permission to.
            key_prefix (Union[CacheItemKey, str]): The key prefix to grant permission to.

        Returns:
            DisposableTokenScope: A set of permissions to grant to a disposable token.
        """
        _prefix = key_prefix if isinstance(key_prefix, CacheItemKeyPrefix) else CacheItemKeyPrefix(key_prefix)
        scope = DisposableTokenCachePermissions(
            [
                DisposableTokenCachePermission(
                    CacheSelector(cache),
                    CacheRole.READ_WRITE,
                    CacheItemSelector(_prefix),
                )
            ]
        )
        return DisposableTokenScope(scope)

    @staticmethod
    def cache_key_read_only(
        cache: Union[AllCaches, CacheName, str], key: Union[CacheItemKey, str]
    ) -> DisposableTokenScope:
        """Create permissions for read-only access to a specific key in specified cache(s).

        Args:
            cache (Union[AllCaches, CacheName, str]): The cache(s) to grant permission to.
            key (Union[CacheItemKey, str]): The key to grant permission to.

        Returns:
            DisposableTokenScope: A set of permissions to grant to a disposable token.
        """
        _key = key if isinstance(key, CacheItemKey) else CacheItemKey(key)
        scope = DisposableTokenCachePermissions(
            [
                DisposableTokenCachePermission(
                    CacheSelector(cache),
                    CacheRole.READ_ONLY,
                    CacheItemSelector(_key),
                )
            ]
        )
        return DisposableTokenScope(scope)

    @staticmethod
    def cache_key_prefix_read_only(
        cache: Union[AllCaches, CacheName, str], key_prefix: Union[CacheItemKeyPrefix, str]
    ) -> DisposableTokenScope:
        """Create permissions for read-only access to keys that match a specific key prefix in specified cache(s).

        Args:
            cache (Union[AllCaches, CacheName, str]): The cache(s) to grant permission to.
            key_prefix (Union[CacheItemKey, str]): The key prefix to grant permission to.

        Returns:
            DisposableTokenScope: A set of permissions to grant to a disposable token.
        """
        _prefix = key_prefix if isinstance(key_prefix, CacheItemKeyPrefix) else CacheItemKeyPrefix(key_prefix)
        scope = DisposableTokenCachePermissions(
            [
                DisposableTokenCachePermission(
                    CacheSelector(cache),
                    CacheRole.READ_ONLY,
                    CacheItemSelector(_prefix),
                )
            ]
        )
        return DisposableTokenScope(scope)

    @staticmethod
    def cache_key_write_only(
        cache: Union[AllCaches, CacheName, str], key: Union[CacheItemKey, str]
    ) -> DisposableTokenScope:
        """Create permissions for write-only access to a specific key in specified cache(s).

        Args:
            cache (Union[AllCaches, CacheName, str]): The cache(s) to grant permission to.
            key (Union[CacheItemKey, str]): The key to grant permission to.

        Returns:
            DisposableTokenScope: A set of permissions to grant to a disposable token.
        """
        _key = key if isinstance(key, CacheItemKey) else CacheItemKey(key)
        scope = DisposableTokenCachePermissions(
            [
                DisposableTokenCachePermission(
                    CacheSelector(cache),
                    CacheRole.WRITE_ONLY,
                    CacheItemSelector(_key),
                )
            ]
        )
        return DisposableTokenScope(scope)

    @staticmethod
    def cache_key_prefix_write_only(
        cache: Union[AllCaches, CacheName, str], key_prefix: Union[CacheItemKeyPrefix, str]
    ) -> DisposableTokenScope:
        """Create permissions for write-only access to keys that match a specific key prefix in specified cache(s).

        Args:
            cache (Union[AllCaches, CacheName, str]): The cache(s) to grant permission to.
            key_prefix (Union[CacheItemKey, str]): The key prefix to grant permission to.

        Returns:
            DisposableTokenScope: A set of permissions to grant to a disposable token.
        """
        _prefix = key_prefix if isinstance(key_prefix, CacheItemKeyPrefix) else CacheItemKeyPrefix(key_prefix)
        scope = DisposableTokenCachePermissions(
            [
                DisposableTokenCachePermission(
                    CacheSelector(cache),
                    CacheRole.WRITE_ONLY,
                    CacheItemSelector(_prefix),
                )
            ]
        )
        return DisposableTokenScope(scope)

    @staticmethod
    def cache_read_write(cache: Union[AllCaches, CacheName, str]) -> DisposableTokenScope:
        """Create permissions for read-write access to specified cache(s).

        Args:
            cache (Union[AllCaches, CacheName, str]): The cache(s) to grant permission to.

        Returns:
            DisposableTokenScope: A set of permissions to grant to a disposable token.
        """
        scope = Permissions(
            [
                CachePermission(
                    CacheSelector(cache),
                    CacheRole.READ_WRITE,
                )
            ]
        )
        return DisposableTokenScope(scope)

    @staticmethod
    def cache_read_only(cache: Union[AllCaches, CacheName, str]) -> DisposableTokenScope:
        """Create permissions for read-only access to specified cache(s).

        Args:
            cache (Union[AllCaches, CacheName, str]): The cache(s) to grant permission to.

        Returns:
            DisposableTokenScope: A set of permissions to grant to a disposable token.
        """
        scope = Permissions(
            [
                CachePermission(
                    CacheSelector(cache),
                    CacheRole.READ_ONLY,
                )
            ]
        )
        return DisposableTokenScope(scope)

    @staticmethod
    def cache_write_only(cache: Union[AllCaches, CacheName, str]) -> DisposableTokenScope:
        """Create permissions for write-only access to specified cache(s).

        Args:
            cache (Union[AllCaches, CacheName, str]): The cache(s) to grant permission to.

        Returns:
            DisposableTokenScope: A set of permissions to grant to a disposable token.
        """
        scope = Permissions(
            [
                CachePermission(
                    CacheSelector(cache),
                    CacheRole.WRITE_ONLY,
                )
            ]
        )
        return DisposableTokenScope(scope)

    @staticmethod
    def topic_publish_subscribe(
        cache: Union[AllCaches, CacheName, str], topic: Union[TopicName, AllTopics, str]
    ) -> DisposableTokenScope:
        """Create permissions for publish-subscribe access to specified topic(s) and cache(s).

        Args:
            cache (Union[AllCaches, CacheName, str]): The cache(s) to grant permission to.
            topic (Union[TopicName, AllTopics, str]): The topic(s) to grant permission to.

        Returns:
            DisposableTokenScope: A set of permissions to grant to a disposable token.
        """
        scope = Permissions(
            [
                TopicPermission(
                    TopicRole.PUBLISH_SUBSCRIBE,
                    CacheSelector(cache),
                    TopicSelector(topic),
                )
            ]
        )
        return DisposableTokenScope(scope)

    @staticmethod
    def topic_subscribe_only(
        cache: Union[AllCaches, CacheName, str], topic: Union[TopicName, AllTopics, str]
    ) -> DisposableTokenScope:
        """Create permissions for subscribe-only access to specified topic(s) and cache(s).

        Args:
            cache (Union[AllCaches, CacheName, str]): The cache(s) to grant permission to.
            topic (Union[TopicName, AllTopics, str]): The topic(s) to grant permission to.

        Returns:
            DisposableTokenScope: A set of permissions to grant to a disposable token.
        """
        scope = Permissions(
            [
                TopicPermission(
                    TopicRole.SUBSCRIBE_ONLY,
                    CacheSelector(cache),
                    TopicSelector(topic),
                )
            ]
        )
        return DisposableTokenScope(scope)

    @staticmethod
    def topic_publish_only(
        cache: Union[AllCaches, CacheName, str], topic: Union[TopicName, AllTopics, str]
    ) -> DisposableTokenScope:
        """Create permissions for publish-only access to specified topic(s) and cache(s).

        Args:
            cache (Union[AllCaches, CacheName, str]): The cache(s) to grant permission to.
            topic (Union[TopicName, AllTopics, str]): The topic(s) to grant permission to.

        Returns:
            DisposableTokenScope: A set of permissions to grant to a disposable token.
        """
        scope = Permissions(
            [
                TopicPermission(
                    TopicRole.PUBLISH_ONLY,
                    CacheSelector(cache),
                    TopicSelector(topic),
                )
            ]
        )
        return DisposableTokenScope(scope)

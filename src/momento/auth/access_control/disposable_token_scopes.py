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
    @staticmethod
    def cache_key_read_write(
        cache: Union[AllCaches, CacheName, str], key: Union[CacheItemKey, str]
    ) -> DisposableTokenScope:
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
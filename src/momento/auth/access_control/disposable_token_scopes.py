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
            permissions=[
                DisposableTokenCachePermission(
                    item=CacheItemSelector(cache_item=_key),
                    cache=CacheSelector(cache=cache),
                    role=CacheRole.READ_WRITE,
                )
            ]
        )
        return DisposableTokenScope(permission_scope=scope)

    @staticmethod
    def cache_key_prefix_read_write(
        cache: Union[AllCaches, CacheName, str], key_prefix: Union[CacheItemKeyPrefix, str]
    ) -> DisposableTokenScope:
        _prefix = key_prefix if isinstance(key_prefix, CacheItemKeyPrefix) else CacheItemKeyPrefix(key_prefix)
        scope = DisposableTokenCachePermissions(
            permissions=[
                DisposableTokenCachePermission(
                    item=CacheItemSelector(cache_item=_prefix),
                    cache=CacheSelector(cache=cache),
                    role=CacheRole.READ_WRITE,
                )
            ]
        )
        return DisposableTokenScope(permission_scope=scope)

    @staticmethod
    def cache_key_read_only(
        cache: Union[AllCaches, CacheName, str], key: Union[CacheItemKey, str]
    ) -> DisposableTokenScope:
        _key = key if isinstance(key, CacheItemKey) else CacheItemKey(key)
        scope = DisposableTokenCachePermissions(
            permissions=[
                DisposableTokenCachePermission(
                    item=CacheItemSelector(cache_item=_key),
                    cache=CacheSelector(cache=cache),
                    role=CacheRole.READ_ONLY,
                )
            ]
        )
        return DisposableTokenScope(permission_scope=scope)

    @staticmethod
    def cache_key_prefix_read_only(
        cache: Union[AllCaches, CacheName, str], key_prefix: Union[CacheItemKeyPrefix, str]
    ) -> DisposableTokenScope:
        _prefix = key_prefix if isinstance(key_prefix, CacheItemKeyPrefix) else CacheItemKeyPrefix(key_prefix)
        scope = DisposableTokenCachePermissions(
            permissions=[
                DisposableTokenCachePermission(
                    item=CacheItemSelector(cache_item=_prefix),
                    cache=CacheSelector(cache=cache),
                    role=CacheRole.READ_ONLY,
                )
            ]
        )
        return DisposableTokenScope(permission_scope=scope)

    @staticmethod
    def cache_key_write_only(
        cache: Union[AllCaches, CacheName, str], key: Union[CacheItemKey, str]
    ) -> DisposableTokenScope:
        _key = key if isinstance(key, CacheItemKey) else CacheItemKey(key)
        scope = DisposableTokenCachePermissions(
            permissions=[
                DisposableTokenCachePermission(
                    item=CacheItemSelector(cache_item=_key),
                    cache=CacheSelector(cache=cache),
                    role=CacheRole.WRITE_ONLY,
                )
            ]
        )
        return DisposableTokenScope(permission_scope=scope)

    @staticmethod
    def cache_key_prefix_write_only(
        cache: Union[AllCaches, CacheName, str], key_prefix: Union[CacheItemKeyPrefix, str]
    ) -> DisposableTokenScope:
        _prefix = key_prefix if isinstance(key_prefix, CacheItemKeyPrefix) else CacheItemKeyPrefix(key_prefix)
        scope = DisposableTokenCachePermissions(
            permissions=[
                DisposableTokenCachePermission(
                    item=CacheItemSelector(cache_item=_prefix),
                    cache=CacheSelector(cache=cache),
                    role=CacheRole.WRITE_ONLY,
                )
            ]
        )
        return DisposableTokenScope(permission_scope=scope)

    @staticmethod
    def cache_read_write(cache: Union[AllCaches, CacheName, str]) -> DisposableTokenScope:
        scope = Permissions(
            permissions=[
                CachePermission(
                    cache_selector=CacheSelector(cache=cache),
                    role=CacheRole.READ_WRITE,
                )
            ]
        )
        return DisposableTokenScope(permission_scope=scope)

    @staticmethod
    def cache_read_only(cache: Union[AllCaches, CacheName, str]) -> DisposableTokenScope:
        scope = Permissions(
            permissions=[
                CachePermission(
                    cache_selector=CacheSelector(cache=cache),
                    role=CacheRole.READ_ONLY,
                )
            ]
        )
        return DisposableTokenScope(permission_scope=scope)

    @staticmethod
    def cache_write_only(cache: Union[AllCaches, CacheName, str]) -> DisposableTokenScope:
        scope = Permissions(
            permissions=[
                CachePermission(
                    cache_selector=CacheSelector(cache=cache),
                    role=CacheRole.WRITE_ONLY,
                )
            ]
        )
        return DisposableTokenScope(permission_scope=scope)

    @staticmethod
    def topic_publish_subscribe(
        cache: Union[AllCaches, CacheName, str], topic: Union[TopicName, AllTopics, str]
    ) -> DisposableTokenScope:
        scope = Permissions(
            permissions=[
                TopicPermission(
                    cache_selector=CacheSelector(cache=cache),
                    role=TopicRole.PUBLISH_SUBSCRIBE,
                    topic_selector=TopicSelector(topic=topic),
                )
            ]
        )
        return DisposableTokenScope(permission_scope=scope)

    @staticmethod
    def topic_subscribe_only(
        cache: Union[AllCaches, CacheName, str], topic: Union[TopicName, AllTopics, str]
    ) -> DisposableTokenScope:
        scope = Permissions(
            permissions=[
                TopicPermission(
                    cache_selector=CacheSelector(cache=cache),
                    role=TopicRole.SUBSCRIBE_ONLY,
                    topic_selector=TopicSelector(topic=topic),
                )
            ]
        )
        return DisposableTokenScope(permission_scope=scope)

    @staticmethod
    def topic_publish_only(
        cache: Union[AllCaches, CacheName, str], topic: Union[TopicName, AllTopics, str]
    ) -> DisposableTokenScope:
        scope = Permissions(
            permissions=[
                TopicPermission(
                    cache_selector=CacheSelector(cache=cache),
                    role=TopicRole.PUBLISH_ONLY,
                    topic_selector=TopicSelector(topic=topic),
                )
            ]
        )
        return DisposableTokenScope(permission_scope=scope)

from typing import Union

from momento.auth.access_control.disposable_token_scope import (
    CacheItemKey,
    CacheItemKeyPrefix,
    CacheItemSelector,
    DisposableTokenCachePermission,
    DisposableTokenCachePermissions,
    DisposableTokenScope,
)
from momento.auth.access_control.permission_scope import AllCaches, CacheName, CacheRole, CacheSelector


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


def cache_key_read_only(cache: Union[AllCaches, CacheName, str], key: Union[CacheItemKey, str]) -> DisposableTokenScope:
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

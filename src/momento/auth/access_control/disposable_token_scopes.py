from momento.auth.access_control.disposable_token_scope import (
    CacheItemKey,
    CacheItemKeyPrefix,
    CacheItemSelector,
    DisposableTokenCachePermission,
    DisposableTokenCachePermissions,
    DisposableTokenScope,
)
from momento.auth.access_control.permission_scope import CacheRole, CacheSelector


def cache_key_read_write(cache_selector: CacheSelector, key: CacheItemKey) -> DisposableTokenScope:
    permissions = DisposableTokenCachePermission(
        item=CacheItemSelector(key),
        cache=cache_selector,
        role=CacheRole.READ_WRITE,
    )
    return DisposableTokenScope(permission_scope=DisposableTokenCachePermissions([permissions]))

def cache_key_prefix_read_write(cache_selector: CacheSelector, key_prefix: CacheItemKeyPrefix) -> DisposableTokenScope:
    permissions = DisposableTokenCachePermission(
        item=CacheItemSelector(key_prefix),
        cache=cache_selector,
        role=CacheRole.READ_WRITE,
    )
    return DisposableTokenScope(permission_scope=DisposableTokenCachePermissions([permissions]))

def cache_key_read_only(cache_selector: CacheSelector, key: str) -> DisposableTokenScope:
    permissions = DisposableTokenCachePermission(
        item=CacheItemSelector(key),
        cache=cache_selector,
        role=CacheRole.READ_ONLY,
    )
    return DisposableTokenScope(permission_scope=DisposableTokenCachePermissions([permissions]))

def cache_key_prefix_read_only(cache_selector: CacheSelector, key_prefix: str) -> DisposableTokenScope:
    permissions = DisposableTokenCachePermission(
        item=CacheItemSelector(key_prefix),
        cache=cache_selector,
        role=CacheRole.READ_ONLY,
    )
    return DisposableTokenScope(permission_scope=DisposableTokenCachePermissions([permissions]))

def cache_key_write_only(cache_selector: CacheSelector, key: str) -> DisposableTokenScope:
    permissions = DisposableTokenCachePermission(
        item=CacheItemSelector(key),
        cache=cache_selector,
        role=CacheRole.WRITE_ONLY,
    )
    return DisposableTokenScope(permission_scope=DisposableTokenCachePermissions([permissions]))

def cache_key_prefix_write_only(cache_selector: CacheSelector, key_prefix: str) -> DisposableTokenScope:
    permissions = DisposableTokenCachePermission(
        item=CacheItemSelector(key_prefix),
        cache=cache_selector,
        role=CacheRole.WRITE_ONLY,
    )
    return DisposableTokenScope(permission_scope=DisposableTokenCachePermissions([permissions]))

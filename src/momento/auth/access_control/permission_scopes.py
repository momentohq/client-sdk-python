from momento.auth.access_control.permission_scope import (
    CachePermission,
    CacheRole,
    CacheSelector,
    Permissions,
    PermissionScope,
    TopicPermission,
    TopicRole,
    TopicSelector,
)


def cache_read_write(cache_selector: CacheSelector) -> PermissionScope:
    permissions = CachePermission(
        cache_selector=cache_selector,
        role=CacheRole.READ_WRITE,
    )
    return PermissionScope(permission_scope=Permissions([permissions]))

def cache_read_only(cache_selector: CacheSelector) -> PermissionScope:
    permissions = CachePermission(
        cache_selector=cache_selector,
        role=CacheRole.READ_ONLY,
    )
    return PermissionScope(permission_scope=Permissions([permissions]))

def cache_write_only(cache_selector: CacheSelector) -> PermissionScope:
    permissions = CachePermission(
        cache_selector=cache_selector,
        role=CacheRole.WRITE_ONLY,
    )
    return PermissionScope(permission_scope=Permissions([permissions]))

def topic_publish_subscribe(cache_selector: CacheSelector, topic_selector: TopicSelector) -> PermissionScope:
    permissions = TopicPermission(
        cache_selector=cache_selector,
        role=TopicRole.PUBLISH_SUBSCRIBE,
        topic_selector=topic_selector,
    )
    return PermissionScope(permission_scope=Permissions([permissions]))

def topic_subscribe_only(cache_selector: CacheSelector, topic_selector: TopicSelector) -> PermissionScope:
    permissions = TopicPermission(
        cache_selector=cache_selector,
        role=TopicRole.SUBSCRIBE_ONLY,
        topic_selector=topic_selector,
    )
    return PermissionScope(permission_scope=Permissions([permissions]))

def topic_publish_only(cache_selector: CacheSelector, topic_selector: TopicSelector) -> PermissionScope:
    permissions = TopicPermission(
        cache_selector=cache_selector,
        role=TopicRole.PUBLISH_ONLY,
        topic_selector=topic_selector,
    )
    return PermissionScope(permission_scope=Permissions([permissions]))

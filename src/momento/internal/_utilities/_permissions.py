"""Permissions conversion functions.

Used to convert from the SDK permissions data classes to the protobuf permissions objects.
"""
from typing import Union

from momento_wire_types import permissionmessages_pb2 as permissions_pb

from momento.auth.access_control.disposable_token_scope import (
    CacheItemKey,
    CacheItemKeyPrefix,
    DisposableTokenCachePermission,
    DisposableTokenCachePermissions,
    DisposableTokenScope,
)
from momento.auth.access_control.permission_scope import (
    CacheName,
    CachePermission,
    CacheRole,
    Permission,
    Permissions,
    PermissionScope,
    PredefinedScope,
    TopicName,
    TopicPermission,
    TopicRole,
)
from momento.utilities.shared_sync_asyncio import str_to_bytes


class SuperuserPermissions(PredefinedScope):
    pass


def permissions_from_permission_scope(scope: PermissionScope) -> permissions_pb.Permissions:
    if isinstance(scope.permission_scope, SuperuserPermissions):
        return permissions_pb.Permissions(super_user=permissions_pb.SuperUserPermissions.SuperUser)  # type: ignore[attr-defined,misc]
    elif isinstance(scope.permission_scope, Permissions) and not isinstance(scope.permission_scope, PredefinedScope):
        converted_perms = [
            token_permission_to_grpc_permission(permission) for permission in scope.permission_scope.permissions
        ]
        if len(converted_perms) == 0:
            raise ValueError("Permissions must have at least one permission")
        return permissions_pb.Permissions(explicit=permissions_pb.ExplicitPermissions(permissions=converted_perms))
    else:
        raise ValueError("Unrecognized permission scope:", scope)


def token_permission_to_grpc_permission(permission: Permission) -> permissions_pb.PermissionsType:
    if isinstance(permission, TopicPermission):
        topic_permission = topic_permission_to_grpc_permission(permission)
        return permissions_pb.PermissionsType(topic_permissions=topic_permission)
    elif isinstance(permission, CachePermission):
        cache_permission = cache_permission_to_grpc_permission(permission)
        return permissions_pb.PermissionsType(cache_permissions=cache_permission)


def cache_permission_to_grpc_permission(permission: CachePermission) -> permissions_pb.PermissionsType.CachePermissions:
    role = assign_cache_role(permission)
    if permission.cache_selector.is_all_caches():
        return permissions_pb.PermissionsType.CachePermissions(
            all_caches=permissions_pb.PermissionsType.All(),
            role=role,
        )
    elif isinstance(permission.cache_selector.cache, str):
        return permissions_pb.PermissionsType.CachePermissions(
            cache_selector=assign_cache_selector(permission),
            role=role,
        )
    elif isinstance(permission.cache_selector.cache, CacheName):
        return permissions_pb.PermissionsType.CachePermissions(
            cache_selector=assign_cache_selector(permission),
            role=role,
        )
    else:
        raise ValueError("Unrecognized cache permission:", permission)


def assign_cache_role(permission: CachePermission) -> permissions_pb.CacheRole:
    if permission.role == CacheRole.READ_WRITE:
        return permissions_pb.CacheReadWrite
    elif permission.role == CacheRole.READ_ONLY:
        return permissions_pb.CacheReadOnly
    elif permission.role == CacheRole.WRITE_ONLY:
        return permissions_pb.CacheWriteOnly
    else:
        raise ValueError("Unrecognized cache role:", permission.role)


def topic_permission_to_grpc_permission(permission: TopicPermission) -> permissions_pb.PermissionsType.TopicPermissions:
    role = assign_topic_role(permission)

    # Cannot assign to all_topics or all_caches after creating TopicPermissions,
    # so we need to assign it at creation instead.
    if permission.topic_selector.is_all_topics() and permission.cache_selector.is_all_caches():
        return permissions_pb.PermissionsType.TopicPermissions(
            all_topics=permissions_pb.PermissionsType.All(),
            all_caches=permissions_pb.PermissionsType.All(),
            role=role,
        )
    elif permission.topic_selector.is_all_topics():
        return permissions_pb.PermissionsType.TopicPermissions(
            all_topics=permissions_pb.PermissionsType.All(),
            cache_selector=assign_cache_selector(permission),
            role=role,
        )
    elif permission.cache_selector.is_all_caches():
        return permissions_pb.PermissionsType.TopicPermissions(
            topic_selector=assign_topic_selector(permission),
            all_caches=permissions_pb.PermissionsType.All(),
            role=role,
        )
    else:
        return permissions_pb.PermissionsType.TopicPermissions(
            topic_selector=assign_topic_selector(permission),
            cache_selector=assign_cache_selector(permission),
            role=role,
        )


def assign_topic_role(permission: TopicPermission) -> permissions_pb.TopicRole:
    if permission.role == TopicRole.PUBLISH_SUBSCRIBE:
        return permissions_pb.TopicReadWrite
    elif permission.role == TopicRole.SUBSCRIBE_ONLY:
        return permissions_pb.TopicReadOnly
    elif permission.role == TopicRole.PUBLISH_ONLY:
        return permissions_pb.TopicWriteOnly
    else:
        raise ValueError("Unrecognized topic role:", permission.role)


def assign_topic_selector(permission: TopicPermission) -> permissions_pb.PermissionsType.TopicSelector:
    if isinstance(permission.topic_selector.topic, str):
        return permissions_pb.PermissionsType.TopicSelector(topic_name=permission.topic_selector.topic)
    elif isinstance(permission.topic_selector.topic, TopicName):
        return permissions_pb.PermissionsType.TopicSelector(topic_name=permission.topic_selector.topic.name)
    else:
        raise ValueError("Unrecognized topic selector:", permission.topic_selector)


def assign_cache_selector(
    permission: Union[CachePermission, TopicPermission, DisposableTokenCachePermission],
) -> permissions_pb.PermissionsType.CacheSelector:
    if isinstance(permission.cache_selector.cache, str):
        return permissions_pb.PermissionsType.CacheSelector(cache_name=permission.cache_selector.cache)
    elif isinstance(permission.cache_selector.cache, CacheName):
        return permissions_pb.PermissionsType.CacheSelector(cache_name=permission.cache_selector.cache.name)
    else:
        raise ValueError("Unrecognized cache selector:", permission.cache_selector)


def permissions_from_disposable_token_scope(scope: DisposableTokenScope) -> permissions_pb.Permissions:
    if isinstance(scope.disposable_token_scope, DisposableTokenCachePermissions):
        converted_perms = [
            disposable_token_permission_to_grpc_permission(permission)
            for permission in scope.disposable_token_scope.disposable_token_permissions
        ]
        if len(converted_perms) == 0:
            raise ValueError("Permissions must have at least one permission")
        return permissions_pb.Permissions(explicit=permissions_pb.ExplicitPermissions(permissions=converted_perms))
    elif isinstance(scope.disposable_token_scope, Permissions):
        converted_perms = [
            token_permission_to_grpc_permission(permission) for permission in scope.disposable_token_scope.permissions
        ]
        if len(converted_perms) == 0:
            raise ValueError("Permissions must have at least one permission")
        return permissions_pb.Permissions(explicit=permissions_pb.ExplicitPermissions(permissions=converted_perms))
    else:
        raise ValueError("Unrecognized disposable token permission scope:", scope)


def disposable_token_permission_to_grpc_permission(
    permission: DisposableTokenCachePermission,
) -> permissions_pb.PermissionsType:
    role = assign_cache_role(permission)

    # Cannot assign to all_items or all_caches after creating CachePermissions,
    # so we need to assign it at creation instead.
    if permission.cache_item_selector.is_all_cache_items() and permission.cache_selector.is_all_caches():
        return permissions_pb.PermissionsType(
            cache_permissions=permissions_pb.PermissionsType.CachePermissions(
                all_items=permissions_pb.PermissionsType.All(),
                all_caches=permissions_pb.PermissionsType.All(),
                role=role,
            )
        )
    elif permission.cache_item_selector.is_all_cache_items():
        return permissions_pb.PermissionsType(
            cache_permissions=permissions_pb.PermissionsType.CachePermissions(
                all_items=permissions_pb.PermissionsType.All(),
                cache_selector=assign_cache_selector(permission),
                role=role,
            )
        )
    elif permission.cache_selector.is_all_caches():
        return permissions_pb.PermissionsType(
            cache_permissions=permissions_pb.PermissionsType.CachePermissions(
                item_selector=assign_cache_item_selector(permission),
                all_caches=permissions_pb.PermissionsType.All(),
                role=role,
            )
        )
    else:
        return permissions_pb.PermissionsType(
            cache_permissions=permissions_pb.PermissionsType.CachePermissions(
                item_selector=assign_cache_item_selector(permission),
                cache_selector=assign_cache_selector(permission),
                role=role,
            )
        )


def assign_cache_item_selector(
    permission: DisposableTokenCachePermission,
) -> permissions_pb.PermissionsType.CacheItemSelector:
    if isinstance(permission.cache_item_selector.cache_item, CacheItemKey):
        return permissions_pb.PermissionsType.CacheItemSelector(
            key=str_to_bytes(permission.cache_item_selector.cache_item.key)
        )
    elif isinstance(permission.cache_item_selector.cache_item, CacheItemKeyPrefix):
        return permissions_pb.PermissionsType.CacheItemSelector(
            key_prefix=str_to_bytes(permission.cache_item_selector.cache_item.key_prefix)
        )
    elif isinstance(permission.cache_item_selector.cache_item, str):
        return permissions_pb.PermissionsType.CacheItemSelector(
            key=str_to_bytes(permission.cache_item_selector.cache_item)
        )
    else:
        raise ValueError("Unrecognized cache item selector:", permission.cache_item_selector)

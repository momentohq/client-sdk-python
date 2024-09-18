
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
from tests.utils import str_to_bytes


class SuperuserPermissions(PredefinedScope):
  pass

def permissions_from_permission_scope(scope: PermissionScope) -> permissions_pb.Permissions:
  result = permissions_pb.Permissions()
  if isinstance(scope.permission_scope, SuperuserPermissions):
    result.super_user = permissions_pb.SuperUserPermissions()
    return result
  elif isinstance(scope.permission_scope, Permissions) and not isinstance(scope.permission_scope, PredefinedScope):
    converted_perms = [
      token_permission_to_grpc_permission(permission) for permission in scope.permission_scope.permissions
    ]
    result.explicit = permissions_pb.ExplicitPermissions(permissions=converted_perms)
    return result
  else:
    raise ValueError("Unrecognized permission scope")

def token_permission_to_grpc_permission(permission: Permission) -> permissions_pb.PermissionsType:
    if isinstance(permission, TopicPermission):
      topic_permission = topic_permission_to_grpc_permission(permission)
      return permissions_pb.PermissionsType(topic_permissions=topic_permission)
    elif isinstance(permission, CachePermission):
      cache_permission = cache_permission_to_grpc_permission(permission)
      return permissions_pb.PermissionsType(cache_permissions=cache_permission)

def cache_permission_to_grpc_permission(permission: CachePermission) -> permissions_pb.PermissionsType.CachePermissions:
  grpc_permission = permissions_pb.PermissionsType.CachePermissions()
  grpc_permission = assign_cache_role(permission, grpc_permission)
  grpc_permission = assign_cache_selector_for_cache_permission(permission, grpc_permission)
  return grpc_permission

def assign_cache_role(
    permission: CachePermission,
    grpc_permission: permissions_pb.PermissionsType.CachePermissions
) -> permissions_pb.PermissionsType.CachePermissions:
  if permission.role == CacheRole.READ_WRITE:
    grpc_permission.role = permissions_pb.CacheReadWrite
  elif permission.role == CacheRole.READ_ONLY:
    grpc_permission.role = permissions_pb.CacheReadOnly
  elif permission.role == CacheRole.WRITE_ONLY:
    grpc_permission.role = permissions_pb.CacheWriteOnly
  else:
    raise ValueError("Unrecognized cache role")
  return grpc_permission

def assign_cache_selector_for_cache_permission(
    permission: Union[DisposableTokenCachePermission, CachePermission],
    grpc_permission: permissions_pb.PermissionsType.CachePermissions
) -> permissions_pb.PermissionsType.CachePermissions:
  if permission.cache_selector.is_all_caches():
    grpc_permission.all_caches = permissions_pb.PermissionsType.All()
  elif isinstance(permission.cache_selector.cache, str):
    grpc_permission.cache_selector = permissions_pb.PermissionsType.CacheSelector(cache_name=permission.cache_selector.cache)
  elif isinstance(permission.cache_selector.cache, CacheName):
    grpc_permission.cache_selector = permissions_pb.PermissionsType.CacheSelector(cache_name=permission.cache_selector.cache.name)
  else:
    raise ValueError("Unrecognized cache selector")
  return grpc_permission

def topic_permission_to_grpc_permission(permission: TopicPermission) -> permissions_pb.PermissionsType.TopicPermissions:
  result = permissions_pb.PermissionsType.TopicPermissions()
  result = assign_topic_role(permission, result)
  result = assign_topic_selector(permission, result)
  result = assign_cache_selector_for_topic_permission(permission, result)
  return result

def assign_topic_role(
    permission: TopicPermission,
    grpc_permission: permissions_pb.PermissionsType.TopicPermissions
) -> permissions_pb.PermissionsType.TopicPermissions:
  if permission.role == TopicRole.PUBLISH_SUBSCRIBE:
    grpc_permission.role = permissions_pb.TopicReadWrite
  elif permission.role == TopicRole.SUBSCRIBE_ONLY:
    grpc_permission.role = permissions_pb.TopicReadOnly
  elif permission.role == TopicRole.PUBLISH_ONLY:
    grpc_permission.role = permissions_pb.TopicWriteOnly
  else:
    raise ValueError("Unrecognized topic role")
  return grpc_permission

def assign_topic_selector(
    permission: TopicPermission,
    grpc_permission: permissions_pb.PermissionsType.TopicPermissions
) -> permissions_pb.PermissionsType.TopicPermissions:
  if permission.topic_selector.is_all_topics():
    grpc_permission.all_topics = permissions_pb.PermissionsType.All()
  elif isinstance(permission.topic_selector.topic, str):
    grpc_permission.topic_selector = permissions_pb.PermissionsType.TopicSelector(topic_name=permission.topic_selector.topic)
  elif isinstance(permission.topic_selector.topic, TopicName):
    grpc_permission.topic_selector = permissions_pb.PermissionsType.TopicSelector(topic_name=permission.topic_selector.topic.name)
  else:
    raise ValueError("Unrecognized topic selector")
  return grpc_permission

def assign_cache_selector_for_topic_permission(
    permission: TopicPermission,
    grpc_permission: permissions_pb.PermissionsType.TopicPermissions
) -> permissions_pb.PermissionsType.TopicPermissions:
  if permission.cache_selector.is_all_caches():
    grpc_permission.all_caches = permissions_pb.PermissionsType.All()
  elif isinstance(permission.cache_selector.cache, str):
    grpc_permission.cache_selector = permissions_pb.PermissionsType.CacheSelector(cache_name=permission.cache_selector.cache)
  elif isinstance(permission.cache_selector.cache, CacheName):
    grpc_permission.cache_selector = permissions_pb.PermissionsType.CacheSelector(cache_name=permission.cache_selector.cache.name)
  else:
    raise ValueError("Unrecognized cache selector")
  return grpc_permission

def permissions_from_disposable_token_scope(scope: DisposableTokenScope) -> permissions_pb.Permissions:
  result = permissions_pb.Permissions()
  if isinstance(scope.disposable_token_scope, DisposableTokenCachePermissions):
    converted_perms = [
      disposable_token_permission_to_grpc_permission(permission) for permission in scope.disposable_token_scope.disposable_token_permissions
    ]
    result.explicit = permissions_pb.ExplicitPermissions(permissions=converted_perms)
    return result
  elif isinstance(scope.disposable_token_scope, Permissions):
    converted_perms = [
      token_permission_to_grpc_permission(permission) for permission in scope.disposable_token_scope.permissions
    ]
    result.explicit = permissions_pb.ExplicitPermissions(permissions=converted_perms)
    return result
  else:
    raise ValueError("Unrecognized disposable token permission scope")

def disposable_token_permission_to_grpc_permission(permission: DisposableTokenCachePermission) -> permissions_pb.PermissionsType.CachePermissions:
  grpc_permission = permissions_pb.PermissionsType.CachePermissions()
  grpc_permission = assign_cache_role(permission, grpc_permission)
  grpc_permission = assign_cache_selector_for_cache_permission(permission, grpc_permission)
  grpc_permission = assign_cache_item_selector(permission, grpc_permission)
  return grpc_permission

def assign_cache_item_selector(
    permission: DisposableTokenCachePermission,
    grpc_permission: permissions_pb.PermissionsType.CachePermissions
) -> permissions_pb.PermissionsType.CachePermissions:
  if permission.cache_item_selector.is_all_cache_items():
    grpc_permission.all_items = permissions_pb.PermissionsType.All()
  elif isinstance(permission.cache_item_selector.cache_item, str):
    grpc_permission.item_selector = permissions_pb.PermissionsType.CacheItemSelector(key=str_to_bytes(permission.cache_item_selector.cache_item))
  elif isinstance(permission.cache_item_selector.cache_item, CacheItemKey):
    grpc_permission.item_selector = permissions_pb.PermissionsType.CacheItemSelector(key=str_to_bytes(permission.cache_item_selector.cache_item.key))
  elif isinstance(permission.cache_item_selector.cache_item, CacheItemKeyPrefix):
    grpc_permission.item_selector = permissions_pb.PermissionsType.CacheItemSelector(key_prefix=str_to_bytes(permission.cache_item_selector.cache_item.key_prefix))
  else:
    raise ValueError("Unrecognized cache item selector")
  return grpc_permission

"""Disposable Token Scope.

Defines the data classes for specifying the permission scope of a disposable token.
"""
from dataclasses import dataclass
from typing import List, Optional, Union

from momento.auth.access_control.permission_scope import (
    CachePermission,
    Permissions,
)


@dataclass
class AllCacheItems:
    pass


@dataclass
class CacheItemKey:
    key: str


@dataclass
class CacheItemKeyPrefix:
    key_prefix: str


@dataclass
class CacheItemSelector:
    cache_item: Union[CacheItemKey, CacheItemKeyPrefix, AllCacheItems, str]

    def is_all_cache_items(self) -> bool:
        return isinstance(self.cache_item, AllCacheItems)


@dataclass
class DisposableTokenCachePermission(CachePermission):
    cache_item_selector: CacheItemSelector


@dataclass
class DisposableTokenCachePermissions:
    disposable_token_permissions: List[DisposableTokenCachePermission]


@dataclass
class DisposableTokenScope:
    disposable_token_scope: Union[Permissions, DisposableTokenCachePermissions]


@dataclass
class DisposableTokenProps:
    token_id: Optional[str]

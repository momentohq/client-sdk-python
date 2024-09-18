from dataclasses import dataclass
from typing import List, Optional, Union

from momento.auth.access_control.permission_scope import (
    CachePermission,
    CacheRole,
    CacheSelector,
    Permissions,
)


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

    def __init__(self, cache_item: Union[CacheItemKey, CacheItemKeyPrefix, AllCacheItems, str]):
        self.cache_item = cache_item

    def is_all_cache_items(self) -> bool:
        return isinstance(self.cache_item, AllCacheItems)


class DisposableTokenCachePermission(CachePermission):
    cache_item_selector: CacheItemSelector

    def __init__(self, item: CacheItemSelector, cache: CacheSelector, role: CacheRole):
        super().__init__(cache, role)
        self.item = item


@dataclass
class DisposableTokenCachePermissions:
    disposable_token_permissions: List[DisposableTokenCachePermission]

    def __init__(self, permissions: List[DisposableTokenCachePermission]):
        self.disposable_token_permissions = permissions


@dataclass
class DisposableTokenScope:
    disposable_token_scope: Union[Permissions, DisposableTokenCachePermissions]

    def __init__(self, permission_scope: Union[Permissions, DisposableTokenCachePermissions]):
        self.disposable_token_scope = permission_scope


@dataclass
class DisposableTokenProps:
    token_id: Optional[str]

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
    """Indicates permission to access all items in a cache."""

    pass


@dataclass
class CacheItemKey:
    """The key of a cache item."""

    key: str


@dataclass
class CacheItemKeyPrefix:
    """The prefix of a cache item key."""

    key_prefix: str


@dataclass
class CacheItemSelector:
    """A selection of cache items to grant permissions to, either all cache items, a specific cache item, or a items that match a specified key prefix."""

    cache_item: Union[CacheItemKey, CacheItemKeyPrefix, AllCacheItems, str]

    def is_all_cache_items(self) -> bool:
        return isinstance(self.cache_item, AllCacheItems)


@dataclass
class DisposableTokenCachePermission(CachePermission):
    """Encapsulates the information needed to grant permissions to a cache item."""

    cache_item_selector: CacheItemSelector


@dataclass
class DisposableTokenCachePermissions:
    """A list of permissions to grant to a disposable token."""

    disposable_token_permissions: List[DisposableTokenCachePermission]


@dataclass
class DisposableTokenScope:
    """A set of permissions to grant to a disposable token."""

    disposable_token_scope: Union[Permissions, DisposableTokenCachePermissions]


@dataclass
class DisposableTokenProps:
    """Additional properties for a disposable token, such as token_id, which can be used to identify the source of a token."""

    token_id: Optional[str]

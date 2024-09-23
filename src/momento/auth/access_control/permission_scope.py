"""Permission Scope.

Defines the data classes for specifying the permission scope of a generated token.
"""
from dataclasses import dataclass
from enum import Enum
from typing import List, Union


@dataclass
class AllCaches:
    """Indicates permission to access all caches."""

    pass


@dataclass
class AllTopics:
    """Indicates permission to access all topics."""

    pass


@dataclass
class PredefinedScope:
    """Indicates a predefined permission scope."""

    pass


class CacheRole(Enum):
    """The permission level for a cache."""

    READ_WRITE = "read_write"
    READ_ONLY = "read_only"
    WRITE_ONLY = "write_only"


@dataclass
class CacheName:
    """The name of a cache."""

    name: str


@dataclass
class CacheSelector:
    """A selection of caches to grant permissions to, either all caches or a specific cache."""

    cache: Union[CacheName, AllCaches, str]

    def is_all_caches(self) -> bool:
        """Check if the cache selector is for all caches."""
        return isinstance(self.cache, AllCaches)


@dataclass
class CachePermission:
    """Encapsulates the information needed to grant permissions to a cache."""

    cache_selector: CacheSelector
    role: CacheRole


class TopicRole(Enum):
    """The permission level for a topic."""

    PUBLISH_SUBSCRIBE = "publish_subscribe"
    SUBSCRIBE_ONLY = "subscribe_only"
    PUBLISH_ONLY = "publish_only"


@dataclass
class TopicName:
    """The name of a topic."""

    name: str


@dataclass
class TopicSelector:
    """A selection of topics to grant permissions to, either all topics or a specific topic."""

    topic: Union[TopicName, AllTopics, str]

    def is_all_topics(self) -> bool:
        return isinstance(self.topic, AllTopics)


@dataclass
class TopicPermission:
    """Encapsulates the information needed to grant permissions to a topic."""

    role: TopicRole
    cache_selector: CacheSelector
    topic_selector: TopicSelector


Permission = Union[CachePermission, TopicPermission]


@dataclass
class Permissions:
    """A list of permissions to grant to an API key."""

    permissions: List[Permission]


ALL_DATA_READ_WRITE = Permissions(
    permissions=[
        CachePermission(CacheSelector(AllCaches()), CacheRole.READ_WRITE),
        TopicPermission(
            TopicRole.PUBLISH_SUBSCRIBE,
            CacheSelector(AllCaches()),
            TopicSelector(AllTopics()),
        ),
    ]
)


@dataclass
class PermissionScope:
    """A set of permissions to grant to an API key, either a predefined scope or a custom scope."""

    permission_scope: Union[Permissions, PredefinedScope]

    def is_all_data_read_write(self) -> bool:
        return self.permission_scope == ALL_DATA_READ_WRITE

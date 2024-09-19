from dataclasses import dataclass
from enum import Enum
from typing import List, Union

# from momento.auth.access_control.disposable_token_scope import DisposableTokenScope


@dataclass
class AllCaches:
    pass


@dataclass
class AllTopics:
    pass


@dataclass
class PredefinedScope:
    pass


class CacheRole(Enum):
    READ_WRITE = "read_write"
    READ_ONLY = "read_only"
    WRITE_ONLY = "write_only"


@dataclass
class CacheName:
    name: str


@dataclass
class CacheSelector:
    cache: Union[CacheName, AllCaches, str]

    def is_all_caches(self) -> bool:
        return isinstance(self.cache, AllCaches)


@dataclass
class CachePermission:
    cache_selector: CacheSelector
    role: CacheRole


class TopicRole(Enum):
    PUBLISH_SUBSCRIBE = "publish_subscribe"
    SUBSCRIBE_ONLY = "subscribe_only"
    PUBLISH_ONLY = "publish_only"


@dataclass
class TopicName:
    name: str


@dataclass
class TopicSelector:
    topic: Union[TopicName, AllTopics, str]

    def is_all_topics(self) -> bool:
        return isinstance(self.topic, AllTopics)


@dataclass
class TopicPermission:
    role: TopicRole
    cache_selector: CacheSelector
    topic_selector: TopicSelector


Permission = Union[CachePermission, TopicPermission]


@dataclass
class Permissions:
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
    permission_scope: Union[Permissions, PredefinedScope]

    def is_all_data_read_write(self) -> bool:
        return self.permission_scope == ALL_DATA_READ_WRITE

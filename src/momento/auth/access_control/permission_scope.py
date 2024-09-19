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

    def __init__(self, cache: Union[CacheName, AllCaches, str]):
        self.cache = cache

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

    def __init__(self, topic: Union[TopicName, AllTopics, str]):
        self.topic = topic

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
        CachePermission(role=CacheRole.READ_WRITE, cache_selector=CacheSelector(cache=AllCaches())),
        TopicPermission(
            role=TopicRole.PUBLISH_SUBSCRIBE,
            cache_selector=CacheSelector(cache=AllCaches()),
            topic_selector=TopicSelector(topic=AllTopics()),
        ),
    ]
)


@dataclass
class PermissionScope:
    permission_scope: Union[Permissions, PredefinedScope]

    def is_all_data_read_write(self) -> bool:
        return self.permission_scope == ALL_DATA_READ_WRITE

    def __init__(self, permission_scope: Union[Permissions, PredefinedScope]):
        self.permission_scope = permission_scope

    def get_permissions_objects(self) -> Permissions:
        if isinstance(self.permission_scope, PredefinedScope):
            raise ValueError("PredefinedScope cannot be converted to a Permissions object")
        return self.permission_scope

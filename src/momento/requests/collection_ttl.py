from dataclasses import dataclass
from datetime import timedelta
from typing import Optional


@dataclass
class CollectionTtl:
    ttl: Optional[timedelta] = None
    refresh_ttl: bool = True

    @staticmethod
    def from_cache_ttl() -> "CollectionTtl":
        return CollectionTtl(ttl=None, refresh_ttl=True)

    @staticmethod
    def of(ttl: timedelta) -> "CollectionTtl":
        return CollectionTtl(ttl=ttl)

    @staticmethod
    def refresh_ttl_if_provided(ttl: Optional[timedelta] = None) -> "CollectionTtl":
        return CollectionTtl(ttl=ttl, refresh_ttl=ttl is not None)

    def with_refresh_ttl_on_updates(self) -> "CollectionTtl":
        return CollectionTtl(ttl=self.ttl, refresh_ttl=True)

    def with_no_refresh_ttl_on_updates(self) -> "CollectionTtl":
        return CollectionTtl(ttl=self.ttl, refresh_ttl=False)

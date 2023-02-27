from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Optional

from momento.internal._utilities import _validate_timedelta_ttl


@dataclass
class CollectionTtl:
    """Represents the desired behavior for managing the TTL on collection objects in your cache.

    For cache operations that modify a collection (dictionaries, lists, sets, etc), there are a few things
    to consider.  The first time the collection is created, we need to
    set a TTL on it.  For subsequent operations that modify the collection
    you may choose to update the TTL in order to prolong the life of the
    cached collection object, or you may choose to leave the TTL unmodified
    in order to ensure that the collection expires at the original TTL.

    The default behavior is to refresh the TTL (to prolong the life of the
    collection) each time it is written.  This behavior can be modified
    by calling the method `with_no_refresh_ttl_on_updates`.

    Raises:
        ValueError: if the ttl is not a positive amount of time.
    """

    ttl: Optional[timedelta] = None
    """The duration after which the cached collection should be expired from the cache.
    If `null`, we use the default TTL that was passed to the `CacheClient` init method.
    """
    refresh_ttl: bool = True
    """If `True`, the collection's TTL will be refreshed (to prolong the life of the collection)
    on every update.  If `False`, the collection's TTL will only be set when the collection is
    initially created.
    """

    def __post_init__(self) -> None:
        if self.ttl is None:
            return
        _validate_timedelta_ttl(ttl=self.ttl, field_name="ttl")

    @staticmethod
    def from_cache_ttl() -> CollectionTtl:
        """The default way to handle TTLs for collections.

        The default TTL `timedelta` that was specified when instantiating the
        `CacheClient` will be used, and the TTL for the collection will be
        refreshed any time the collection is modified.

        Returns:
            CollectionTtl
        """
        return CollectionTtl(ttl=None, refresh_ttl=True)

    @staticmethod
    def of(ttl: timedelta) -> CollectionTtl:
        """Constructs a CollectionTtl with the specified `timedelta`.

        TTL for the collection will be refreshed any time the collection is
        modified.

        Args:
            ttl (timedelta): the duration to use for the TTL

        Returns:
            CollectionTtl
        """
        return CollectionTtl(ttl=ttl)

    @staticmethod
    def refresh_ttl_if_provided(ttl: Optional[timedelta] = None) -> CollectionTtl:
        """Constructs a `CollectionTtl` with the specified `timedelta`.

        Will only refresh if the TTL is provided (ie not `null`).

        Args:
            ttl (Optional[timedelta], optional): the duration to use for the TTL. Defaults to None.

        Returns:
            CollectionTtl
        """
        return CollectionTtl(ttl=ttl, refresh_ttl=ttl is not None)

    def with_refresh_ttl_on_updates(self) -> CollectionTtl:
        """Specifies the TTL for the collection be refreshed when the collection is modified.

        This is the default behavior for a CollectionTTL.

        Returns:
            CollectionTtl
        """
        return CollectionTtl(ttl=self.ttl, refresh_ttl=True)

    def with_no_refresh_ttl_on_updates(self) -> CollectionTtl:
        """Specifies the TTL for the collection should not be refreshed when the collection is modified.

        Use this to ensure your collection expires at the originally specified time,
        even if you make modifications to the collection.

        Returns:
            CollectionTtl
        """
        return CollectionTtl(ttl=self.ttl, refresh_ttl=False)

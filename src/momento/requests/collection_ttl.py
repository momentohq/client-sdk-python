from dataclasses import dataclass
from datetime import timedelta
from typing import Optional


@dataclass
class CollectionTtl:
    """Represents the desired behavior for managing the TTL on collection
    objects (dictionaries, lists, sets, etc) in your cache.

    For cache operations that modify a collection, there are a few things
    to consider.  The first time the collection is created, we need to
    set a TTL on it.  For subsequent operations that modify the collection
    you may choose to update the TTL in order to prolong the life of the
    cached collection object, or you may choose to leave the TTL unmodified
    in order to ensure that the collection expires at the original TTL.

    The default behavior is to refresh the TTL (to prolong the life of the
    collection) each time it is written.  This behavior can be modified
    by calling the method `with_no_refresh_ttl_on_updates`.
    """

    ttl: Optional[timedelta] = None
    """The duration after which the cached collection should be expired from the cache.
    If `null`, we use the default TTL that was passed to the `SimpleCacheClient` init method.
    """
    refresh_ttl: bool = True
    """If `True`, the collection's TTL will be refreshed (to prolong the life of the collection)
    on every update.  If `False`, the collection's TTL will only be set when the collection is
    initially created.
    """

    @staticmethod
    def from_cache_ttl() -> "CollectionTtl":
        """The default way to handle TTLs for collections. The default TTL
        `timedelta` that was specified when instantiating the `SimpleCacheClient`
        will be used, and the TTL for the collection will be refreshed any
        time the collection is modified.

        Returns:
            CollectionTtl
        """
        return CollectionTtl(ttl=None, refresh_ttl=True)

    @staticmethod
    def of(ttl: timedelta) -> "CollectionTtl":
        """Constructs a CollectionTtl with the specified `timedelta`. TTL
        for the collection will be refreshed any time the collection is
        modified.

        Args:
            ttl (timedelta): the duration to use for the TTL

        Returns:
            CollectionTtl
        """
        return CollectionTtl(ttl=ttl)

    @staticmethod
    def refresh_ttl_if_provided(ttl: Optional[timedelta] = None) -> "CollectionTtl":
        """Constructs a `CollectionTtl` with the specified `timedelta`.
        Will only refresh if the TTL is provided (ie not `null`).

        Args:
            ttl (Optional[timedelta], optional): the duration to use for the TTL. Defaults to None.

        Returns:
            CollectionTtl
        """
        return CollectionTtl(ttl=ttl, refresh_ttl=ttl is not None)

    def with_refresh_ttl_on_updates(self) -> "CollectionTtl":
        """Specifies that the TTL for the collection should be refreshed when
        the collection is modified.  (This is the default behavior.)

        Returns:
            CollectionTtl
        """
        return CollectionTtl(ttl=self.ttl, refresh_ttl=True)

    def with_no_refresh_ttl_on_updates(self) -> "CollectionTtl":
        """Specifies that the TTL for the collection should not be refreshed
        when the collection is modified.  Use this if you want to ensure
        that your collection expires at the originally specified time, even
        if you make modifications to the value of the collection.

        Returns:
            CollectionTtl
        """
        return CollectionTtl(ttl=self.ttl, refresh_ttl=False)

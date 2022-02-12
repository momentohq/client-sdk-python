import functools

from ..aio.simple_cache_client import SimpleCacheClient
from ..cache_operation_responses import CacheGetStatus
from ..errors import SdkError
import json
import time
import typing


_TReturn = typing.TypeVar("_TReturn")
_TArg = typing.TypeVar("_TArg")


def cached_async(
    client: SimpleCacheClient,
    cache: str,
    *,
    is_class_method: bool = False,
    ttl_seconds: float = None,
    key_prefix: str = None,
    from_string_or_bytes=json.loads,
    to_string_or_bytes=json.dumps,
    on_hit: typing.Callable[[int], None] = lambda load_latency_ns: None,
    on_miss: typing.Callable[
        [int, int, int], None
    ] = lambda load_latency_ns, store_latency_ns, compute_latency_ns: None,
    on_load_error: typing.Callable[[SdkError], None] = lambda error: None,
    on_store_error: typing.Callable[[SdkError], None] = lambda error: None,
    fail_open: bool = True,
):
    """
    Decorator for a function that returns an expensive value from Momento Simple Cache.

    If you have a simple function that caches a string, you'll just do:

    @cached_async(client, 'function_cache', ttl_seconds=90)
    async def load_from_database(user: str, attribute: str) -> str:
        user_object = await database.read(table='users', user_id=user)
        return user_object[attribute]

    This will cache each user's accessed attributes for 90 seconds. Hot attributes will
    tend to be in the cache and attributes displayed to the user won't be very stale.

    :param client: your configured Simple Cache client
    :param cache: Cache to use for this function's values
    :param is_class_method: [default=False] OPTIONAL set this True if you're decorating something that uses self
    :param ttl_seconds: OPTIONAL how long to store answers in Simple Cache
    :param key_prefix: [default=function_name] OPTIONAL prefix for keys in the cache to separate them from other cached
                       functions in the same shared cache
    :param from_string_or_bytes: OPTIONAL strategy for loading directly into an object. If you are storing
                                 complex objects you'll need to provide a deserializer here. You can use json,
                                 protocol buffers, whatever you want
    :param to_string_or_bytes: OPTIONAL strategy for writing cache values. If you are storing complex objects you'll
                               need to provide a serializer here. You can use json, protocol buffers, whatever you want
    :param on_hit: OPTIONAL callback for metrics
    :param on_miss: OPTIONAL callback for metrics
    :param on_load_error: OPTIONAL callback for metrics and tracing
    :param on_store_error: OPTIONAL callback for metrics and tracing
    :param fail_open: [default=True] OPTIONAL set False if you want the function to raise on error instead of invoking
                      your loader. on_*_error notification callbacks is the default behavior
    :return: cached or computed value
    """

    def decorator(fn: typing.Callable[[_TArg], _TReturn]):
        actual_key_prefix = fn.__name__ if key_prefix is None else key_prefix

        @functools.wraps(fn)
        async def wrapped(*args, **kwargs) -> _TReturn:
            try:
                key_args = args[1:] if is_class_method else args
                key = actual_key_prefix + "-" + json.dumps(_make_key(key_args, kwargs))
                start = time.perf_counter_ns()
                cached_value = await client.get(cache, key)
                duration_load_ns = time.perf_counter_ns() - start
            except SdkError as e:
                on_load_error(e)
                if fail_open:
                    fallback = await fn(*args, **kwargs)
                    return fallback
                raise e

            if cached_value.status() == CacheGetStatus.HIT:
                value = from_string_or_bytes(cached_value.value_as_bytes())
                on_hit(duration_load_ns)
                return value

            start = time.perf_counter_ns()
            fallback = await fn(*args, **kwargs)
            duration_compute_ns = time.perf_counter_ns() - start
            fallback_cache_value = to_string_or_bytes(fallback)

            try:
                start = time.perf_counter_ns()
                await client.set(cache, key, fallback_cache_value, ttl_seconds)
            except SdkError as e:
                on_store_error(e)
                # Continue anyway
            duration_store_ns = time.perf_counter_ns() - start

            on_miss(duration_load_ns, duration_store_ns, duration_compute_ns)
            return fallback

        return wrapped

    return decorator


def _make_key(args, kwds):
    """Make a cache key from optionally typed positional and keyword arguments

    The key is constructed in a way that is flat as possible rather than
    as a nested structure that would take more memory.

    If there is only a single argument and its data type is known to cache
    its hash value, then that argument is returned without a wrapper.  This
    saves space and improves lookup speed.

    ~~~ Borrowed from functools and lightly modified ~~~
    """
    # All of code below relies on kwds preserving the order input by the user.
    # Formerly, we sorted() the kwds before looping.  The new way is *much*
    # faster; however, it means that f(x=1, y=2) will now be treated as a
    # distinct call from f(y=2, x=1) which will be cached separately.
    key = args
    if kwds:
        key += (None,)  # keyword mark
        for item in kwds.items():
            key += item
    if len(key) == 1 and type(key[0]) in {int, str}:
        return key[0]
    return key

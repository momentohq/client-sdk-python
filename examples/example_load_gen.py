import asyncio
import logging
import os
from dataclasses import dataclass
from enum import Enum
from time import perf_counter_ns
from typing import Callable, Coroutine, Optional, Tuple, TypeVar

import colorlog  # type: ignore
from hdrh.histogram import HdrHistogram

import momento.errors
from momento.aio import simple_cache_client as scc
from momento.cache_operation_types import (
    CacheGetResponse,
    CacheGetStatus,
    CacheSetResponse,
)
from momento.logs import initialize_momento_logging


def initialize_logging(level: int) -> None:
    initialize_momento_logging()
    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    handler = colorlog.StreamHandler()
    handler.setFormatter(
        colorlog.ColoredFormatter(
            "%(asctime)s %(log_color)s%(levelname)-8s%(reset)s %(thin_cyan)s%(name)s%(reset)s %(message)s"
        )
    )
    handler.setLevel(level)
    root_logger.addHandler(handler)


class AsyncSetGetResult(Enum):
    SUCCESS = ("SUCCESS",)
    UNAVAILABLE = ("UNAVAILABLE",)
    DEADLINE_EXCEEDED = ("DEADLINE_EXCEEDED",)
    THROTTLE = ("THROTTLE",)


@dataclass
class BasicPythonLoadGenContext:
    start_time: float
    get_latencies: HdrHistogram
    set_latencies: HdrHistogram
    # TODO: these could be generalized into a map structure that
    #  would make it possible to deal with a broader range of
    #  failure types more succinctly.
    global_request_count: int
    global_success_count: int
    global_unavailable_count: int
    global_deadline_exceeded_count: int
    global_throttle_count: int


class BasicPythonLoadGen:
    cache_name = "python-loadgen"

    def __init__(
        self,
        request_timeout_ms: int,
        cache_item_payload_bytes: int,
        max_requests_per_second: int,
        number_of_concurrent_requests: int,
        show_stats_interval_seconds: int,
        total_seconds_to_run: int,
    ):
        self.logger = logging.getLogger("load-gen")
        self.auth_token = os.getenv("MOMENTO_AUTH_TOKEN")
        if not self.auth_token:
            raise ValueError("Missing required environment variable MOMENTO_AUTH_TOKEN")
        self.request_timeout_ms = request_timeout_ms
        self.max_requests_per_second = max_requests_per_second
        self.number_of_concurrent_requests = number_of_concurrent_requests
        self.show_stats_interval_seconds = show_stats_interval_seconds
        self.total_seconds_to_run = total_seconds_to_run
        self.cache_value = "x" * cache_item_payload_bytes
        self.request_interval_ms = number_of_concurrent_requests / max_requests_per_second * 1000

    async def run(self) -> None:
        cache_item_ttl_seconds = 60
        async with scc.SimpleCacheClient(
            self.auth_token, cache_item_ttl_seconds, self.request_timeout_ms
        ) as cache_client:
            try:
                await cache_client.create_cache(BasicPythonLoadGen.cache_name)
            except momento.errors.AlreadyExistsError:
                self.logger.info(f"Cache with name: {BasicPythonLoadGen.cache_name} already exists.")

            load_gen_context = BasicPythonLoadGenContext(
                start_time=perf_counter_ns(),
                get_latencies=HdrHistogram(1, 1000 * 60, 1),
                set_latencies=HdrHistogram(1, 1000 * 60, 1),
                global_request_count=0,
                global_success_count=0,
                global_unavailable_count=0,
                global_deadline_exceeded_count=0,
                global_throttle_count=0,
            )

            self.logger.info(f"Limiting to {self.max_requests_per_second} tps.")
            self.logger.info(f"Running {self.number_of_concurrent_requests} concurrent requests.")
            self.logger.info(f"Running for {self.total_seconds_to_run} seconds.")

            # Run for total_seconds_to_run
            try:
                await asyncio.wait_for(self.start(cache_client, load_gen_context), timeout=self.total_seconds_to_run)
            except asyncio.TimeoutError:
                # Show stats one last time.
                self.log_stats(load_gen_context)

                self.logger.info("DONE!")

    async def display_stats(self, context: BasicPythonLoadGenContext) -> None:
        while True:
            await asyncio.sleep(self.show_stats_interval_seconds)
            self.log_stats(context)

    async def start(
        self,
        cache_client: scc.SimpleCacheClient,
        context: BasicPythonLoadGenContext,
    ) -> None:
        async_get_set_results = (
            self.launch_and_run_worker(
                cache_client,
                context,
                worker_id,
            )
            for worker_id in range(1, self.number_of_concurrent_requests)
        )
        await asyncio.gather(*async_get_set_results, self.display_stats(context))

    def log_stats(self, context: BasicPythonLoadGenContext) -> None:
        self.logger.info(
            f"""
    cumulative stats:
    total requests: {context.global_request_count} ({self.tps(context, context.global_request_count)} tps)
      success: {context.global_success_count} ({self.percent_requests(context, context.global_success_count)}%) ({self.tps(context, context.global_success_count)} tps)
    unavailable: {context.global_unavailable_count} ({self.percent_requests(context, context.global_unavailable_count)}%)
    deadline exceeded: {context.global_deadline_exceeded_count} ({self.percent_requests(context, context.global_deadline_exceeded_count)}%)
    throttled: {context.global_throttle_count} ({self.percent_requests(context, context.global_throttle_count)}%)
               (Default throttling limit is 100tps; please contact Momento for a limit increase!)

    cumulative set latencies:
    {self.output_histogram_summary(context.set_latencies)}

    cumulative get latencies:
    {self.output_histogram_summary(context.get_latencies)}
    """  # noqa
        )

    async def launch_and_run_worker(
        self,
        client: scc.SimpleCacheClient,
        context: BasicPythonLoadGenContext,
        worker_id: int,
    ) -> None:
        operation_id = 1
        while True:
            await self.issue_async_set_get(client, context, worker_id, operation_id)
            operation_id += 1

    async def rate_limit(self, duration_ms):
        if duration_ms < self.request_interval_ms:
            delay = (self.request_interval_ms - duration_ms) / 1000
            await asyncio.sleep(delay)

    async def issue_async_set_get(
        self,
        client: scc.SimpleCacheClient,
        context: BasicPythonLoadGenContext,
        worker_id: int,
        operation_id: int,
    ) -> None:
        cache_key = f"worker{worker_id}operation{operation_id}"
        set_start_time = perf_counter_ns()
        result: Optional[CacheSetResponse] = await self.execute_request_and_update_context_counts(
            context, lambda: client.set(self.cache_name, cache_key, self.cache_value)
        )
        if result:
            set_duration = self.get_elapsed_millis(set_start_time)
            context.set_latencies.record_value(set_duration)
            await self.rate_limit(set_duration)

        get_start_time = perf_counter_ns()
        get_result: Optional[CacheGetResponse] = await self.execute_request_and_update_context_counts(
            context, lambda: client.get(self.cache_name, cache_key)
        )
        if get_result:
            get_duration = self.get_elapsed_millis(get_start_time)
            context.get_latencies.record_value(get_duration)
            if get_result.status() == CacheGetStatus.HIT:
                value = get_result.value()
                value_string = f"{value[0:10]}... (len: {len(value)})"
            else:
                value_string = "n/a"
            await self.rate_limit(set_duration)

    T = TypeVar("T")

    async def execute_request_and_update_context_counts(
        self,
        context: BasicPythonLoadGenContext,
        block: Callable[[], Coroutine[None, None, T]],
    ) -> Optional[T]:
        result, response = await self.execute_request(context, block)
        self.update_context_counts_for_request(context, result)
        return response

    async def execute_request(
        self,
        context: BasicPythonLoadGenContext,
        block: Callable[[], Coroutine[None, None, T]],
    ) -> Tuple[AsyncSetGetResult, Optional[T]]:
        try:
            result = await block()
            return AsyncSetGetResult.SUCCESS, result
        except momento.errors.InternalServerError as e:
            # TODO verify exception type
            self.logger.error(f"Caught InternalServerError: {e}")
            return AsyncSetGetResult.UNAVAILABLE, None
        except momento.errors.TimeoutError as e:
            # TODO need to verify exception type
            self.logger.error(f"Caught TimeoutError: {e}")
            return AsyncSetGetResult.DEADLINE_EXCEEDED, None
        except momento.errors.LimitExceededError:
            if context.global_throttle_count % 5_000 == 0:
                self.logger.warning("Received limit exceeded responses from the server.")
                self.logger.warning(
                    "Default limit is 100tps; please contact support@momentohq.com for a limit increase!"
                )
            return AsyncSetGetResult.THROTTLE, None

    @staticmethod
    def update_context_counts_for_request(context: BasicPythonLoadGenContext, result: AsyncSetGetResult):
        context.global_request_count += 1
        if result == AsyncSetGetResult.SUCCESS:
            context.global_success_count += 1
        elif result == AsyncSetGetResult.UNAVAILABLE:
            context.global_unavailable_count += 1
        elif result == AsyncSetGetResult.DEADLINE_EXCEEDED:
            context.global_deadline_exceeded_count += 1
        elif result == AsyncSetGetResult.THROTTLE:
            context.global_throttle_count += 1
        else:
            raise ValueError(f"Unsupported result type: {result}")

    def tps(self, context: BasicPythonLoadGenContext, request_count: int) -> int:
        return round((request_count * 1000) / self.get_elapsed_millis(context.start_time))

    @staticmethod
    def percent_requests(context: BasicPythonLoadGenContext, count: int) -> float:
        # multiply the ratio by 100 to get a percentage.  round to the nearest 0.1.
        return round((count / context.global_request_count) * 100, 1)

    @staticmethod
    def output_histogram_summary(histogram: HdrHistogram) -> str:
        return f"""
    count: {histogram.total_count}
      min: {histogram.min_value}
      p50: {histogram.get_value_at_percentile(50)}
      p90: {histogram.get_value_at_percentile(90)}
      p99: {histogram.get_value_at_percentile(99)}
    p99.9: {histogram.get_value_at_percentile(99.9)}
      max: {histogram.max_value}
"""

    @staticmethod
    def get_elapsed_millis(start_time: float) -> int:
        end_time = perf_counter_ns()
        result = round((end_time - start_time) / 1e6)
        return result


PERFORMANCE_INFORMATION_MESSAGE = """
Thanks for trying out our basic python load generator!  This tool is
included to allow you to experiment with performance in your environment
based on different configurations.  It's very simplistic, and only intended
to give you a quick way to explore the performance of the Momento client
running on a single python process.

Note that because python has a global interpreter lock, user code runs on
a single thread and cannot take advantage of multiple CPU cores.  Thus, the
limiting factor in request throughput will often be CPU.  Keep an eye on your CPU
consumption while running the load generator, and if you reach 100%
of a CPU core then you most likely won't be able to improve throughput further
without running additional python processes.

CPU will also impact your client-side latency; as you increase the number of
concurrent requests, if they are competing for CPU time then the observed
latency will increase.

Also, since performance will be impacted by network latency, you'll get the best
results if you run on a cloud VM in the same region as your Momento cache.

Check out the configuration settings at the bottom of the 'example_load_gen.py' to
see how different configurations impact performance.

If you have questions or need help experimenting further, please reach out to us!
"""


async def main(
    log_level: int,
    request_timeout_ms: int,
    cache_item_payload_bytes: int,
    max_requests_per_second: int,
    number_of_concurrent_requests: int,
    show_stats_interval_seconds: int,
    total_seconds_to_run: int,
) -> None:
    initialize_logging(log_level)
    load_generator = BasicPythonLoadGen(
        request_timeout_ms=request_timeout_ms,
        cache_item_payload_bytes=cache_item_payload_bytes,
        max_requests_per_second=max_requests_per_second,
        number_of_concurrent_requests=number_of_concurrent_requests,
        show_stats_interval_seconds=show_stats_interval_seconds,
        total_seconds_to_run=total_seconds_to_run,
    )
    await load_generator.run()
    print(PERFORMANCE_INFORMATION_MESSAGE)


load_generator_options = dict(
    #
    # This setting allows you to control the verbosity of the log output during
    # the load generator run. Available log levels are TRACE, DEBUG, INFO, WARN,
    # and ERROR.  DEBUG is a reasonable choice for this load generator program.
    #
    log_level=logging.DEBUG,
    #
    # Configures the Momento client to timeout if a request exceeds this limit.
    # Momento client default is 5 seconds.
    #
    request_timeout_ms=5 * 1_000,
    #
    # Controls the size of the payload that will be used for the cache items in
    # the load test.  Smaller payloads will generally provide lower latencies than
    # larger payloads.
    #
    cache_item_payload_bytes=100,
    #
    # Limit the load generator to this many requests per second to avoid being
    # rate limited by Momento servers.
    #
    max_requests_per_second=100,
    #
    # Controls the number of concurrent requests that will be made (via asynchronous
    # function calls) by the load test.  Increasing this number may improve throughput,
    # but it will also increase CPU consumption.  As CPU usage increases and there
    # is more contention between the concurrent function calls, client-side latencies
    # may increase.
    #
    number_of_concurrent_requests=50,
    #
    # Print some statistics about throughput and latency every time this many
    # seconds have passed.
    #
    show_stats_interval_seconds=5,
    #
    # Controls how long the load test will run, in seconds. We will execute operations
    # for this long and the exit.
    #
    total_seconds_to_run=60,
)


if __name__ == "__main__":
    asyncio.run(main(**load_generator_options))

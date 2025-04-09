from collections import defaultdict
from typing import Dict, List

from tests.momento.local.momento_rpc_method import MomentoRpcMethod


class MomentoLocalMetricsCollector:
    def __init__(self) -> None:
        # Data structure to store timestamps: cacheName -> requestName -> [timestamps]
        self.data: Dict[str, Dict[MomentoRpcMethod, List[int]]] = defaultdict(lambda: defaultdict(list))

    def add_timestamp(self, cache_name: str, request_name: MomentoRpcMethod, timestamp: int) -> None:
        """Add a timestamp for a specific request and cache.

        Args:
            cache_name: The name of the cache
            request_name: The name of the request (using MomentoRpcMethod enum)
            timestamp: The timestamp to record in seconds since epoch
        """
        self.data[cache_name][request_name].append(timestamp)

    def get_total_retry_count(self, cache_name: str, request_name: MomentoRpcMethod) -> int:
        """Calculate the total retry count for a specific cache and request.

        Args:
            cache_name: The name of the cache
            request_name: The name of the request (using MomentoRpcMethod enum)

        Returns:
            The total number of retries
        """
        timestamps = self.data.get(cache_name, {}).get(request_name, [])
        # Number of retries is one less than the number of timestamps
        return max(0, len(timestamps) - 1)

    def get_average_time_between_retries(self, cache_name: str, request_name: MomentoRpcMethod) -> float:
        """Calculate the average time between retries for a specific cache and request.

        Args:
            cache_name: The name of the cache
            request_name: The name of the request (using MomentoRpcMethod enum)

        Returns:
            The average time in seconds, or 0.0 if there are no retries
        """
        timestamps = self.data.get(cache_name, {}).get(request_name, [])
        if len(timestamps) < 2:
            return 0.0  # No retries occurred

        total_interval = sum(timestamps[i] - timestamps[i - 1] for i in range(1, len(timestamps)))
        return total_interval / (len(timestamps) - 1)

    def get_all_metrics(self) -> Dict[str, Dict[MomentoRpcMethod, List[int]]]:
        """Retrieve all collected metrics for debugging or analysis.

        Returns:
            The complete data structure with all recorded metrics
        """
        return self.data

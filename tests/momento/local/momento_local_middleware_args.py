from dataclasses import dataclass
from typing import List, Optional

from momento.errors import MomentoErrorCode

from tests.momento.local.momento_local_metrics_collector import MomentoLocalMetricsCollector
from tests.momento.local.momento_rpc_method import MomentoRpcMethod


@dataclass
class MomentoLocalMiddlewareArgs:
    """Arguments for Momento local middleware."""

    request_id: str
    test_metrics_collector: Optional[MomentoLocalMetricsCollector] = None
    return_error: Optional[MomentoErrorCode] = None
    error_rpc_list: Optional[List[MomentoRpcMethod]] = None
    error_count: Optional[int] = None
    delay_rpc_list: Optional[List[MomentoRpcMethod]] = None
    delay_millis: Optional[int] = None
    delay_count: Optional[int] = None
    stream_error_rpc_list: Optional[List[MomentoRpcMethod]] = None
    stream_error: Optional[MomentoErrorCode] = None
    stream_error_message_limit: Optional[int] = None

import pytest
from momento.errors import MomentoErrorCode
from momento.responses import CacheGet, CacheIncrement

from tests.conftest import client_local
from tests.momento.local.momento_local_async_middleware import MomentoLocalMiddlewareArgs
from tests.momento.local.momento_local_metrics_collector import MomentoLocalMetricsCollector
from tests.momento.local.momento_rpc_method import MomentoRpcMethod
from tests.utils import uuid_str


@pytest.mark.local
def test_retry_eligible_api_should_make_max_attempts_when_full_network_outage() -> None:
    metrics_collector = MomentoLocalMetricsCollector()
    middleware_args = MomentoLocalMiddlewareArgs(
        request_id=str(uuid_str()),
        test_metrics_collector=metrics_collector,
        return_error=MomentoErrorCode.SERVER_UNAVAILABLE,
        error_rpc_list=[MomentoRpcMethod.GET],
    )
    cache_name = uuid_str()

    with client_local(cache_name, middleware_args) as client:
        response = client.get(cache_name, "key")

        assert isinstance(response, CacheGet.Error)
        assert response.error_code == MomentoErrorCode.SERVER_UNAVAILABLE

        retry_count = metrics_collector.get_total_retry_count(cache_name, MomentoRpcMethod.GET)
        assert retry_count == 3


@pytest.mark.local
def test_non_retry_eligible_api_should_make_no_attempts_when_full_network_outage() -> None:
    metrics_collector = MomentoLocalMetricsCollector()
    middleware_args = MomentoLocalMiddlewareArgs(
        request_id=str(uuid_str()),
        test_metrics_collector=metrics_collector,
        return_error=MomentoErrorCode.SERVER_UNAVAILABLE,
        error_rpc_list=[MomentoRpcMethod.INCREMENT],
    )
    cache_name = uuid_str()

    with client_local(cache_name, middleware_args) as client:
        response = client.increment(cache_name, "key", 1)

        assert isinstance(response, CacheIncrement.Error)
        assert response.error_code == MomentoErrorCode.SERVER_UNAVAILABLE

        retry_count = metrics_collector.get_total_retry_count(cache_name, MomentoRpcMethod.INCREMENT)
        assert retry_count == 0


@pytest.mark.local
def test_retry_eligible_api_should_make_less_than_max_attempts_when_temporary_network_outage() -> None:
    metrics_collector = MomentoLocalMetricsCollector()
    middleware_args = MomentoLocalMiddlewareArgs(
        request_id=str(uuid_str()),
        test_metrics_collector=metrics_collector,
        return_error=MomentoErrorCode.SERVER_UNAVAILABLE,
        error_rpc_list=[MomentoRpcMethod.GET],
        error_count=2,
    )
    cache_name = uuid_str()

    with client_local(cache_name, middleware_args) as client:
        response = client.get(cache_name, "key")

        assert isinstance(response, CacheGet.Miss)

        retry_count = metrics_collector.get_total_retry_count(cache_name, MomentoRpcMethod.GET)
        assert 2 <= retry_count <= 3

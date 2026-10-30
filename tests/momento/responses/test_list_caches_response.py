from __future__ import annotations

from momento.responses import ListCaches
from momento.responses.control.cache.list import CacheInfo
from momento_wire_types import controlclient_pb2 as ctrl_pb


def test_list_caches_success_maps_cache_name_and_id() -> None:
    grpc_response = ctrl_pb._ListCachesResponse(
        cache=[
            ctrl_pb._Cache(cache_name="with-id", cache_id="0f1e2d3c"),
            ctrl_pb._Cache(cache_name="without-id"),
        ]
    )

    response = ListCaches.Success.from_grpc_response(grpc_response)

    assert response.caches == [CacheInfo(name="with-id", id="0f1e2d3c"), CacheInfo(name="without-id", id="")]

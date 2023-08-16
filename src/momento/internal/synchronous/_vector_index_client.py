from __future__ import annotations

from datetime import timedelta
from typing import Any, Optional

from momento_wire_types import vectorindex_pb2 as vectorindex_pb
from momento_wire_types import vectorindex_pb2_grpc as vectorindex_grpc

from momento import logs
from momento.auth import CredentialProvider
from momento.config import Configuration
from momento.errors import convert_error
from momento.internal._utilities import _validate_index_name
from momento.internal.synchronous._scs_grpc_manager import _VectorIndexDataGrpcManager
from momento.internal.synchronous._utilities import make_metadata
from momento.requests.item import Item
from momento.responses.vector_index.upsert_item_batch import (
    VectorIndexUpsertItemBatch,
    VectorIndexUpsertItemBatchResponse,
)


class _VectorIndexClient:
    """Internal vector index data client."""

    def __init__(self, configuration: Configuration, credential_provider: CredentialProvider):
        endpoint = credential_provider.cache_endpoint
        self._logger = logs.logger
        self._logger.debug("Vector index data client instantiated with endpoint: %s", endpoint)
        self._endpoint = endpoint

        default_deadline: timedelta = configuration.get_transport_strategy().get_grpc_configuration().get_deadline()
        self._default_deadline_seconds = int(default_deadline.total_seconds())

        self._grpc_manager = _VectorIndexDataGrpcManager(configuration, credential_provider)

    @property
    def endpoint(self) -> str:
        return self._endpoint

    def upsert_item_batch(
        self,
        index_name: str,
        items: list[Item],
    ) -> VectorIndexUpsertItemBatchResponse:
        try:
            self._log_issuing_request("UpsertItemBatch", {"index_name": index_name})
            _validate_index_name(index_name)
            request = vectorindex_pb.UpsertItemBatchRequest(
                index_name=index_name,
                items=[item.to_proto() for item in items],  # type: ignore
            )

            self._build_stub().UpsertItemBatch(request, timeout=self._default_deadline_seconds)  # type: ignore

            self._log_received_response("UpsertItemBatch", {"index_name": index_name})
            return VectorIndexUpsertItemBatch.Success()
        except Exception as e:
            self._log_request_error("set", e)
            return VectorIndexUpsertItemBatch.Error(convert_error(e))

    # TODO these were copied from the data client. Shouldn't use interpolation here for perf?
    def _log_received_response(self, request_type: str, request_args: dict[str, str]) -> None:
        self._logger.log(logs.TRACE, f"Received a {request_type} response for {request_args}")

    def _log_issuing_request(self, request_type: str, request_args: dict[str, str]) -> None:
        self._logger.log(logs.TRACE, f"Issuing a {request_type} request with {request_args}")

    def _log_request_error(self, request_type: str, e: Exception) -> None:
        self._logger.warning(f"{request_type} failed with exception: {e}")

    def _build_stub(self) -> vectorindex_grpc.VectorIndexStub:
        return self._grpc_manager.stub()

    def close(self) -> None:
        self._grpc_manager.close()

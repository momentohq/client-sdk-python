from __future__ import annotations

from datetime import timedelta
from typing import Optional

from momento_wire_types import vectorindex_pb2 as vectorindex_pb
from momento_wire_types import vectorindex_pb2_grpc as vectorindex_grpc

from momento import logs
from momento.auth import CredentialProvider
from momento.config import VectorIndexConfiguration
from momento.errors import convert_error
from momento.internal._utilities import _validate_index_name, _validate_top_k
from momento.internal.aio._vector_index_grpc_manager import _VectorIndexDataGrpcManager
from momento.internal.services import Service
from momento.requests.vector_index import AllMetadata, FilterExpression, Item
from momento.requests.vector_index import filters as F
from momento.responses.vector_index import (
    CountItems,
    CountItemsResponse,
    DeleteItemBatch,
    DeleteItemBatchResponse,
    GetItemBatch,
    GetItemBatchResponse,
    GetItemMetadataBatch,
    GetItemMetadataBatchResponse,
    Search,
    SearchAndFetchVectors,
    SearchAndFetchVectorsHit,
    SearchAndFetchVectorsResponse,
    SearchHit,
    SearchResponse,
    UpsertItemBatch,
    UpsertItemBatchResponse,
)


class _VectorIndexDataClient:
    """Internal vector index data client."""

    def __init__(self, configuration: VectorIndexConfiguration, credential_provider: CredentialProvider):
        endpoint = credential_provider.vector_endpoint
        self._logger = logs.logger
        self._logger.debug("Vector index data client instantiated with endpoint: %s", endpoint)
        self._endpoint = endpoint

        default_deadline: timedelta = configuration.get_transport_strategy().get_grpc_configuration().get_deadline()
        self._default_deadline_seconds = default_deadline.total_seconds()

        self._grpc_manager = _VectorIndexDataGrpcManager(configuration, credential_provider)

    @property
    def endpoint(self) -> str:
        return self._endpoint

    async def count_items(
        self,
        index_name: str,
    ) -> CountItemsResponse:
        try:
            self._log_issuing_request("CountItems", {"index_name": index_name})
            _validate_index_name(index_name)

            request = vectorindex_pb._CountItemsRequest(
                index_name=index_name, all=vectorindex_pb._CountItemsRequest.All()
            )
            response: vectorindex_pb._CountItemsResponse = await self._build_stub().CountItems(
                request, timeout=self._default_deadline_seconds
            )

            self._log_received_response("CountItems", {"index_name": index_name})
            return CountItems.Success(item_count=response.item_count)
        except Exception as e:
            self._log_request_error("count_items", e)
            return CountItems.Error(convert_error(e, Service.INDEX))

    async def upsert_item_batch(
        self,
        index_name: str,
        items: list[Item],
    ) -> UpsertItemBatchResponse:
        try:
            self._log_issuing_request("UpsertItemBatch", {"index_name": index_name})
            _validate_index_name(index_name)
            request = vectorindex_pb._UpsertItemBatchRequest(
                index_name=index_name,
                items=[item.to_proto() for item in items],
            )

            await self._build_stub().UpsertItemBatch(request, timeout=self._default_deadline_seconds)

            self._log_received_response("UpsertItemBatch", {"index_name": index_name})
            return UpsertItemBatch.Success()
        except Exception as e:
            self._log_request_error("set", e)
            return UpsertItemBatch.Error(convert_error(e, Service.INDEX))

    async def delete_item_batch(
        self,
        index_name: str,
        filter: FilterExpression | list[str],
    ) -> DeleteItemBatchResponse:
        try:
            self._log_issuing_request("DeleteItemBatch", {"index_name": index_name})
            _validate_index_name(index_name)

            filter_expression: vectorindex_pb._FilterExpression

            if isinstance(filter, FilterExpression):
                filter_expression = filter.to_filter_expression_proto()
            else:
                if len(filter) == 0:
                    return DeleteItemBatch.Success()
                filter_expression = F.IdInSet(filter).to_filter_expression_proto()

            request = vectorindex_pb._DeleteItemBatchRequest(index_name=index_name, filter=filter_expression)

            await self._build_stub().DeleteItemBatch(request, timeout=self._default_deadline_seconds)

            self._log_received_response("DeleteItemBatch", {"index_name": index_name})
            return DeleteItemBatch.Success()
        except Exception as e:
            self._log_request_error("delete", e)
            return DeleteItemBatch.Error(convert_error(e, Service.INDEX))

    @staticmethod
    def __build_metadata_request(metadata_fields: Optional[list[str]] | AllMetadata) -> vectorindex_pb._MetadataRequest:
        if isinstance(metadata_fields, AllMetadata):
            return vectorindex_pb._MetadataRequest(all=vectorindex_pb._MetadataRequest.All())
        else:
            return vectorindex_pb._MetadataRequest(
                some=vectorindex_pb._MetadataRequest.Some(fields=metadata_fields if metadata_fields else [])
            )

    @staticmethod
    def __build_no_score_threshold(score_threshold: Optional[float]) -> Optional[vectorindex_pb._NoScoreThreshold]:
        if score_threshold is None:
            return vectorindex_pb._NoScoreThreshold()
        return None

    @staticmethod
    def __build_filter_expression(
        filter_expression: Optional[FilterExpression],
    ) -> Optional[vectorindex_pb._FilterExpression]:
        if filter_expression is None:
            return None
        return filter_expression.to_filter_expression_proto()

    async def search(
        self,
        index_name: str,
        query_vector: list[float],
        top_k: int,
        metadata_fields: Optional[list[str]] | AllMetadata = None,
        score_threshold: Optional[float] = None,
        filter: Optional[FilterExpression] = None,
    ) -> SearchResponse:
        try:
            self._log_issuing_request("Search", {"index_name": index_name})
            _validate_index_name(index_name)
            _validate_top_k(top_k)

            query_vector_pb = vectorindex_pb._Vector(elements=query_vector)
            metadata_fields_pb = _VectorIndexDataClient.__build_metadata_request(metadata_fields)
            no_score_threshold = _VectorIndexDataClient.__build_no_score_threshold(score_threshold)
            filter_expression_pb = _VectorIndexDataClient.__build_filter_expression(filter)

            request = vectorindex_pb._SearchRequest(
                index_name=index_name,
                query_vector=query_vector_pb,
                top_k=top_k,
                metadata_fields=metadata_fields_pb,
                score_threshold=score_threshold,
                no_score_threshold=no_score_threshold,
                filter=filter_expression_pb,
            )

            response: vectorindex_pb._SearchResponse = await self._build_stub().Search(
                request, timeout=self._default_deadline_seconds
            )

            hits = [SearchHit.from_proto(hit) for hit in response.hits]
            self._log_received_response("Search", {"index_name": index_name})
            return Search.Success(hits=hits)
        except Exception as e:
            self._log_request_error("search", e)
            return Search.Error(convert_error(e, Service.INDEX))

    async def search_and_fetch_vectors(
        self,
        index_name: str,
        query_vector: list[float],
        top_k: int,
        metadata_fields: Optional[list[str]] | AllMetadata = None,
        score_threshold: Optional[float] = None,
        filter: Optional[FilterExpression] = None,
    ) -> SearchAndFetchVectorsResponse:
        try:
            self._log_issuing_request("SearchAndFetchVectors", {"index_name": index_name})
            _validate_index_name(index_name)
            _validate_top_k(top_k)

            query_vector_pb = vectorindex_pb._Vector(elements=query_vector)
            metadata_fields_pb = _VectorIndexDataClient.__build_metadata_request(metadata_fields)
            no_score_threshold = _VectorIndexDataClient.__build_no_score_threshold(score_threshold)
            filter_expression_pb = _VectorIndexDataClient.__build_filter_expression(filter)

            request = vectorindex_pb._SearchAndFetchVectorsRequest(
                index_name=index_name,
                query_vector=query_vector_pb,
                top_k=top_k,
                metadata_fields=metadata_fields_pb,
                score_threshold=score_threshold,
                no_score_threshold=no_score_threshold,
                filter=filter_expression_pb,
            )

            response: vectorindex_pb._SearchAndFetchVectorsResponse = await self._build_stub().SearchAndFetchVectors(
                request, timeout=self._default_deadline_seconds
            )

            hits = [SearchAndFetchVectorsHit.from_proto(hit) for hit in response.hits]
            self._log_received_response("SearchAndFetchVectors", {"index_name": index_name})
            return SearchAndFetchVectors.Success(hits=hits)
        except Exception as e:
            self._log_request_error("search_and_fetch_vectors", e)
            return SearchAndFetchVectors.Error(convert_error(e, Service.INDEX))

    async def get_item_batch(
        self,
        index_name: str,
        filter: list[str],
    ) -> GetItemBatchResponse:
        try:
            self._log_issuing_request("GetItemBatch", {"index_name": index_name})
            _validate_index_name(index_name)

            if len(filter) == 0:
                return GetItemBatch.Success(values={})

            request = vectorindex_pb._GetItemBatchRequest(
                index_name=index_name,
                filter=F.IdInSet(filter).to_filter_expression_proto(),
                metadata_fields=vectorindex_pb._MetadataRequest(all=vectorindex_pb._MetadataRequest.All()),
            )

            batch_response: vectorindex_pb._GetItemBatchResponse = await self._build_stub().GetItemBatch(
                request, timeout=self._default_deadline_seconds
            )
            self._log_received_response("GetItemBatch", {"index_name": index_name})
            return GetItemBatch.Success.from_proto(batch_response)
        except Exception as e:
            self._log_request_error("get_item_batch", e)
            return GetItemBatch.Error(convert_error(e, Service.INDEX))

    async def get_item_metadata_batch(
        self,
        index_name: str,
        filter: list[str],
    ) -> GetItemMetadataBatchResponse:
        try:
            self._log_issuing_request("GetItemMetadataBatch", {"index_name": index_name})
            _validate_index_name(index_name)

            if len(filter) == 0:
                return GetItemMetadataBatch.Success(values={})

            request = vectorindex_pb._GetItemMetadataBatchRequest(
                index_name=index_name,
                filter=F.IdInSet(filter).to_filter_expression_proto(),
                metadata_fields=vectorindex_pb._MetadataRequest(all=vectorindex_pb._MetadataRequest.All()),
            )

            batch_response: vectorindex_pb._GetItemMetadataBatchResponse = (
                await self._build_stub().GetItemMetadataBatch(request, timeout=self._default_deadline_seconds)
            )
            self._log_received_response("GetItemMetadataBatch", {"index_name": index_name})
            return GetItemMetadataBatch.Success.from_proto(batch_response)
        except Exception as e:
            self._log_request_error("get_item_metadata_batch", e)
            return GetItemMetadataBatch.Error(convert_error(e, Service.INDEX))

    # TODO these were copied from the data client. Shouldn't use interpolation here for perf?
    def _log_received_response(self, request_type: str, request_args: dict[str, str]) -> None:
        self._logger.log(logs.TRACE, f"Received a {request_type} response for {request_args}")

    def _log_issuing_request(self, request_type: str, request_args: dict[str, str]) -> None:
        self._logger.log(logs.TRACE, f"Issuing a {request_type} request with {request_args}")

    def _log_request_error(self, request_type: str, e: Exception) -> None:
        self._logger.warning(f"{request_type} failed with exception: {e}")

    def _build_stub(self) -> vectorindex_grpc.VectorIndexStub:
        return self._grpc_manager.async_stub()

    async def close(self) -> None:
        await self._grpc_manager.close()

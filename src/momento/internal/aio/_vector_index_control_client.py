import grpc
from momento_wire_types import controlclient_pb2 as ctrl_pb
from momento_wire_types import controlclient_pb2_grpc as ctrl_grpc

from momento import logs
from momento.auth import CredentialProvider
from momento.config import VectorIndexConfiguration
from momento.errors import InvalidArgumentException, convert_error
from momento.internal._utilities import _validate_index_name, _validate_num_dimensions
from momento.internal.aio._vector_index_grpc_manager import (
    _VectorIndexControlGrpcManager,
)
from momento.internal.services import Service
from momento.requests.vector_index import SimilarityMetric
from momento.responses.vector_index import (
    CreateIndex,
    CreateIndexResponse,
    DeleteIndex,
    DeleteIndexResponse,
    ListIndexes,
    ListIndexesResponse,
)

_DEADLINE_SECONDS = 60.0  # 1 minute


class _VectorIndexControlClient:
    """Momento Internal."""

    def __init__(self, configuration: VectorIndexConfiguration, credential_provider: CredentialProvider):
        endpoint = credential_provider.control_endpoint
        self._logger = logs.logger
        self._logger.debug("Vector index control client instantiated with endpoint: %s", endpoint)
        self._grpc_manager = _VectorIndexControlGrpcManager(configuration, credential_provider)
        self._endpoint = endpoint

    @property
    def endpoint(self) -> str:
        return self._endpoint

    async def create_index(
        self, index_name: str, num_dimensions: int, similarity_metric: SimilarityMetric
    ) -> CreateIndexResponse:
        try:
            self._logger.info(f"Creating index with name: {index_name}")
            _validate_index_name(index_name)
            _validate_num_dimensions(num_dimensions)

            if similarity_metric == SimilarityMetric.EUCLIDEAN_SIMILARITY:
                request = ctrl_pb._CreateIndexRequest(
                    index_name=index_name,
                    num_dimensions=num_dimensions,
                    similarity_metric=ctrl_pb._SimilarityMetric(
                        euclidean_similarity=ctrl_pb._SimilarityMetric._EuclideanSimilarity()
                    ),
                )
            elif similarity_metric == SimilarityMetric.INNER_PRODUCT:
                request = ctrl_pb._CreateIndexRequest(
                    index_name=index_name,
                    num_dimensions=num_dimensions,
                    similarity_metric=ctrl_pb._SimilarityMetric(
                        inner_product=ctrl_pb._SimilarityMetric._InnerProduct()
                    ),
                )
            elif similarity_metric == SimilarityMetric.COSINE_SIMILARITY:
                request = ctrl_pb._CreateIndexRequest(
                    index_name=index_name,
                    num_dimensions=num_dimensions,
                    similarity_metric=ctrl_pb._SimilarityMetric(
                        cosine_similarity=ctrl_pb._SimilarityMetric._CosineSimilarity()
                    ),
                )
            else:
                raise InvalidArgumentException(f"Invalid similarity metric `{similarity_metric}`", Service.INDEX)
            await self._build_stub().CreateIndex(request, timeout=_DEADLINE_SECONDS)
        except Exception as e:
            self._logger.debug("Failed to create index: %s with exception: %s", index_name, e)
            if isinstance(e, grpc.RpcError) and e.code() == grpc.StatusCode.ALREADY_EXISTS:
                return CreateIndex.IndexAlreadyExists()
            return CreateIndex.Error(convert_error(e, Service.INDEX))
        return CreateIndex.Success()

    async def delete_index(self, index_name: str) -> DeleteIndexResponse:
        try:
            self._logger.info(f"Deleting index with name: {index_name}")
            _validate_index_name(index_name)
            request = ctrl_pb._DeleteIndexRequest(index_name=index_name)
            await self._build_stub().DeleteIndex(request, timeout=_DEADLINE_SECONDS)
        except Exception as e:
            self._logger.debug("Failed to delete index: %s with exception: %s", index_name, e)
            return DeleteIndex.Error(convert_error(e, Service.INDEX))
        return DeleteIndex.Success()

    async def list_indexes(self) -> ListIndexesResponse:
        try:
            list_indexes_request = ctrl_pb._ListIndexesRequest()
            response = await self._build_stub().ListIndexes(list_indexes_request, timeout=_DEADLINE_SECONDS)
            return ListIndexes.Success.from_grpc_response(response)
        except Exception as e:
            return ListIndexes.Error(convert_error(e, Service.INDEX))

    def _build_stub(self) -> ctrl_grpc.ScsControlStub:
        return self._grpc_manager.async_stub()

    async def close(self) -> None:
        await self._grpc_manager.close()

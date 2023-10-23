from momento import (
    VectorIndexConfigurations,
    PreviewVectorIndexClient,
    PreviewVectorIndexClientAsync,
    CredentialProvider,
)
from momento.config import VectorIndexConfiguration
from momento.errors.error_details import MomentoErrorCode
from momento.responses.vector_index import ListIndexes, Search
from pathlib import Path

import pytest


def test_missing_root_cert() -> None:
    path = Path("bad")
    with pytest.raises(FileNotFoundError) as e:
        VectorIndexConfigurations.Default.latest().with_root_certificates_pem(path)

    assert f"Root certificate file not found at path: {path}" in str(e.value)


def test_bad_root_cert(
    vector_index_configuration: VectorIndexConfiguration, credential_provider: CredentialProvider
) -> None:
    root_cert = b"asdfasdf"

    grpc_configuration = vector_index_configuration.get_transport_strategy().get_grpc_configuration()
    grpc_configuration._root_certificates_pem = root_cert  # type: ignore[attr-defined]
    config = vector_index_configuration.with_transport_strategy(
        vector_index_configuration.get_transport_strategy().with_grpc_configuration(grpc_configuration)
    )

    client = PreviewVectorIndexClient(config, credential_provider)
    list_indexes_response = client.list_indexes()
    assert isinstance(list_indexes_response, ListIndexes.Error)
    assert list_indexes_response.error_code == MomentoErrorCode.SERVER_UNAVAILABLE

    search_response = client.search("asdf", [1, 2])
    assert isinstance(search_response, Search.Error)
    assert search_response.error_code == MomentoErrorCode.SERVER_UNAVAILABLE


async def test_bad_root_cert_async(
    vector_index_configuration: VectorIndexConfiguration, credential_provider: CredentialProvider
) -> None:
    root_cert = b"asdfasdf"

    grpc_configuration = vector_index_configuration.get_transport_strategy().get_grpc_configuration()
    grpc_configuration._root_certificates_pem = root_cert  # type: ignore[attr-defined]
    config = vector_index_configuration.with_transport_strategy(
        vector_index_configuration.get_transport_strategy().with_grpc_configuration(grpc_configuration)
    )

    client = PreviewVectorIndexClientAsync(config, credential_provider)
    list_indexes_response = await client.list_indexes()
    assert isinstance(list_indexes_response, ListIndexes.Error)
    assert list_indexes_response.error_code == MomentoErrorCode.SERVER_UNAVAILABLE

    search_response = await client.search("asdf", [1, 2])
    assert isinstance(search_response, Search.Error)
    assert search_response.error_code == MomentoErrorCode.SERVER_UNAVAILABLE

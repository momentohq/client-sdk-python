from pathlib import Path

import pytest

from momento import (
    CredentialProvider,
    PreviewVectorIndexClient,
    PreviewVectorIndexClientAsync,
    VectorIndexConfigurations,
)
from momento.config import VectorIndexConfiguration
from momento.config.transport.transport_strategy import StaticGrpcConfiguration
from momento.errors.error_details import MomentoErrorCode
from momento.responses.vector_index import ListIndexes, Search
from tests.utils import unique_test_vector_index_name


def _with_root_cert(config: VectorIndexConfiguration, root_cert: bytes) -> VectorIndexConfiguration:
    grpc_configuration = StaticGrpcConfiguration(
        config.get_transport_strategy().get_grpc_configuration().get_deadline(), root_cert
    )
    new_config = config.with_transport_strategy(
        config.get_transport_strategy().with_grpc_configuration(grpc_configuration)
    )
    return new_config


def test_missing_root_cert() -> None:
    path = Path("bad")
    with pytest.raises(FileNotFoundError) as e:
        VectorIndexConfigurations.Default.latest().with_root_certificates_pem(path)

    assert f"Root certificate file not found at path: {path}" in str(e.value)


def test_bad_root_cert(
    vector_index_configuration: VectorIndexConfiguration, credential_provider: CredentialProvider
) -> None:
    root_cert = b"asdfasdf"

    config = _with_root_cert(vector_index_configuration, root_cert)
    client = PreviewVectorIndexClient(config, credential_provider)
    list_indexes_response = client.list_indexes()
    assert isinstance(list_indexes_response, ListIndexes.Error)
    assert list_indexes_response.error_code == MomentoErrorCode.SERVER_UNAVAILABLE

    search_response = client.search(unique_test_vector_index_name(), [1, 2])
    assert isinstance(search_response, Search.Error)
    assert search_response.error_code == MomentoErrorCode.SERVER_UNAVAILABLE


async def test_bad_root_cert_async(
    vector_index_configuration: VectorIndexConfiguration, credential_provider: CredentialProvider
) -> None:
    root_cert = b"asdfasdf"
    config = _with_root_cert(vector_index_configuration, root_cert)

    client = PreviewVectorIndexClientAsync(config, credential_provider)
    list_indexes_response = await client.list_indexes()
    assert isinstance(list_indexes_response, ListIndexes.Error)
    assert list_indexes_response.error_code == MomentoErrorCode.SERVER_UNAVAILABLE

    search_response = await client.search(unique_test_vector_index_name(), [1, 2])
    assert isinstance(search_response, Search.Error)
    assert search_response.error_code == MomentoErrorCode.SERVER_UNAVAILABLE


# def test_good_root_cert(
#     vector_index_configuration: VectorIndexConfiguration, credential_provider: CredentialProvider
# ) -> None:
#     # On my machine, this is the path to the root certificates pem file that is cached by the grpc library.
#     root_cert_path = Path("./.venv/lib/python3.11/site-packages/grpc/_cython/_credentials/roots.pem")
#     config = vector_index_configuration.with_root_certificates_pem(root_cert_path)

#     client = PreviewVectorIndexClient(config, credential_provider)
#     list_indexes_response = client.list_indexes()
#     assert isinstance(list_indexes_response, ListIndexes.Success)

#     search_response = client.search(unique_test_vector_index_name(), [1, 2])
#     assert isinstance(search_response, Search.Error)
#     assert search_response.error_code == MomentoErrorCode.NOT_FOUND_ERROR


# async def test_good_root_cert_async(
#     vector_index_configuration: VectorIndexConfiguration, credential_provider: CredentialProvider
# ) -> None:
#     # On my machine, this is the path to the root certificates pem file that is cached by the grpc library.
#     root_cert_path = Path("./.venv/lib/python3.11/site-packages/grpc/_cython/_credentials/roots.pem")
#     config = vector_index_configuration.with_root_certificates_pem(root_cert_path)

#     client = PreviewVectorIndexClientAsync(config, credential_provider)
#     list_indexes_response = await client.list_indexes()
#     assert isinstance(list_indexes_response, ListIndexes.Success)

#     search_response = await client.search(unique_test_vector_index_name(), [1, 2])
#     assert isinstance(search_response, Search.Error)
#     assert search_response.error_code == MomentoErrorCode.NOT_FOUND_ERROR

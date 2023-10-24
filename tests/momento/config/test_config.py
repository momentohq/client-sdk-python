from datetime import timedelta
from pathlib import Path

import pytest

from momento import CacheClient, CacheClientAsync, Configurations, CredentialProvider
from momento.config import Configuration
from momento.config.transport.transport_strategy import StaticGrpcConfiguration
from momento.errors import MomentoErrorCode
from momento.responses import CacheGet, ListCaches


def test_configuration_client_timeout_copy_constructor(configuration: Configuration) -> None:
    def snag_deadline(config: Configuration) -> timedelta:
        return config.get_transport_strategy().get_grpc_configuration().get_deadline()

    original_deadline: timedelta = snag_deadline(configuration)
    assert original_deadline.total_seconds() == 15
    configuration = configuration.with_client_timeout(timedelta(seconds=600))
    assert snag_deadline(configuration).total_seconds() == 600


def _with_root_cert(config: Configuration, root_cert: bytes) -> Configuration:
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
        Configurations.Laptop.latest().with_root_certificates_pem(path)

    assert f"Root certificate file not found at path: {path}" in str(e.value)


def test_bad_root_cert(configuration: Configuration, credential_provider: CredentialProvider) -> None:
    root_cert = b"asdfasdf"

    config = _with_root_cert(configuration, root_cert)
    client = CacheClient(config, credential_provider, timedelta(seconds=60))
    list_indexes_response = client.list_caches()
    assert isinstance(list_indexes_response, ListCaches.Error)
    assert list_indexes_response.error_code == MomentoErrorCode.SERVER_UNAVAILABLE

    get_response = client.get("asdf", "key")
    assert isinstance(get_response, CacheGet.Error)
    assert get_response.error_code == MomentoErrorCode.SERVER_UNAVAILABLE


async def test_bad_root_cert_async(configuration: Configuration, credential_provider: CredentialProvider) -> None:
    root_cert = b"asdfasdf"

    config = _with_root_cert(configuration, root_cert)
    client = CacheClientAsync(config, credential_provider, timedelta(seconds=60))
    list_indexes_response = await client.list_caches()
    assert isinstance(list_indexes_response, ListCaches.Error)
    assert list_indexes_response.error_code == MomentoErrorCode.SERVER_UNAVAILABLE

    get_response = await client.get("asdf", "key")
    assert isinstance(get_response, CacheGet.Error)
    assert get_response.error_code == MomentoErrorCode.SERVER_UNAVAILABLE

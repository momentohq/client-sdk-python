from datetime import timedelta
from pathlib import Path

import pytest
from momento import CacheClient, CacheClientAsync, Configurations, CredentialProvider
from momento.config import Configuration
from momento.config.transport.transport_strategy import StaticGrpcConfiguration
from momento.errors import MomentoErrorCode
from momento.responses import CacheGet, ListCaches

from tests.utils import unique_test_cache_name


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

    get_response = client.get(unique_test_cache_name(), "key")
    assert isinstance(get_response, CacheGet.Error)
    assert get_response.error_code == MomentoErrorCode.SERVER_UNAVAILABLE


async def test_bad_root_cert_async(configuration: Configuration, credential_provider: CredentialProvider) -> None:
    root_cert = b"asdfasdf"

    config = _with_root_cert(configuration, root_cert)
    client = CacheClientAsync(config, credential_provider, timedelta(seconds=60))
    list_indexes_response = await client.list_caches()
    assert isinstance(list_indexes_response, ListCaches.Error)
    assert list_indexes_response.error_code == MomentoErrorCode.SERVER_UNAVAILABLE

    get_response = await client.get(unique_test_cache_name(), "key")
    assert isinstance(get_response, CacheGet.Error)
    assert get_response.error_code == MomentoErrorCode.SERVER_UNAVAILABLE


# def test_good_root_cert(configuration: Configuration, credential_provider: CredentialProvider) -> None:
#     # On my machine, this is the path to the root certificates pem file that is cached by the grpc library.
#     root_cert_path = Path("./.venv/lib/python3.11/site-packages/grpc/_cython/_credentials/roots.pem")
#     config = configuration.with_root_certificates_pem(root_cert_path)

#     client = CacheClient(config, credential_provider, timedelta(seconds=60))
#     list_indexes_response = client.list_caches()
#     assert isinstance(list_indexes_response, ListCaches.Success)

#     get_response = client.get(unique_test_cache_name(), "key")
#     assert isinstance(get_response, CacheGet.Error)
#     assert get_response.error_code == MomentoErrorCode.NOT_FOUND_ERROR


# async def test_good_root_cert_async(configuration: Configuration, credential_provider: CredentialProvider) -> None:
#     # On my machine, this is the path to the root certificates pem file that is cached by the grpc library.
#     root_cert_path = Path("./.venv/lib/python3.11/site-packages/grpc/_cython/_credentials/roots.pem")
#     config = configuration.with_root_certificates_pem(root_cert_path)

#     client = CacheClientAsync(config, credential_provider, timedelta(seconds=60))
#     list_indexes_response = await client.list_caches()
#     assert isinstance(list_indexes_response, ListCaches.Success)

#     get_response = await client.get(unique_test_cache_name(), "key")
#     assert isinstance(get_response, CacheGet.Error)
#     assert get_response.error_code == MomentoErrorCode.NOT_FOUND_ERROR


def test_lambda_config_disables_keepalive() -> None:
    config = Configurations.Lambda.latest()
    grpc_config = config.get_transport_strategy().get_grpc_configuration()
    assert grpc_config.get_keepalive_permit_without_calls() == 0
    assert grpc_config.get_keepalive_time() is None
    assert grpc_config.get_keepalive_timeout() is None


def test_laptop_config_enables_keepalive() -> None:
    config = Configurations.Laptop.latest()
    grpc_config = config.get_transport_strategy().get_grpc_configuration()
    assert grpc_config.get_keepalive_permit_without_calls() == 1

    keepalive_time = grpc_config.get_keepalive_time()
    assert keepalive_time is not None
    if keepalive_time is not None:
        assert keepalive_time.seconds == 5

    keepalive_timeout = grpc_config.get_keepalive_timeout()
    assert keepalive_timeout is not None
    if keepalive_timeout is not None:
        assert keepalive_timeout.seconds == 1

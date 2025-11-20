import base64
import json
import os

import jwt
import pytest
from momento.auth.credential_provider import CredentialProvider
from momento.auth.momento_endpoint_resolver import _Base64DecodedV1Token

from tests.utils import uuid_str

test_email = "user@test.com"
test_control_endpoint = "control.test.com"
test_cache_endpoint = "cache.test.com"
test_message = {"sub": test_email, "cp": test_control_endpoint, "c": test_cache_endpoint}
test_token = jwt.encode(test_message, "secret", algorithm="HS512")
test_env_var_name = uuid_str()
test_v1_env_var_name = uuid_str()
test_v1_api_key = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJPbmxpbmUgSldUIEJ1aWxkZXIiLCJpYXQiOjE2NzgzMDU4MTIsImV4cCI6NDg2NTUxNTQxMiwiYXVkIjoiIiwic3ViIjoianJvY2tldEBleGFtcGxlLmNvbSJ9.8Iy8q84Lsr-D3YCo_HP4d-xjHdT8UCIuvAYcxhFMyz8"  # noqa: E501
test_decoded_v1_token = _Base64DecodedV1Token(api_key=test_v1_api_key, endpoint="test.momentohq.com")
test_encoded_v1_token = base64.b64encode(json.dumps(test_decoded_v1_token.__dict__).encode("utf-8"))

os.environ[test_env_var_name] = test_token
os.environ[test_v1_env_var_name] = test_encoded_v1_token.decode("utf-8")


@pytest.mark.parametrize(
    "provider, auth_token, control_endpoint, cache_endpoint",
    [
        (CredentialProvider.from_string(auth_token=test_token), test_token, test_control_endpoint, test_cache_endpoint),
        (
            CredentialProvider.from_string(
                auth_token=test_token,
                control_endpoint="give.me.control.test.com",
                cache_endpoint="secret.cache.test.com",
            ),
            test_token,
            "give.me.control.test.com",
            "secret.cache.test.com",
        ),
        (
            CredentialProvider.from_environment_variable(env_var_name=test_env_var_name),
            test_token,
            test_control_endpoint,
            test_cache_endpoint,
        ),
        (
            CredentialProvider.from_environment_variable(
                env_var_name=test_env_var_name,
                control_endpoint="give.me.control.test.com",
                cache_endpoint="secret.cache.test.com",
            ),
            test_token,
            "give.me.control.test.com",
            "secret.cache.test.com",
        ),
        (
            CredentialProvider.from_string(auth_token=test_encoded_v1_token.decode("utf-8")),
            test_v1_api_key,
            "control.test.momentohq.com",
            "cache.test.momentohq.com",
        ),
        (
            CredentialProvider.from_string(
                auth_token=test_encoded_v1_token.decode("utf-8"),
                control_endpoint="give.me.control.test.com",
                cache_endpoint="secret.cache.test.com",
            ),
            test_v1_api_key,
            "give.me.control.test.com",
            "secret.cache.test.com",
        ),
        (
            CredentialProvider.from_environment_variable(env_var_name=test_v1_env_var_name),
            test_v1_api_key,
            "control.test.momentohq.com",
            "cache.test.momentohq.com",
        ),
        (
            CredentialProvider.from_environment_variable(
                env_var_name=test_v1_env_var_name,
                control_endpoint="give.me.control.test.com",
                cache_endpoint="secret.cache.test.com",
            ),
            test_v1_api_key,
            "give.me.control.test.com",
            "secret.cache.test.com",
        ),
    ],
)
def test_endpoints(provider: CredentialProvider, auth_token: str, control_endpoint: str, cache_endpoint: str) -> None:
    assert provider.auth_token == auth_token
    assert provider.control_endpoint == control_endpoint
    assert provider.cache_endpoint == cache_endpoint


def test_env_token_raises_if_not_exists() -> None:
    with pytest.raises(RuntimeError, match=r"Missing required environment variable"):
        CredentialProvider.from_environment_variable(env_var_name=uuid_str())


# Global API Key Tests
test_global_api_key = "testToken"
test_global_endpoint = "testEndpoint"
test_global_env_var_name = "MOMENTO_TEST_GLOBAL_API_KEY"
os.environ[test_global_env_var_name] = test_global_api_key


@pytest.mark.parametrize(
    "provider, expected_api_key, expected_control_endpoint, expected_cache_endpoint, expected_token_endpoint",
    [
        # global_key_from_string - basic usage
        (
            CredentialProvider.global_key_from_string(
                api_key=test_global_api_key,
                endpoint=test_global_endpoint,
            ),
            test_global_api_key,
            f"control.{test_global_endpoint}",
            f"cache.{test_global_endpoint}",
            f"token.{test_global_endpoint}",
        ),
        # global_key_from_environment_variable - basic usage
        (
            CredentialProvider.global_key_from_environment_variable(
                env_var_name=test_global_env_var_name,
                endpoint=test_global_endpoint,
            ),
            test_global_api_key,
            f"control.{test_global_endpoint}",
            f"cache.{test_global_endpoint}",
            f"token.{test_global_endpoint}",
        ),
    ],
)
def test_global_api_key_endpoints(
    provider: CredentialProvider,
    expected_api_key: str,
    expected_control_endpoint: str,
    expected_cache_endpoint: str,
    expected_token_endpoint: str,
) -> None:
    assert provider.auth_token == expected_api_key
    assert provider.control_endpoint == expected_control_endpoint
    assert provider.cache_endpoint == expected_cache_endpoint
    assert provider.token_endpoint == expected_token_endpoint


def test_global_key_from_string_raises_if_api_key_empty() -> None:
    with pytest.raises(RuntimeError, match=r"API key cannot be empty"):
        CredentialProvider.global_key_from_string(api_key="", endpoint=test_global_endpoint)


def test_global_key_from_string_raises_if_endpoint_empty() -> None:
    with pytest.raises(RuntimeError, match=r"Endpoint cannot be empty"):
        CredentialProvider.global_key_from_string(api_key=test_global_api_key, endpoint="")


def test_global_key_from_env_raises_if_env_var_name_empty() -> None:
    with pytest.raises(RuntimeError, match=r"Environment variable name cannot be empty"):
        CredentialProvider.global_key_from_environment_variable(env_var_name="", endpoint=test_global_endpoint)


def test_global_key_from_env_raises_if_env_var_missing() -> None:
    with pytest.raises(RuntimeError, match=r"Missing required environment variable"):
        CredentialProvider.global_key_from_environment_variable(env_var_name=uuid_str(), endpoint=test_global_endpoint)


def test_global_key_from_env_raises_if_endpoint_empty() -> None:
    with pytest.raises(RuntimeError, match=r"Endpoint cannot be empty"):
        CredentialProvider.global_key_from_environment_variable(env_var_name=test_global_env_var_name, endpoint="")


def test_global_key_from_env_raises_if_api_key_empty_string() -> None:
    empty_api_key_env_var = uuid_str()
    os.environ[empty_api_key_env_var] = ""
    with pytest.raises(RuntimeError, match=r"Missing required environment variable"):
        CredentialProvider.global_key_from_environment_variable(
            env_var_name=empty_api_key_env_var, endpoint=test_global_endpoint
        )

import base64
import json
import os
import re

import jwt
import pytest
from momento.auth.credential_provider import CredentialProvider
from momento.auth.momento_endpoint_resolver import _Base64DecodedV1Token
from momento.errors.exceptions import InvalidArgumentException

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

# For v2 API key tests
test_v2_key_message = {"t": "g", "id": "some-id"}
test_v2_api_key = jwt.encode(test_v2_key_message, "secret", algorithm="HS512")
test_v2_key_env_var_name = uuid_str()
test_v2_endpoint = "testEndpoint"
test_v2_endpoint_env_var_name = uuid_str()
os.environ[test_v2_key_env_var_name] = test_v2_api_key
os.environ[test_v2_endpoint_env_var_name] = test_v2_endpoint


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


@pytest.mark.parametrize(
    "provider, expected_api_key, expected_control_endpoint, expected_cache_endpoint, expected_token_endpoint",
    [
        # v2_key_from_string - basic usage
        (
            CredentialProvider.from_api_key_v2(
                api_key=test_v2_api_key,
                endpoint=test_v2_endpoint,
            ),
            test_v2_api_key,
            f"control.{test_v2_endpoint}",
            f"cache.{test_v2_endpoint}",
            f"token.{test_v2_endpoint}",
        ),
        # v2_key_from_environment_variable - basic usage
        (
            CredentialProvider.from_env_var_v2(
                api_key_env_var=test_v2_key_env_var_name,
                endpoint_env_var=test_v2_endpoint_env_var_name,
            ),
            test_v2_api_key,
            f"control.{test_v2_endpoint}",
            f"cache.{test_v2_endpoint}",
            f"token.{test_v2_endpoint}",
        ),
    ],
)
def test_v2_api_key_endpoints(
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


def test_v2_key_from_string_raises_if_api_key_empty() -> None:
    with pytest.raises(InvalidArgumentException, match="API key cannot be empty"):
        CredentialProvider.from_api_key_v2(api_key="", endpoint=test_v2_endpoint)


def test_v2_key_from_string_raises_if_endpoint_empty() -> None:
    with pytest.raises(InvalidArgumentException, match="Endpoint cannot be empty"):
        CredentialProvider.from_api_key_v2(api_key=test_v2_api_key, endpoint="")


def test_v2_key_from_env_raises_if_env_var_name_empty() -> None:
    with pytest.raises(InvalidArgumentException, match="API key environment variable name cannot be empty"):
        CredentialProvider.from_env_var_v2(api_key_env_var="", endpoint_env_var=test_v2_endpoint_env_var_name)


def test_v2_key_from_env_raises_if_env_var_missing() -> None:
    with pytest.raises(RuntimeError, match="Missing required environment variable"):
        CredentialProvider.from_env_var_v2(api_key_env_var=uuid_str(), endpoint_env_var=test_v2_endpoint_env_var_name)


def test_v2_key_from_env_raises_if_endpoint_empty() -> None:
    with pytest.raises(InvalidArgumentException, match="Endpoint environment variable name cannot be empty"):
        CredentialProvider.from_env_var_v2(api_key_env_var=test_v2_key_env_var_name, endpoint_env_var="")


def test_v2_key_from_env_raises_if_api_key_empty_string() -> None:
    empty_api_key_env_var = uuid_str()
    os.environ[empty_api_key_env_var] = ""
    with pytest.raises(RuntimeError, match="Missing required environment variable"):
        CredentialProvider.from_env_var_v2(
            api_key_env_var=empty_api_key_env_var, endpoint_env_var=test_v2_endpoint_env_var_name
        )


def test_v2_key_from_string_raises_if_base64_api_key() -> None:
    with pytest.raises(
        InvalidArgumentException,
        match=re.escape(
            "Received an invalid v2 API key. Are you using the correct key? Or did you mean to use `from_string()` with a legacy key instead?"
        ),
    ):
        CredentialProvider.from_api_key_v2(
            api_key=test_encoded_v1_token.decode("utf-8"), endpoint=test_v2_endpoint_env_var_name
        )


def test_v2_key_from_env_raises_if_base64_api_key() -> None:
    with pytest.raises(
        InvalidArgumentException,
        match=re.escape(
            "Received an invalid v2 API key. Are you using the correct key? Or did you mean to use `from_environment_variable()` with a legacy key instead?"
        ),
    ):
        CredentialProvider.from_env_var_v2(
            api_key_env_var=test_v1_env_var_name, endpoint_env_var=test_v2_endpoint_env_var_name
        )


def test_v2_key_from_string_raises_if_pre_v1_token() -> None:
    with pytest.raises(
        InvalidArgumentException,
        match=re.escape(
            "Received an invalid v2 API key. Are you using the correct key? Or did you mean to use `from_string()` with a legacy key instead?"
        ),
    ):
        CredentialProvider.from_api_key_v2(api_key=test_token, endpoint=test_v2_endpoint_env_var_name)


def test_v2_key_from_env_raises_if_pre_v1_token() -> None:
    with pytest.raises(
        InvalidArgumentException,
        match=re.escape(
            "Received an invalid v2 API key. Are you using the correct key? Or did you mean to use `from_environment_variable()` with a legacy key instead?"
        ),
    ):
        CredentialProvider.from_env_var_v2(
            api_key_env_var=test_env_var_name, endpoint_env_var=test_v2_endpoint_env_var_name
        )


def test_v2_key_provided_to_from_string() -> None:
    with pytest.raises(
        InvalidArgumentException,
        match=re.escape(
            "Received a v2 API key. Are you using the correct key? Or did you mean to use `from_api_key_v2()` or `from_env_var_v2()` instead?"
        ),
    ):
        CredentialProvider.from_string(auth_token=test_v2_api_key)


def test_v2_key_provided_to_from_disposable_token() -> None:
    with pytest.raises(
        InvalidArgumentException,
        match=re.escape(
            "Received a v2 API key. Are you using the correct key? Or did you mean to use `from_api_key_v2()` or `from_env_var_v2()` instead?"
        ),
    ):
        CredentialProvider.from_disposable_token(auth_token=test_v2_api_key)


def test_from_disposable_token_raises_if_token_empty() -> None:
    with pytest.raises(InvalidArgumentException, match="Disposable token cannot be empty."):
        CredentialProvider.from_disposable_token(auth_token="")


def test_from_disposable_token_accepts_v1_api_key() -> None:
    provider = CredentialProvider.from_disposable_token(auth_token=test_encoded_v1_token.decode("utf-8"))
    assert provider.auth_token == test_v1_api_key
    assert provider.control_endpoint == "control.test.momentohq.com"
    assert provider.cache_endpoint == "cache.test.momentohq.com"
    assert provider.token_endpoint == "token.test.momentohq.com"


def test_from_disposable_token_accepts_pre_v1_token() -> None:
    provider = CredentialProvider.from_disposable_token(auth_token=test_token)
    assert provider.auth_token == test_token
    assert provider.control_endpoint == test_control_endpoint
    assert provider.cache_endpoint == test_cache_endpoint
    assert provider.token_endpoint == f"token.{test_cache_endpoint}"

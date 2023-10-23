from momento.config import VectorIndexConfigurations
from pathlib import Path

import pytest


def test_bad_root_cert() -> None:
    path = Path("bad")
    with pytest.raises(FileNotFoundError) as e:
        VectorIndexConfigurations.Default.latest().with_root_certificates_pem(path)

    assert f"Root certificate file not found at path: {path}" in str(e.value)

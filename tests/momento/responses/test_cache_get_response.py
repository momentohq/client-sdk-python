from __future__ import annotations

import pytest

from momento.responses import CacheGet


@pytest.mark.parametrize(
    "value_bytes, expected_str",
    [
        (b"hello", "CacheGet.Hit(value_bytes=b'hello')"),
        (("i" * 100).encode(), "CacheGet.Hit(value_bytes=b'iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii...')"),
    ],
)
def test_dictionary_fetch_hit_str_and_repr(value_bytes: bytes, expected_str: str) -> None:
    hit = CacheGet.Hit(value_bytes)
    assert str(hit) == expected_str
    assert eval(repr(hit)) == hit

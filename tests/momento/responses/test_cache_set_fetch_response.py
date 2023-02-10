from __future__ import annotations

import pytest

from momento.responses import CacheSetFetch


@pytest.mark.parametrize(
    "set_, expected_str",
    [
        ({b"hello"}, "CacheSetFetch.Hit(value_set_bytes={b'hello'})"),
        (
            {("i" * 100).encode()},
            f"CacheSetFetch.Hit(values_bytes=[b'{'i'*32}...'])",
        ),
        (
            {f"{i}".encode() for i in range(10)},
            "CacheSetFetch.Hit(values_bytes=[b'0', b'1', b'2', b'3', b'4', ...])",
        ),
    ],
)
def test_set_fetch_hit_str_and_repr(set_: set[bytes], expected_str: str) -> None:
    hit = CacheSetFetch.Hit(set_)
    if "..." in expected_str:
        assert "..." in str(hit)
    else:
        assert str(hit) == expected_str
    assert eval(repr(hit)) == hit

from __future__ import annotations

import pytest
from momento.responses import CacheListFetch


@pytest.mark.parametrize(
    "list_, expected_str",
    [
        (
            [b"hello", b"world"],
            "CacheListFetch.Hit(value_list_bytes=[b'hello', b'world'])",
        ),
        (
            [("i" * 100).encode()],
            f"CacheListFetch.Hit(value_list_bytes=[b'{'i'*32}...'])",
        ),
        (
            [f"{i}".encode() for i in range(10)],
            "CacheListFetch.Hit(value_list_bytes=[b'0', b'1', b'2', b'3', b'4', ...])",
        ),
    ],
)
def test_list_fetch_hit_str_and_repr(list_: list[bytes], expected_str: str) -> None:
    hit = CacheListFetch.Hit(list_)
    assert str(hit) == expected_str
    assert eval(repr(hit)) == hit

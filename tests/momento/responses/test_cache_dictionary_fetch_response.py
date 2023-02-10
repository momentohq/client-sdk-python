from __future__ import annotations

import pytest

from momento.responses import CacheDictionaryFetch


@pytest.mark.parametrize(
    "dictionary, expected_str",
    [
        ({b"hello": b"world"}, "CacheDictionaryFetch.Hit(value_dictionary_bytes_bytes={b'hello': b'world'})"),
        (
            {("i" * 100).encode(): ("i" * 100).encode()},
            "CacheDictionaryFetch.Hit(value_dictionary_bytes_bytes={b'iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii...': b'iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii...'})",  # noqa: E501
        ),
        (
            {f"{i}".encode(): f"{i}".encode() for i in range(10)},
            "CacheDictionaryFetch.Hit(value_dictionary_bytes_bytes={b'7', b'2', b'6', b'9', b'3', ...})",
        ),
    ],
)
def test_dictionary_fetch_hit_str_and_repr(dictionary: dict[bytes, bytes], expected_str: str) -> None:
    hit = CacheDictionaryFetch.Hit(dictionary)
    if "..." in expected_str:
        assert "..." in str(hit)
    else:
        assert str(hit) == expected_str
    assert eval(repr(hit)) == hit

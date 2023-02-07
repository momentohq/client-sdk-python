from __future__ import annotations

from typing import Tuple


def make_metadata(cache_name: str) -> list[Tuple[str, str]]:
    return [("cache", cache_name)]

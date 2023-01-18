from typing import List, Tuple


def make_metadata(cache_name: str) -> List[Tuple[str, str]]:
    return [("cache", cache_name)]

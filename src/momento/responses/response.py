from abc import ABC


class CacheResponse(ABC):
    """Parent of all responses"""

    def __str__(self) -> str:
        return f"{self.__class__.__name__}"

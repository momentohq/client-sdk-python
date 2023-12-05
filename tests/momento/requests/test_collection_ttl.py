from datetime import timedelta
from typing import Optional

import pytest
from momento.errors import InvalidArgumentException
from momento.requests import CollectionTtl


def describe_collection_ttl() -> None:
    @pytest.mark.parametrize(
        "collection_ttl, expected_ttl, expected_refresh_ttl",
        [
            (CollectionTtl(ttl=None, refresh_ttl=False), None, False),
            (CollectionTtl(ttl=timedelta(days=1), refresh_ttl=False), timedelta(days=1), False),
            (CollectionTtl.from_cache_ttl(), None, True),
            (CollectionTtl.of(ttl=timedelta(days=1)), timedelta(days=1), True),
            (CollectionTtl.refresh_ttl_if_provided(ttl=None), None, False),
            (CollectionTtl.refresh_ttl_if_provided(ttl=timedelta(days=1)), timedelta(days=1), True),
        ],
    )
    def test_collection_ttl_builders(
        collection_ttl: CollectionTtl, expected_ttl: Optional[timedelta], expected_refresh_ttl: bool
    ) -> None:
        if expected_ttl is None:
            assert collection_ttl.ttl is None, "ttl was not None but should be"
        else:
            assert collection_ttl.ttl == expected_ttl, "ttl did not match"
        assert collection_ttl.refresh_ttl == expected_refresh_ttl, "refresh_ttl did not match"

    @pytest.mark.parametrize(
        "collection_ttl",
        [
            (CollectionTtl(ttl=None, refresh_ttl=False)),
            (CollectionTtl(ttl=timedelta(days=1), refresh_ttl=False)),
            (CollectionTtl(ttl=None, refresh_ttl=True)),
        ],
    )
    def test_with_and_without_refresh_ttl_on_updates(collection_ttl: CollectionTtl) -> None:
        new_collection_ttl = collection_ttl.with_refresh_ttl_on_updates()
        assert collection_ttl.ttl == new_collection_ttl.ttl, "ttl should have passed through identically but did not"
        assert new_collection_ttl.refresh_ttl, "refresh_ttl should be true after with_refresh_ttl_on_updates but wasn't"

        new_collection_ttl = collection_ttl.with_no_refresh_ttl_on_updates()
        assert collection_ttl.ttl == new_collection_ttl.ttl, "ttl should have passed through identically but did not"
        assert (
            not new_collection_ttl.refresh_ttl
        ), "refresh_ttl should be false after with_no_refresh_ttl_on_updates but wasn't"

    @pytest.mark.parametrize(
        "ttl",
        [
            (timedelta(seconds=-1)),
            (timedelta(seconds=42, minutes=-99)),
        ],
    )
    def test_negative_ttl(ttl: timedelta) -> None:
        with pytest.raises(InvalidArgumentException, match=r"ttl must be a positive amount of time."):
            CollectionTtl(ttl=ttl)

    def test_int_ttl() -> None:
        with pytest.raises(InvalidArgumentException, match=r"ttl must be a timedelta."):
            CollectionTtl(ttl=23)  # type: ignore[arg-type]

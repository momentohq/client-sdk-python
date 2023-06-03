from momento.internal._utilities import momento_version


def test_momento_version() -> None:
    assert momento_version != ""

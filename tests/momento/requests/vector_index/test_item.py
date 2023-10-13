import pytest

from momento.errors.exceptions import InvalidArgumentException
from momento.internal.services import Service
from momento.requests.vector_index import Item


def test_serialize_item_with_string_metadata() -> None:
    item = Item(id="id", vector=[1, 2, 3], metadata={"key": "value"}).to_proto()
    assert item.id == "id"
    assert item.vector.elements == [1, 2, 3]
    assert item.metadata[0].field == "key"
    assert item.metadata[0].string_value == "value"


def test_serialize_item_with_bad_metadata() -> None:
    with pytest.raises(InvalidArgumentException) as exc_info:
        Item(id="id", vector=[1, 2, 3], metadata={"key": 1}).to_proto()  # type: ignore

    assert exc_info.value.service == Service.INDEX
    assert (
        exc_info.value.message
        == "Metadata values must be strings. Field 'key' has a value of type <class 'int'> with value 1."
    )


def test_serialize_item_with_null_metadata() -> None:
    with pytest.raises(InvalidArgumentException) as exc_info:
        Item(id="id", vector=[1, 2, 3], metadata={"key": None}).to_proto()  # type: ignore

    assert exc_info.value.service == Service.INDEX
    assert (
        exc_info.value.message
        == "Metadata values must be strings. Field 'key' has a value of type <class 'NoneType'> with value None."
    )

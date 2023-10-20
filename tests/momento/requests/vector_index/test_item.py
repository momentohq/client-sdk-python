import pytest

from momento.errors.exceptions import InvalidArgumentException
from momento.internal.services import Service
from momento.requests.vector_index import Item


def test_serialize_item_with_diverse_metadata() -> None:
    item = Item(
        id="id",
        vector=[1, 2, 3],
        metadata={
            "string_key": "value",
            "int_key": 1,
            "double_key": 3.14,
            "bool_key": True,
            "list_of_strings_key": ["one", "two"],
        },
    ).to_proto()
    assert item.id == "id"
    assert item.vector.elements == [1, 2, 3]

    assert item.metadata[0].field == "string_key"
    assert item.metadata[0].string_value == "value"

    assert item.metadata[1].field == "int_key"
    assert item.metadata[1].integer_value == 1

    assert item.metadata[2].field == "double_key"
    assert item.metadata[2].double_value == 3.14

    assert item.metadata[3].field == "bool_key"
    assert item.metadata[3].boolean_value == True

    assert item.metadata[4].field == "list_of_strings_key"
    assert item.metadata[4].list_of_strings_value.values == ["one", "two"]


def test_serialize_item_with_bad_metadata() -> None:
    with pytest.raises(InvalidArgumentException) as exc_info:
        Item(id="id", vector=[1, 2, 3], metadata={"key": {"nested_key": "nested_value"}}).to_proto()  # type: ignore

    assert exc_info.value.service == Service.INDEX
    assert (
        exc_info.value.message
        == "Metadata values must be either str, int, float, bool, or list[str]. Field 'key' has a value of type <class 'dict'> with value {'nested_key': 'nested_value'}."  # noqa: E501 W503
    )


def test_serialize_item_with_null_metadata() -> None:
    with pytest.raises(InvalidArgumentException) as exc_info:
        Item(id="id", vector=[1, 2, 3], metadata={"key": None}).to_proto()  # type: ignore

    assert exc_info.value.service == Service.INDEX
    assert (
        exc_info.value.message
        == "Metadata values must be either str, int, float, bool, or list[str]. Field 'key' has a value of type <class 'NoneType'> with value None."  # noqa: E501 W503
    )

"""Filter expressions for the vector index request.

The filter expressions are used to filter the results of a vector index request.
This module contains the base class for all filter expressions, as well as
implementations for all the different types of filter expressions::
    And(Equals("foo", "bar"), GreaterThan("age", 18))
    Or(Equals("foo", "bar"), LessThan("age", 18))
    Not(Equals("foo", "bar"))

The `Field` class is used to create filter expressions in a more idiomatic way.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Iterable

from momento_wire_types import vectorindex_pb2 as vectorindex_pb

from momento.errors.exceptions import InvalidArgumentException
from momento.internal.services import Service


@dataclass
class FilterExpression(ABC):
    """Base class for all filter expressions."""

    @abstractmethod
    def to_filter_expression_proto(self) -> vectorindex_pb._FilterExpression:
        """Converts the filter expression to a protobuf filter expression.

        Returns:
            vectorindex_pb._FilterExpression: The protobuf filter expression.
        """
        ...

    def __and__(self, other: FilterExpression) -> FilterExpression:
        """Creates an AND expression between this expression and another.

        Args:
            other (FilterExpression): The other expression to AND with.

        Returns:
            FilterExpression: The AND expression.
        """
        return And(self, other)

    def __or__(self, other: FilterExpression) -> FilterExpression:
        """Creates an OR expression between this expression and another.

        Args:
            other (FilterExpression): The other expression to OR with.

        Returns:
            FilterExpression: The OR expression.
        """
        return Or(self, other)

    def __invert__(self) -> FilterExpression:
        """Creates a NOT expression of this expression.

        Returns:
            FilterExpression: The NOT expression.
        """
        return Not(self)


@dataclass
class And(FilterExpression):
    """Represents an AND expression between two filter expressions."""

    first_expression: FilterExpression
    """The first expression to AND."""
    second_expression: FilterExpression
    """The second expression to AND."""

    def to_filter_expression_proto(self) -> vectorindex_pb._FilterExpression:
        return vectorindex_pb._FilterExpression(and_expression=self.to_proto())

    def to_proto(self) -> vectorindex_pb._AndExpression:
        return vectorindex_pb._AndExpression(
            first_expression=self.first_expression.to_filter_expression_proto(),
            second_expression=self.second_expression.to_filter_expression_proto(),
        )


@dataclass
class Or(FilterExpression):
    """Represents an OR expression between two filter expressions."""

    first_expression: FilterExpression
    """The first expression to OR."""
    second_expression: FilterExpression
    """The second expression to OR."""

    def to_filter_expression_proto(self) -> vectorindex_pb._FilterExpression:
        return vectorindex_pb._FilterExpression(or_expression=self.to_proto())

    def to_proto(self) -> vectorindex_pb._OrExpression:
        return vectorindex_pb._OrExpression(
            first_expression=self.first_expression.to_filter_expression_proto(),
            second_expression=self.second_expression.to_filter_expression_proto(),
        )


@dataclass
class Not(FilterExpression):
    """Represents a NOT expression of a filter expression."""

    expression_to_negate: FilterExpression
    """The expression to negate."""

    def to_filter_expression_proto(self) -> vectorindex_pb._FilterExpression:
        return vectorindex_pb._FilterExpression(not_expression=self.to_proto())

    def to_proto(self) -> vectorindex_pb._NotExpression:
        return vectorindex_pb._NotExpression(
            expression_to_negate=self.expression_to_negate.to_filter_expression_proto()
        )


@dataclass
class Equals(FilterExpression):
    """Represents an equals expression between a field and a value."""

    field: str
    """The field to compare."""
    value: str | int | float | bool
    """The value to test equality with."""

    def to_filter_expression_proto(self) -> vectorindex_pb._FilterExpression:
        return vectorindex_pb._FilterExpression(equals_expression=self.to_proto())

    def to_proto(self) -> vectorindex_pb._EqualsExpression:
        if type(self.value) is str:
            return vectorindex_pb._EqualsExpression(field=self.field, string_value=self.value)
        elif type(self.value) is int:
            return vectorindex_pb._EqualsExpression(field=self.field, integer_value=self.value)
        elif type(self.value) is float:
            return vectorindex_pb._EqualsExpression(field=self.field, float_value=self.value)
        elif type(self.value) is bool:
            return vectorindex_pb._EqualsExpression(field=self.field, boolean_value=self.value)
        else:
            raise InvalidArgumentException(
                f"Invalid type for value: {type(self.value)} in equals expression. Must be one of str, int, float, bool.",
                Service.INDEX,
            )


@dataclass
class GreaterThan(FilterExpression):
    """Represents a greater than expression between a field and a value."""

    field: str
    """The field to compare."""
    value: int | float
    """The value to test greater than with."""

    def to_filter_expression_proto(self) -> vectorindex_pb._FilterExpression:
        return vectorindex_pb._FilterExpression(greater_than_expression=self.to_proto())

    def to_proto(self) -> vectorindex_pb._GreaterThanExpression:
        if type(self.value) is int:
            return vectorindex_pb._GreaterThanExpression(field=self.field, integer_value=self.value)
        elif type(self.value) is float:
            return vectorindex_pb._GreaterThanExpression(field=self.field, float_value=self.value)
        else:
            raise InvalidArgumentException(
                f"Invalid type for value: {type(self.value)} in greater than expression. Must be one of int, float.",
                Service.INDEX,
            )


@dataclass
class GreaterThanOrEqual(FilterExpression):
    """Represents a greater than or equal expression between a field and a value."""

    field: str
    """The field to compare."""
    value: int | float
    """The value to test greater than or equal with."""

    def to_filter_expression_proto(self) -> vectorindex_pb._FilterExpression:
        return vectorindex_pb._FilterExpression(greater_than_or_equal_expression=self.to_proto())

    def to_proto(self) -> vectorindex_pb._GreaterThanOrEqualExpression:
        if type(self.value) is int:
            return vectorindex_pb._GreaterThanOrEqualExpression(field=self.field, integer_value=self.value)
        elif type(self.value) is float:
            return vectorindex_pb._GreaterThanOrEqualExpression(field=self.field, float_value=self.value)
        else:
            raise InvalidArgumentException(
                f"Invalid type for value: {type(self.value)} in greater than or equal expression. Must be one of int, float.",
                Service.INDEX,
            )


@dataclass
class LessThan(FilterExpression):
    """Represents a less than expression between a field and a value."""

    field: str
    """The field to compare."""
    value: int | float
    """The value to test less than with."""

    def to_filter_expression_proto(self) -> vectorindex_pb._FilterExpression:
        return vectorindex_pb._FilterExpression(less_than_expression=self.to_proto())

    def to_proto(self) -> vectorindex_pb._LessThanExpression:
        if type(self.value) is int:
            return vectorindex_pb._LessThanExpression(field=self.field, integer_value=self.value)
        elif type(self.value) is float:
            return vectorindex_pb._LessThanExpression(field=self.field, float_value=self.value)
        else:
            raise InvalidArgumentException(
                f"Invalid type for value: {type(self.value)} in less than expression. Must be one of int, float.",
                Service.INDEX,
            )


@dataclass
class LessThanOrEqual(FilterExpression):
    """Represents a less than or equal expression between a field and a value."""

    field: str
    """The field to compare."""
    value: int | float
    """The value to test less than or equal with."""

    def to_filter_expression_proto(self) -> vectorindex_pb._FilterExpression:
        return vectorindex_pb._FilterExpression(less_than_or_equal_expression=self.to_proto())

    def to_proto(self) -> vectorindex_pb._LessThanOrEqualExpression:
        if type(self.value) is int:
            return vectorindex_pb._LessThanOrEqualExpression(field=self.field, integer_value=self.value)
        elif type(self.value) is float:
            return vectorindex_pb._LessThanOrEqualExpression(field=self.field, float_value=self.value)
        else:
            raise InvalidArgumentException(
                f"Invalid type for value: {type(self.value)} in less than or equal expression. Must be one of int, float.",
                Service.INDEX,
            )


@dataclass
class ListContains(FilterExpression):
    """Represents a list contains expression between a field and a value."""

    field: str
    """The list field to test."""
    value: str
    """The value to test list contains with."""

    def to_filter_expression_proto(self) -> vectorindex_pb._FilterExpression:
        return vectorindex_pb._FilterExpression(list_contains_expression=self.to_proto())

    def to_proto(self) -> vectorindex_pb._ListContainsExpression:
        # todo should make oneof defensively
        return vectorindex_pb._ListContainsExpression(field=self.field, string_value=self.value)


@dataclass
class IdInSet(FilterExpression):
    """Represents an expression to test if an item id is in a set of ids."""

    ids: Iterable[str]
    """The set of ids to test id in set with."""

    def to_filter_expression_proto(self) -> vectorindex_pb._FilterExpression:
        return vectorindex_pb._FilterExpression(id_in_set_expression=self.to_proto())

    def to_proto(self) -> vectorindex_pb._IdInSetExpression:
        return vectorindex_pb._IdInSetExpression(ids=list(self.ids))

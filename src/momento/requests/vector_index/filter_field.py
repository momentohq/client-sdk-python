"""Field class for filter expressions.

This class is used to create filter expressions in a more readable way::

        from momento.requests.vector_index.filters import Field

        Field("name") == "foo"
        Field("age") >= 18
        Field("tags").list_contains("books")
        (Field("year") > 2000) | (Field("year") < 1990)
"""

from __future__ import annotations

from dataclasses import dataclass

from momento_wire_types import vectorindex_pb2 as vectorindex_pb

from . import filters as F


@dataclass
class Field:
    """Represents a field in a filter expression.

    Can be used to create filter expressions in a more readable way::

        from momento.requests.vector_index.filters import Field

        Field("name") == "foo"
        Field("age") >= 18
        Field("tags").list_contains("books")
        (Field("year") > 2000) | (Field("year") < 1990)
    """

    name: str
    """The name of the field."""

    def to_filter_expression_proto(self) -> vectorindex_pb._FilterExpression:
        """Converts the field to a protobuf filter expression.

        A bare field is equivalent to the field in a boolean context.

        Returns:
            vectorindex_pb._FilterExpression: The protobuf filter expression.
        """
        return F.Equals(self.name, True).to_filter_expression_proto()

    def __eq__(self, other: str | int | float | bool) -> F.Equals:  # type: ignore
        return F.Equals(self.name, other)

    def __invert__(self) -> F.Equals:
        return F.Equals(self.name, False)

    def __ne__(self, other: str | int | float | bool) -> F.Not:  # type: ignore
        return F.Not(F.Equals(self.name, other))

    def __gt__(self, other: int | float) -> F.GreaterThan:
        return F.GreaterThan(self.name, other)

    def __ge__(self, other: int | float) -> F.GreaterThanOrEqual:
        return F.GreaterThanOrEqual(self.name, other)

    def __lt__(self, other: int | float) -> F.LessThan:
        return F.LessThan(self.name, other)

    def __le__(self, other: int | float) -> F.LessThanOrEqual:
        return F.LessThanOrEqual(self.name, other)

    def list_contains(self, value: str) -> F.ListContains:
        return F.ListContains(self.name, value)

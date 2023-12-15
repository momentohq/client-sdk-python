import pytest
from momento.requests.vector_index import Field, FilterExpression
from momento.requests.vector_index import filters as F
from momento_wire_types import vectorindex_pb2 as pb


@pytest.mark.parametrize(
    ["field_based_expression", "full_expression", "protobuf"],
    [
        (
            Field("foo") == "bar",
            F.Equals("foo", "bar"),
            pb._FilterExpression(equals_expression=pb._EqualsExpression(field="foo", string_value="bar")),
        ),
        (
            Field("foo"),
            F.Equals("foo", True),
            pb._FilterExpression(equals_expression=pb._EqualsExpression(field="foo", boolean_value=True)),
        ),
        (
            ~Field("foo"),
            F.Equals("foo", False),
            pb._FilterExpression(equals_expression=pb._EqualsExpression(field="foo", boolean_value=False)),
        ),
        (
            Field("foo") == 5.5,
            F.Equals("foo", 5.5),
            pb._FilterExpression(equals_expression=pb._EqualsExpression(field="foo", float_value=5.5)),
        ),
        (
            Field("foo") != "bar",
            F.Not(F.Equals("foo", "bar")),
            pb._FilterExpression(
                not_expression=pb._NotExpression(
                    expression_to_negate=pb._FilterExpression(
                        equals_expression=pb._EqualsExpression(field="foo", string_value="bar")
                    )
                )
            ),
        ),
        (
            ~(Field("foo") == "bar"),
            F.Not(F.Equals("foo", "bar")),
            pb._FilterExpression(
                not_expression=pb._NotExpression(
                    expression_to_negate=pb._FilterExpression(
                        equals_expression=pb._EqualsExpression(field="foo", string_value="bar")
                    )
                )
            ),
        ),
        (
            Field("foo") > 5,
            F.GreaterThan("foo", 5),
            pb._FilterExpression(greater_than_expression=pb._GreaterThanExpression(field="foo", integer_value=5)),
        ),
        (
            Field("foo") > 5.5,
            F.GreaterThan("foo", 5.5),
            pb._FilterExpression(greater_than_expression=pb._GreaterThanExpression(field="foo", float_value=5.5)),
        ),
        (
            Field("foo") >= 5,
            F.GreaterThanOrEqual("foo", 5),
            pb._FilterExpression(
                greater_than_or_equal_expression=pb._GreaterThanOrEqualExpression(field="foo", integer_value=5)
            ),
        ),
        (
            Field("foo") >= 5.5,
            F.GreaterThanOrEqual("foo", 5.5),
            pb._FilterExpression(
                greater_than_or_equal_expression=pb._GreaterThanOrEqualExpression(field="foo", float_value=5.5)
            ),
        ),
        (
            Field("foo") < 5,
            F.LessThan("foo", 5),
            pb._FilterExpression(less_than_expression=pb._LessThanExpression(field="foo", integer_value=5)),
        ),
        (
            Field("foo") < 5.5,
            F.LessThan("foo", 5.5),
            pb._FilterExpression(less_than_expression=pb._LessThanExpression(field="foo", float_value=5.5)),
        ),
        (
            Field("foo") <= 5,
            F.LessThanOrEqual("foo", 5),
            pb._FilterExpression(
                less_than_or_equal_expression=pb._LessThanOrEqualExpression(field="foo", integer_value=5)
            ),
        ),
        (
            Field("foo") <= 5.5,
            F.LessThanOrEqual("foo", 5.5),
            pb._FilterExpression(
                less_than_or_equal_expression=pb._LessThanOrEqualExpression(field="foo", float_value=5.5)
            ),
        ),
        (
            Field("foo").list_contains("bar"),
            F.ListContains("foo", "bar"),
            pb._FilterExpression(list_contains_expression=pb._ListContainsExpression(field="foo", string_value="bar")),
        ),
        (
            (Field("foo") == "bar") & (Field("baz") == 5),
            F.And(F.Equals("foo", "bar"), F.Equals("baz", 5)),
            pb._FilterExpression(
                and_expression=pb._AndExpression(
                    first_expression=pb._FilterExpression(
                        equals_expression=pb._EqualsExpression(field="foo", string_value="bar")
                    ),
                    second_expression=pb._FilterExpression(
                        equals_expression=pb._EqualsExpression(field="baz", integer_value=5)
                    ),
                )
            ),
        ),
        (
            (Field("foo") == "bar") | (Field("baz") == 5),
            F.Or(F.Equals("foo", "bar"), F.Equals("baz", 5)),
            pb._FilterExpression(
                or_expression=pb._OrExpression(
                    first_expression=pb._FilterExpression(
                        equals_expression=pb._EqualsExpression(field="foo", string_value="bar")
                    ),
                    second_expression=pb._FilterExpression(
                        equals_expression=pb._EqualsExpression(field="baz", integer_value=5)
                    ),
                )
            ),
        ),
    ],
)
def test_field_shorthand(
    field_based_expression: FilterExpression, full_expression: FilterExpression, protobuf: pb._FilterExpression
) -> None:
    assert field_based_expression == full_expression
    assert field_based_expression.to_filter_expression_proto() == protobuf
    assert full_expression.to_filter_expression_proto() == protobuf

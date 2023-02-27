import pytest

import momento.internal.codegen as codegen


@pytest.mark.parametrize(
    "input_, expected",
    [
        (
            """
async def calc():
    return await do_calc()
""",
            """
def calc():
    return do_calc()
""",
        ),
        (
            """
class A:
    async def action(self):
        result = await act()
        return result
""",
            """
class A:
    def action(self):
        result = act()
        return result
""",
        ),
        (
            """
TFunction = Callable[[str, int], Awaitable[ErrorResponseMixin]]
""",
            """
TFunction = Callable[[str, int], ErrorResponseMixin]
""",
        ),
        (
            """
from typing import Awaitable, BC, CD
""",
            """
from typing import BC, CD
""",
        ),
        (
            """
from typing import Awaitable
""",
            """
""",
        ),
        ("2+2", "2+2"),
        (
            """
async with slow_func():
    pass
""",
            """
with slow_func():
    pass
""",
        ),
    ],
)
def test_async_to_sync(input_: str, expected: str) -> None:
    async_to_sync = codegen.AsyncToSyncTransformer()
    input_module = codegen.parse_module(input_)
    output_module = input_module.visit(async_to_sync)
    output_program = output_module.code
    assert expected == output_program


@pytest.mark.parametrize(
    "input_, expected",
    [
        (
            """
def async_func():
    pass
""",
            """
def func():
    pass
""",
        ),
        (
            """
def wrap_async_with_stuff():
    pass
""",
            """
def wrap_with_stuff():
    pass
""",
        ),
        (
            """
def ok():
    pass
""",
            """
def ok():
    pass
""",
        ),
    ],
)
def test_name_replacement(input_: str, expected: str) -> None:
    async_to_sync = codegen.name_replacements
    input_module = codegen.parse_module(input_)
    output_module = input_module.visit(async_to_sync)
    output_program = output_module.code
    assert expected == output_program


@pytest.mark.parametrize(
    "input_, expected",
    [
        (
            """
class A:
    \"\"\"Async Cache Client\"\"\"
""",
            """
class A:
    \"\"\"Synchronous Cache Client\"\"\"
""",
        ),
        (
            """
class A:
    \"\"\"Here is an example: result = await call()\"\"\"
""",
            """
class A:
    \"\"\"Here is an example: result = call()\"\"\"
""",
        ),
        (
            """
class A:
    \"\"\"Here is an example: result = await call()
    result2 = await call2()\"\"\"
""",
            """
class A:
    \"\"\"Here is an example: result = call()
    result2 = call2()\"\"\"
""",
        ),
    ],
)
def test_simple_string_replacement(input_: str, expected: str) -> None:
    async_to_sync = codegen.simple_string_replacements
    input_module = codegen.parse_module(input_)
    output_module = input_module.visit(async_to_sync)
    output_program = output_module.code
    assert expected == output_program

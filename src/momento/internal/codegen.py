"""Generate synchronous modules from asynchronous modules.

Because many synchronous modules are identical to the asynchronous ones, modulo the async
verbiage, we opt for code generation to produce parts of the synchronous code. Importantly,
by doing this we reduce errors and increase productivity.

This module leverages Instagram's libcst library to parse code into an abstract syntax tree,
traverse and transform the tree, and emit the corresponding code. To produce the synchronous
code from the async code, we do the following transformations:
- convert async functions into synchronous ones,
- convert async context managers into synchronous ones,
- lift expressions from an await expression
- perform adhoc name replacements
"""
import argparse
import re
from typing import List, Tuple

import libcst as cst


class AsyncToSyncTransformer(cst.CSTTransformer):
    """Convert async methods, async context managers, and awaited expressions into synchronous ones."""

    def leave_FunctionDef(self, original_node: cst.FunctionDef, updated_node: cst.FunctionDef) -> cst.BaseStatement:
        updated_node = updated_node.with_changes(asynchronous=None)
        return updated_node

    def leave_Await(self, original_node: cst.Await, updated_node: cst.BaseExpression) -> cst.BaseExpression:
        return original_node.expression

    def leave_With(self, original_node: cst.With, updated_node: cst.With) -> cst.With:
        updated_node = updated_node.with_changes(asynchronous=None)
        return updated_node


class NameReplacement(cst.CSTTransformer):
    """Applies regex-based substitutions to names in the AST."""

    def __init__(self, substitutions: List[Tuple[str, str]]) -> None:
        """Instantiate a `NameReplacement`

        Args:
            substitutions (List[Tuple[str, str]]): A list of regex (string) and replacements to apply, in order.
        """
        self.substitutions = [(re.compile(pattern_), substitution) for pattern_, substitution in substitutions]

    def leave_Name(self, original_node: cst.Name, updated_node: cst.Name) -> cst.Name:
        name = original_node.value

        for pattern, replacement in self.substitutions:
            name = pattern.sub(replacement, name)

        updated_node = updated_node.with_changes(value=name)
        return updated_node


class Pipeline:
    """Runs a list of `cst.CSTTransformer`s in sequence."""

    def __init__(self, transformers: List[cst.CSTTransformer]) -> None:
        self.transformers = transformers

    def transform(self, input_module: cst.Module) -> cst.Module:
        for transformer in self.transformers:
            input_module = input_module.visit(transformer)
        return input_module


name_replacements = NameReplacement(
    [
        ("^async_(.*)", "\\1"),
        ("(.*?)_async_(.*)", "\\1_\\2"),
        ("(.*?)_async$", "\\1"),
        ("^(SimpleCacheClient)Async$", "\\1"),
        ("__aenter__", "__enter__"),
        ("__aexit__", "__exit__"),
        ("^aio$", "synchronous"),
    ]
)
canonical_pipeline = Pipeline([AsyncToSyncTransformer(), name_replacements])


def read_file(filepath: str) -> str:
    with open(filepath, "r") as fd:
        return fd.read()


def write_file(contents: str, filepath: str) -> None:
    with open(filepath, "w") as fd:
        print(contents, file=fd, end="")


def parse_module(input_program: str) -> cst.Module:
    return cst.parse_module(input_program)


def parse_clargs() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Port asynchronous client to synchronous client.")
    parser.add_argument("input", type=str, help="Path to input file.")
    parser.add_argument("output", type=str, help="Path to output file.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_clargs()

    input_contents = read_file(args.input)  # type: ignore
    parsed_input = parse_module(input_contents)
    output_contents = canonical_pipeline.transform(parsed_input).code
    write_file(output_contents, args.output)  # type: ignore

import argparse
import re
from typing import List, Tuple

import libcst as cst


class AsyncToSyncTransformer(cst.CSTTransformer):
    def leave_FunctionDef(self, original_node: cst.FunctionDef, updated_node: cst.FunctionDef) -> cst.BaseStatement:
        updated_node = updated_node.with_changes(asynchronous=None)
        return updated_node

    def leave_Await(self, original_node: cst.Await, updated_node: cst.BaseExpression) -> cst.BaseExpression:
        return original_node.expression

    def leave_With(self, original_node: cst.With, updated_node: cst.With) -> cst.With:
        updated_node = updated_node.with_changes(asynchronous=None)
        return updated_node


class NameReplacement(cst.CSTTransformer):
    def __init__(self, substitutions: List[Tuple[str, str]]) -> None:
        self.substitutions = [(re.compile(pattern_), substitution) for pattern_, substitution in substitutions]

    def leave_Name(self, original_node: cst.Name, updated_node: cst.Name) -> cst.Name:
        name = original_node.value

        for pattern, replacement in self.substitutions:
            name = pattern.sub(replacement, name)

        updated_node = updated_node.with_changes(value=name)
        return updated_node


class Pipeline:
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

#!/bin/sh

cp pyproject.toml pyproject.toml.real
cp pyproject.toml.tmp pyproject.toml
poetry update momento
cp pyproject.toml pyproject.toml.tmp
cp pyproject.toml.real pyproject.toml
poetry update momento


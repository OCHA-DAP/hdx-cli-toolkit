#!/usr/bin/env python
# encoding: utf-8

from click.testing import CliRunner
from hdx_cli_toolkit.cli import list_datasets


def test_list_datasets():
    command = list_datasets
    cli_arguments = [
        "--organisation=healthsites",
        "--dataset_filter=mali-healthsites",
        "--hdx_site=stage",
        "--key=private",
    ]

    expected_output = (
        "Found 1 datasets for organisation 'Global Healthsites Mapping Project (healthsites)' "
        "matching filter conditions:"
    )

    cli_test_template(command, cli_arguments, expected_output, forbidden_output="")


def cli_test_template(command, cli_arguments, expected_output, forbidden_output=""):
    runner = CliRunner()
    result = runner.invoke(command, cli_arguments)

    print(f"CLI output:\n {result.output}", flush=True)
    print(f"CLI exceptions: \n {result.exception}", flush=True)

    assert result.exception is None
    assert expected_output in result.output

    if forbidden_output != "":
        assert forbidden_output not in str(result.exception)

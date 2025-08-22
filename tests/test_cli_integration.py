#!/usr/bin/env python
# encoding: utf-8
import pytest
from click.testing import CliRunner

from hdx_cli_toolkit.cli import (
    show_configuration,
    download,
    get_organization_metadata,
    get_user_metadata,
    list_datasets,
    print_datasets,
)


def test_help():
    command = list_datasets
    cli_arguments = ["--help"]

    expected_output = "--organization TEXT"
    cli_test_template(command, cli_arguments, expected_output, forbidden_output="")


@pytest.mark.skip(reason="This requires a live stage key to pass")
def test_configuration():
    command = show_configuration
    cli_arguments = []

    # As I recall the CLI test runner works in isolation so no file system or environment variable
    # available for API keys to be stored
    expected_outputs = [
        "Values of relevant environment variables (used in absence of supplied values):",
        ' url: "https://stage.data-humdata-org.ahconu.org"',
        "API key valid on 'stage' to create datasets for organization 'hdx'",
    ]

    cli_test_template(command, cli_arguments, expected_outputs, forbidden_output="")


@pytest.mark.skip(
    reason="This works locally but not in GitHUb Actions, probably because it needs a prod api key"
)
def test_download():
    command = download
    cli_arguments = ["--dataset=bangladesh-bgd-attacks-on-protection", "--hdx_site=stage"]
    expected_outputs = [
        "2020-2023 BGD Protection in Danger Incident Data.xlsx",
    ]

    cli_test_template(command, cli_arguments, expected_outputs, forbidden_output="")


def test_get_organization_metadata():
    command = get_organization_metadata
    cli_arguments = ["--organization=hdx"]

    expected_outputs = [
        "hdx-collaboration",
        "bb161f77-39f4-433c-96ac-1df48b67454d",
    ]

    cli_test_template(command, cli_arguments, expected_outputs, forbidden_output="")


@pytest.mark.skip(
    reason="This requires a key with admin privileges to access abitrary userr metadata"
)
def test_get_user_metadata():
    command = get_user_metadata
    cli_arguments = ["--user=hopkinson", "--verbose"]

    expected_outputs = [
        "{",
        '"id": "972627a5-4f23',
    ]

    cli_test_template(command, cli_arguments, expected_outputs, forbidden_output="")


def test_list_datasets():
    command = list_datasets
    cli_arguments = [
        "--organization=healthsites",
        "--dataset_filter=malawi-healthsites",
        "--hdx_site=stage",
        "--key=private",
    ]

    expected_outputs = [
        (
            "Found 1 datasets for organization 'Global Healthsites Mapping Project (healthsites)' "
            "matching filter conditions:"
        )
    ]

    cli_test_template(command, cli_arguments, expected_outputs, forbidden_output="")


def test_print():
    command = print_datasets
    cli_arguments = [
        "--dataset_filter=wfp-food-prices-for-nigeria",
        "--with_extras",
    ]

    expected_outputs = ['"resources"', '"quickcharts"', '"showcases"']

    cli_test_template(command, cli_arguments, expected_outputs, forbidden_output="")


def cli_test_template(command, cli_arguments, expected_outputs, forbidden_output=""):
    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(command, cli_arguments)

        print(f"CLI output:\n {result.output}", flush=True)
        print(f"CLI exceptions: \n {result.exception}", flush=True)

        assert result.exception is None
        for expected_output in expected_outputs:
            assert expected_output.lower() in result.output.lower()

        if forbidden_output != "":
            assert forbidden_output not in str(result.exception)

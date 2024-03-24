#!/usr/bin/env python
# encoding: utf-8

import os
from unittest import mock
from click.testing import CliRunner

from hdx.data.dataset import Dataset
from hdx.api.configuration import Configuration, ConfigurationError
from hdx_cli_toolkit.cli import list_datasets

try:
    Configuration.create(
        user_agent_config_yaml=os.path.join(os.path.expanduser("~"), ".useragents.yaml"),
        user_agent_lookup="hdx-cli-toolkit",
        hdx_site="stage",
        hdx_read_only=False,
    )
except ConfigurationError:
    pass


@mock.patch("hdx.data.organization.Organization.get_datasets")
def test_list_datasets(mock_hdx, json_fixture):
    # mock_hdx.return_value = [{"name": "mali-healthsites", "private": True}]
    mock_datasets_json = json_fixture("healthsites.json")

    mock_datasets = []
    for mock_dataset_json in mock_datasets_json:
        mock_datasets.append(Dataset(initial_data=mock_dataset_json))

    mock_hdx.return_value = mock_datasets
    command = list_datasets
    cli_arguments = [
        "--organization=healthsites",
        "--dataset_filter=malawi-healthsites",
        "--hdx_site=stage",
        "--key=private",
    ]

    expected_output = (
        "Found 1 datasets for organization 'Global Healthsites Mapping Project (healthsites)' "
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

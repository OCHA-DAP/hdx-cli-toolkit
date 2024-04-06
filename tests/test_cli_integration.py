#!/usr/bin/env python
# encoding: utf-8

from click.testing import CliRunner

from hdx_cli_toolkit.cli import list_datasets, show_configuration, download

# These are the commandline tests:


# hdx-toolkit list --organization=healthsites --dataset_filter=*al*-healthsites --hdx_site=stage --key=private --value=True
# hdx-toolkit list --organization=international-organization-for-migration --key=data_update_frequency,dataset_date --output_path=list-test-1.csv
# rm list-test-1.csv
# hdx-toolkit list --query=archived:true --key=owner_org --output_path=list-test-2.csv
# rm list-test-2.csv
# hdx-toolkit get_organization_metadata --organization=zurich
# hdx-toolkit get_organization_metadata --organization=eth-zurich-weather-and-climate-risks --verbose
# hdx-toolkit get_user_metadata --user=hopkinson
# hdx-toolkit get_user_metadata --user=hopkinson --verbose
# hdx-toolkit print --dataset_filter=climada-litpop-dataset
# hdx-toolkit print --dataset_filter=wfp-food-prices-for-nigeria --with_extras


def test_help():
    command = list_datasets
    cli_arguments = ["--help"]

    expected_output = "--organization TEXT"
    cli_test_template(command, cli_arguments, expected_output, forbidden_output="")


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


def test_configuration():
    command = show_configuration
    cli_arguments = []

    expected_outputs = ['hdx_key: "*****', ' url: "https://stage.data-humdata-org.ahconu.org"']

    cli_test_template(command, cli_arguments, expected_outputs, forbidden_output="")


def test_download():
    command = download
    cli_arguments = ["--dataset=bangladesh-bgd-attacks-on-protection", "--hdx_site=stage"]
    expected_outputs = [
        "2020-2023 BGD Protection in Danger Incident Data.xlsx",
        "2020-2024-BGD-protection-incident-data.xlsx",
        "2020-2024-BGD Protection Incident Data.xlsx",
    ]

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

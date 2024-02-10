#!/usr/bin/env python
# encoding: utf-8

import datetime
import fnmatch
import json
import os
import time
from collections.abc import Callable

import click
from click.decorators import FC

from hdx.api.configuration import Configuration, ConfigurationError
from hdx.data.hdxobject import HDXError
from hdx.data.dataset import Dataset
from hdx.data.organization import Organization
from hdx.data.user import User
from hdx.utilities.path import script_dir_plus_file

from hdx_cli_toolkit.utilities import (
    write_dictionary,
    print_table_from_list_of_dicts,
    censor_secret,
    make_conversion_func,
)


@click.group()
def hdx_toolkit() -> None:
    """Tools for Commandline interactions with HDX"""


OPTIONS = [
    click.option(
        "--organization",
        is_flag=False,
        default="",
        help="an organization name",
    ),
    click.option(
        "--key",
        is_flag=False,
        default="private",
        help="a single key to list alongside organization",
    ),
    click.option(
        "--value",
        is_flag=False,
        default=None,
        help="a value to set",
    ),
    click.option(
        "--dataset_filter",
        is_flag=False,
        default="*",
        help="a dataset name or pattern on which to filter list",
    ),
    click.option(
        "--query",
        is_flag=False,
        default=None,
        help=(
            "a dataset query string to pass to CKAN, "
            "organization and dataset_filter are ignored if it is provided"
        ),
    ),
    click.option(
        "--hdx_site",
        is_flag=False,
        default="stage",
        help="an hdx_site value {stage|prod}",
    ),
]


def multi_decorator(options: list[Callable[[FC], FC]]) -> Callable[[FC], FC]:
    def decorator(f: FC) -> FC:
        for option in reversed(options):
            f = option(f)

        return f

    return decorator


@hdx_toolkit.command(name="list")
@multi_decorator(OPTIONS)
@click.option(
    "--output_path",
    is_flag=False,
    default=None,
    help="A file path to export data from list to CSV",
)
def list_datasets(
    organization: str = "",
    key: str = "private",
    value: str = "value",
    dataset_filter: str = "*",
    query: str = None,
    hdx_site: str = "stage",
    output_path: str = None,
):
    """List datasets in HDX"""
    print_banner("list")

    filtered_datasets = get_filtered_datasets(
        organization=organization,
        key=key,
        value=value,
        dataset_filter=dataset_filter,
        query=query,
        hdx_site=hdx_site,
    )

    keys = key.split(",")
    output_template = {"dataset_name": ""}
    for key_ in keys:
        output_template[key_] = ""

    output = []
    for dataset in filtered_datasets:
        output_row = output_template.copy()
        output_row["dataset_name"] = dataset["name"]
        for key_ in keys:
            output_row[key_] = dataset.get(key_, "")
        output.append(output_row)

    print_table_from_list_of_dicts(output)
    if output_path is not None:
        status = write_dictionary(output_path, output, append=False)
        print(status, flush=True)


@hdx_toolkit.command(name="update")
@multi_decorator(OPTIONS)
def update(
    organization: str = "",
    key: str = "private",
    value: str = "value",
    dataset_filter: str = "*",
    query: str = None,
    hdx_site: str = "stage",
):
    """Update datasets in HDX"""
    print_banner("Update")
    filtered_datasets = get_filtered_datasets(
        organization=organization,
        key=key,
        value=value,
        dataset_filter=dataset_filter,
        query=query,
        hdx_site=hdx_site,
    )

    if len(filtered_datasets) == 0:
        print("Specified filter returns no datasets", flush=True)
        return

    print(f"Updating key '{key}' with value '{value}'")
    conversion_func, type_name = make_conversion_func(filtered_datasets[0][key])
    if conversion_func is None:
        print(f"Type name '{type_name}' is not recognised, aborting", flush=True)
        return

    print(f"Detected value type is '{type_name}'", flush=True)
    print(
        f"{'dataset_name':<70.70}{'old value':<20.20}{'new value':<20.20}"
        f"{'Time to update/seconds':<25.25}",
        flush=True,
    )
    n_changed = 0
    n_failures = 0
    for dataset in filtered_datasets:
        t0 = time.time()
        old_value = str(dataset[key])
        dataset[key] = conversion_func(value)
        if old_value != str(dataset[key]):
            n_changed += 1
        else:
            print(
                f"{dataset['name']:<70.70}{old_value:<20.20}{str(dataset[key]):<20.20}"
                f"{'No update required':<25.25}",
                flush=True,
            )
            continue
        try:
            dataset.update_in_hdx(
                update_resources=False,
                hxl_update=False,
                operation="patch",
                batch_mode="KEEP_OLD",
                skip_validation=True,
                ignore_check=True,
            )
        except (HDXError, KeyError):
            n_failures += 0
            print(f"Could not update {dataset['name']}")
        print(
            f"{dataset['name']:<70.70}{old_value:<20.20}{str(dataset[key]):<20.20}"
            f"{time.time()-t0:0.2f}",
            flush=True,
        )

    print(f"Changed {n_changed} values", flush=True)
    print(f"{n_failures} failures as evidenced by HDXError", flush=True)


@hdx_toolkit.command(name="print")
@multi_decorator(OPTIONS)
def print_datasets(
    organization: str = "healthsites",
    key: str = "private",
    value: str = "value",
    dataset_filter: str = "*",
    query: str = None,
    hdx_site: str = "stage",
):
    """Print datasets in HDX to the terminal"""

    filtered_datasets = get_filtered_datasets(
        organization=organization,
        key=key,
        value=value,
        dataset_filter=dataset_filter,
        hdx_site=hdx_site,
        query=query,
        verbose=False,
    )

    print("[", flush=True)
    for i, dataset in enumerate(filtered_datasets):
        print(json.dumps(dataset.data, indent=4), flush=True)
        if i != len(filtered_datasets) - 1:
            print(",", flush=True)
    print("]", flush=True)


@hdx_toolkit.command(name="get_organization_metadata")
@click.option(
    "--organization",
    is_flag=False,
    default="",
    help="an organization name, wildcards are implicitly included",
)
@click.option(
    "--hdx_site",
    is_flag=False,
    default="stage",
    help="an hdx_site value {stage|prod}",
)
@click.option(
    "--verbose",
    is_flag=True,
    default=False,
    help="if true show all organization metadata",
)
def get_organization_metadata(organization: str, hdx_site: str = "stage", verbose: bool = False):
    """Get an organization id and other metadata"""
    print_banner("Get organization Metadata")
    try:
        Configuration.create(
            user_agent_config_yaml=os.path.join(os.path.expanduser("~"), ".useragents.yaml"),
            user_agent_lookup="hdx-cli-toolkit",
            hdx_site=hdx_site,
            hdx_read_only=False,
        )
    except ConfigurationError:
        pass

    all_organizations = Organization.get_all_organization_names(include_extras=True)
    for an_organization in all_organizations:
        if fnmatch.fnmatch(an_organization, f"*{organization}*"):
            organization_metadata = Organization.read_from_hdx(an_organization)
            if verbose:
                print(json.dumps(organization_metadata.data, indent=2), flush=True)
            else:
                print(
                    f"{organization_metadata['name']:<50.50}: {organization_metadata['id']}",
                    flush=True,
                )


@hdx_toolkit.command(name="get_user_metadata")
@click.option(
    "--user",
    is_flag=False,
    default="",
    help="a user name or email, wildcards are implicitly included",
)
@click.option(
    "--hdx_site",
    is_flag=False,
    default="stage",
    help="an hdx_site value {stage|prod}",
)
@click.option(
    "--verbose",
    is_flag=True,
    default=False,
    help="if true show all user metadata",
)
def get_user_metadata(user: str, hdx_site: str = "stage", verbose: bool = False):
    """Get user id and other metadata"""
    print_banner("Get User Metadata")
    try:
        Configuration.create(
            user_agent_config_yaml=os.path.join(os.path.expanduser("~"), ".useragents.yaml"),
            user_agent_lookup="hdx-cli-toolkit",
            hdx_site=hdx_site,
            hdx_read_only=False,
        )
    except ConfigurationError:
        pass

    user_list = User.get_all_users(q=user)
    for a_user in user_list:
        if verbose:
            print(json.dumps(a_user.data, indent=2), flush=True)
        else:
            print(
                f"{a_user['name']:<50.50}: {a_user['id']}",
                flush=True,
            )


@hdx_toolkit.command(name="configuration")
def show_configuration():
    print_banner("configuration")
    # Check files
    user_hdx_config_yaml = os.path.join(os.path.expanduser("~"), ".hdx_configuration.yaml")
    default_hdx_config_yaml = script_dir_plus_file(
        "hdx_base_configuration.yaml", ConfigurationError
    )

    if os.path.exists(user_hdx_config_yaml):
        click.secho(
            f"Found a user configuration file at {user_hdx_config_yaml}. "
            "Contents (secrets censored):",
            bold=True,
        )
        with open(user_hdx_config_yaml, encoding="utf-8") as config_file:
            config_file_contents = config_file.read()
            rows = config_file_contents.split("\n")
            for row in rows:
                if row.startswith("hdx_key"):
                    key_part, secret_part = row.split(":")
                    secret_part = censor_secret(secret_part)
                    row = key_part + ': "' + secret_part
                print(row, flush=True)

    user_agent_config_yaml = os.path.join(os.path.expanduser("~"), ".useragents.yaml")
    if os.path.exists(user_agent_config_yaml):
        click.secho(f"User agents file found at {user_agent_config_yaml}", bold=True)
        with open(user_agent_config_yaml, encoding="utf-8") as config_file:
            user_agents_file_contents = config_file.read()
            print(user_agents_file_contents, flush=True)

    # Check Environment variables
    environment_variables = ["HDX_KEY", "HDX_SITE", "HDX_URL"]
    click.secho(
        "Values of relevant environment variables (used in absence of supplied values):", bold=True
    )
    for variable in environment_variables:
        env_variable = os.getenv(variable)
        if env_variable is not None:
            if variable == "HDX_KEY":
                env_variable = censor_secret(env_variable)
            print(f"{variable}:{env_variable}", flush=True)
        else:
            print(f"{variable} is not set", flush=True)

    click.secho(
        f"\nDefault base configuration file is at {default_hdx_config_yaml}. Contents:", bold=True
    )
    with open(default_hdx_config_yaml, encoding="utf-8") as config_file:
        config_file_contents = config_file.read()
        print(config_file_contents, flush=True)


def get_filtered_datasets(
    organization: str = "",
    key: str = "private",
    value: str = "value",
    dataset_filter: str = "*",
    query: str = None,
    hdx_site: str = "stage",
    verbose: bool = True,
) -> list[Dataset]:
    try:
        Configuration.create(
            user_agent_config_yaml=os.path.join(os.path.expanduser("~"), ".useragents.yaml"),
            user_agent_lookup="hdx-cli-toolkit",
            hdx_site=hdx_site,
            hdx_read_only=False,
        )
    except ConfigurationError:
        pass

    if organization != "":
        organization = Organization.read_from_hdx(organization)
        datasets = organization.get_datasets(include_private=True)
    elif query is not None:
        datasets = Dataset.search_in_hdx(query=query)
        organization = {"display_name": "", "name": ""}
    else:
        dataset = Dataset.read_from_hdx(dataset_filter)
        if dataset is None:
            datasets = []
            organization = {"display_name": "", "name": ""}
        else:
            datasets = [dataset]
            organization = dataset.get_organization()
            organization = {"display_name": organization["title"], "name": organization["name"]}

    filtered_datasets = []
    for dataset in datasets:
        if fnmatch.fnmatch(dataset["name"], dataset_filter):
            filtered_datasets.append(dataset)

    if verbose:
        print(Configuration.read().hdx_site, flush=True)
        print(
            f"Found {len(filtered_datasets)} datasets for organization "
            f"'{organization['display_name']} "
            f"({organization['name']})' matching filter conditions:",
            flush=True,
        )

    return filtered_datasets


def print_banner(action: str):
    title = f"HDX CLI toolkit - {action}"
    timestamp = f"Invoked at: {datetime.datetime.now().isoformat()}"
    width = max(len(title), len(timestamp))
    click.secho((width + 4) * "*", bold=True)
    click.secho(f"* {title:<{width}} *", bold=True)
    click.secho(f"* {timestamp:<{width}} *", bold=True)
    click.secho((width + 4) * "*", bold=True)

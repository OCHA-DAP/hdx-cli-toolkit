#!/usr/bin/env python
# encoding: utf-8

import json
import os
import time

from collections.abc import Callable

import click
from click.decorators import FC

from hdx.api.configuration import ConfigurationError
from hdx.utilities.path import script_dir_plus_file

from hdx_cli_toolkit.utilities import (
    write_dictionary,
    print_table_from_list_of_dicts,
    censor_secret,
    make_conversion_func,
    print_banner,
)

from hdx_cli_toolkit.hdx_utilities import (
    add_showcase,
    add_quickcharts,
    get_organizations_from_hdx,
    get_users_from_hdx,
    update_values_in_hdx,
    configure_hdx_connection,
    update_resource_in_hdx,
    get_filtered_datasets,
    decorate_dataset_with_extras,
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
    n_changed, n_failures = update_values_in_hdx(
        filtered_datasets, key, value, conversion_func, hdx_site=hdx_site
    )

    print(f"Changed {n_changed} values", flush=True)
    print(f"{n_failures} failures as evidenced by HDXError", flush=True)


@hdx_toolkit.command(name="print")
@multi_decorator(OPTIONS)
@click.option(
    "--with_extras",
    is_flag=True,
    default=False,
    help=(
        "If set resources, resource_views (QuickCharts) "
        "and Showcases are added to the dataset output"
    ),
)
def print_datasets(
    organization: str = "healthsites",
    key: str = "private",
    value: str = "value",
    dataset_filter: str = "*",
    query: str = None,
    hdx_site: str = "stage",
    with_extras: bool = False,
):
    """Print datasets in HDX to the terminal"""

    filtered_datasets = get_filtered_datasets(
        organization=organization,
        dataset_filter=dataset_filter,
        hdx_site=hdx_site,
        query=query,
        verbose=False,
    )

    print("[", flush=True)
    for i, dataset in enumerate(filtered_datasets):
        output_dict = dataset.data
        if with_extras:
            output_dict = decorate_dataset_with_extras(dataset)

        print(json.dumps(output_dict, indent=4), flush=True)
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
    configure_hdx_connection(hdx_site=hdx_site)

    filtered_organizations = get_organizations_from_hdx(organization, hdx_site=hdx_site)
    for filtered_organization in filtered_organizations:
        if verbose:
            print(json.dumps(filtered_organization.data, indent=2), flush=True)
        else:
            print(
                f"{filtered_organization['name']:<50.50}: {filtered_organization['id']}",
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

    user_list = get_users_from_hdx(user, hdx_site=hdx_site)
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
    """Print configuration information to terminal"""
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
    environment_variables = ["HDX_KEY", "HDX_KEY_STAGE", "HDX_SITE", "HDX_URL"]
    click.secho(
        "Values of relevant environment variables (used in absence of supplied values):", bold=True
    )
    for variable in environment_variables:
        env_variable = os.getenv(variable)
        if env_variable is not None:
            if "HDX_KEY" in variable:
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


@hdx_toolkit.command(name="quickcharts")
@click.option(
    "--dataset_filter",
    is_flag=False,
    default="*",
    help="a dataset name",
)
@click.option(
    "--hdx_site",
    is_flag=False,
    default="stage",
    help="an hdx_site value {stage|prod}",
)
@click.option(
    "--resource_name",
    is_flag=False,
    default="stage",
    help="name of resource to which the QuickCharts are attached",
)
@click.option(
    "--hdx_hxl_preview_file_path",
    is_flag=False,
    default="stage",
    help="name of resource to which the QuickCharts are attached",
)
def quickcharts(
    dataset_filter: str = "",
    hdx_site: str = "stage",
    resource_name: str = "",
    hdx_hxl_preview_file_path: str = "",
):
    """Upload QuickChart JSON description to HDX"""
    print_banner("quickcharts")
    print(
        f"Adding Quick Chart defined at '{hdx_hxl_preview_file_path}' to dataset "
        f"'{dataset_filter}', resource '{resource_name}'"
    )
    t0 = time.time()
    status = add_quickcharts(dataset_filter, hdx_site, resource_name, hdx_hxl_preview_file_path)

    print(status, flush=True)

    print(f"Quick Chart update took {time.time() - t0:.2f} seconds")


@hdx_toolkit.command(name="showcase")
@click.option(
    "--showcase_name",
    is_flag=False,
    default="*",
    help="showcase name",
)
@click.option(
    "--hdx_site",
    is_flag=False,
    default="stage",
    help="an hdx_site value {stage|prod}",
)
@click.option(
    "--attributes_file_path",
    is_flag=False,
    default="stage",
    help="path to the attributes file describing the showcase",
)
def showcase(
    showcase_name: str = "",
    hdx_site: str = "stage",
    attributes_file_path: str = "",
):
    """Upload showcase to HDX"""
    print_banner("showcase")
    print(f"Adding showcase defined at '{attributes_file_path}'")
    t0 = time.time()
    statuses = add_showcase(showcase_name, hdx_site, attributes_file_path)
    for status in statuses:
        print(status, flush=True)

    print(f"Showcase update took {time.time() - t0:.2f} seconds")


@hdx_toolkit.command(name="update_resource")
@click.option(
    "--dataset_name",
    is_flag=False,
    default="*",
    help="name of the dataset to update",
)
@click.option(
    "--resource_name",
    is_flag=False,
    default="*",
    help="name of the resource in the dataset to update",
)
@click.option(
    "--hdx_site",
    is_flag=False,
    default="stage",
    help="an hdx_site value {stage|prod}",
)
@click.option(
    "--resource_file_path",
    is_flag=False,
    default="stage",
    help="path to the resource file to upload",
)
@click.option(
    "--live",
    is_flag=True,
    default=False,
    help="if present then update to HDX is made, if absent then a dry run is done",
)
@click.option(
    "--description",
    is_flag=False,
    default="new resource",
    help="if the resource is to be added, rather than updated this provides the description",
)
def update_resource(
    dataset_name: str = "",
    resource_name: str = "",
    hdx_site: str = "stage",
    resource_file_path: str = "",
    live: bool = False,
    description: str = "new resource",
):
    """Update a resource in HDX"""
    print_banner("Update resource")
    print(
        f"Updating/adding '{resource_name}' in '{dataset_name}' "
        f"with file at '{resource_file_path}'"
    )
    t0 = time.time()
    statuses = update_resource_in_hdx(
        dataset_name, resource_name, hdx_site, resource_file_path, live, description=description
    )
    for status in statuses:
        print(status, flush=True)

    print(f"Resource update took {time.time() - t0:.2f} seconds")

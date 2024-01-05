#!/usr/bin/env python
# encoding: utf-8

import datetime
import fnmatch
import os
from collections.abc import Callable

import click
from click.decorators import FC

from hdx.api.configuration import Configuration, ConfigurationError
from hdx.data.dataset import Dataset
from hdx.data.organization import Organization


@click.group()
def hdx_toolkit() -> None:
    """Tools for Commandline interactions with HDX"""


# This method for bundling reusable options is borrowed from here:
#
OPTIONS = [
    click.option(
        "--organisation",
        is_flag=False,
        default="healthsites",
        help="an organisation name",
    ),
    click.option(
        "--key",
        is_flag=False,
        default="private",
        help="a single key to list alongside organisation",
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
def list_datasets(
    organisation: str = "healthsites",
    key: str = "private",
    value: str = "value",
    dataset_filter: str = "*",
    hdx_site: str = "stage",
):
    """List datasets in HDX"""
    print_banner("list")

    filtered_datasets = get_filtered_datasets(
        organisation=organisation,
        key=key,
        value=value,
        dataset_filter=dataset_filter,
        hdx_site=hdx_site,
    )

    print(f"{'dataset_name':<70.70}{key:<50.50}", flush=True)
    for dataset in filtered_datasets:
        print(f"{dataset['name']:<70.70}{str(dataset[key]):<50.50}", flush=True)


@hdx_toolkit.command(name="update")
@multi_decorator(OPTIONS)
def update(
    organisation: str = "healthsites",
    key: str = "private",
    value: str = "value",
    dataset_filter: str = "*",
    hdx_site: str = "stage",
):
    """Update datasets in HDX"""
    print_banner("Update")
    filtered_datasets = get_filtered_datasets(
        organisation=organisation,
        key=key,
        value=value,
        dataset_filter=dataset_filter,
        hdx_site=hdx_site,
    )
    print(f"Updating key '{key}' with value '{value}'")

    print(f"{'dataset_name':<70.70}{'old value':<20.20}{'new value':<20.20}", flush=True)
    n_changed = 0
    for dataset in filtered_datasets:
        old_value = str(dataset[key])
        value_type = type(dataset[key])
        if value_type.__name__ == "bool":
            dataset[key] = bool(value)
        elif value_type.__name__ == "int":
            dataset[key] = int(value)
        elif value_type.__name__ == "float":
            dataset[key] = float(value)
        elif value_type.__name__ == "str":
            dataset[key] = str(value)
        if old_value != str(dataset[key]):
            n_changed += 1
        print(f"{dataset['name']:<70.70}{old_value:<20.20}{str(dataset[key]):<20.20}", flush=True)

    print(f"Changed {n_changed} values")


def get_filtered_datasets(
    organisation: str = "healthsites",
    key: str = "private",
    value: str = "value",
    dataset_filter: str = "*",
    hdx_site: str = "stage",
) -> list[Dataset]:
    Configuration.create(
        user_agent_config_yaml=os.path.join(os.path.expanduser("~"), ".useragents.yaml"),
        user_agent_lookup="hdx-cli-toolkit",
        hdx_site=hdx_site,
    )
    organization = Organization.read_from_hdx(organisation)
    datasets = organization.get_datasets()
    filtered_datasets = []
    for dataset in datasets:
        if fnmatch.fnmatch(dataset["name"], dataset_filter):
            filtered_datasets.append(dataset)

    print(Configuration.read().hdx_site, flush=True)
    print(
        f"Found {len(filtered_datasets)} datasets for organisation '{organization['display_name']} "
        f"({organization['name']})' matching filter conditions:",
        flush=True,
    )

    return filtered_datasets


def print_banner(action: str):
    title = f"HDX CLI toolkit - {action}"
    timestamp = f"Invoked at: {datetime.datetime.now().isoformat()}"
    width = max(len(title), len(timestamp))
    print((width + 4) * "*")
    print(f"* {title:<{width}} *")
    print(f"* {timestamp:<{width}} *")
    print((width + 4) * "*")

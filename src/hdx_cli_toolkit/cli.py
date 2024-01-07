#!/usr/bin/env python
# encoding: utf-8

import datetime
import fnmatch
import json
import os
import time
from collections.abc import Callable
from typing import Any

import click
from click.decorators import FC

from hdx.api.configuration import Configuration, ConfigurationError
from hdx.data.hdxobject import HDXError
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
        default="",
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
    organisation: str = "",
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
    organisation: str = "",
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
        except HDXError:
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
    organisation: str = "healthsites",
    key: str = "private",
    value: str = "value",
    dataset_filter: str = "*",
    hdx_site: str = "stage",
):
    """Print datasets in HDX to the terminal"""

    filtered_datasets = get_filtered_datasets(
        organisation=organisation,
        key=key,
        value=value,
        dataset_filter=dataset_filter,
        hdx_site=hdx_site,
        verbose=False,
    )

    print("[", flush=True)
    for i, dataset in enumerate(filtered_datasets):
        print(json.dumps(dataset.data, indent=4), flush=True)
        if i != len(filtered_datasets) - 1:
            print(",", flush=True)
    print("]", flush=True)


def str_to_bool(x: str) -> bool:
    return x == "True"


def make_conversion_func(value: Any) -> (Callable | None, str):
    value_type = type(value)
    if value_type.__name__ == "bool":
        conversion_func = str_to_bool  # bool
    elif value_type.__name__ == "int":
        conversion_func = int
    elif value_type.__name__ == "float":
        conversion_func = float
    elif value_type.__name__ == "str":
        conversion_func = str
    else:
        conversion_func = None

    return conversion_func, value_type.__name__


def get_filtered_datasets(
    organisation: str = "healthsites",
    key: str = "private",
    value: str = "value",
    dataset_filter: str = "*",
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

    organisation = Organization.read_from_hdx(organisation)
    datasets = organisation.get_datasets(include_private=True)
    filtered_datasets = []
    for dataset in datasets:
        if fnmatch.fnmatch(dataset["name"], dataset_filter):
            filtered_datasets.append(dataset)

    if verbose:
        print(Configuration.read().hdx_site, flush=True)
        print(
            f"Found {len(filtered_datasets)} datasets for organisation "
            f"'{organisation['display_name']} "
            f"({organisation['name']})' matching filter conditions:",
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

#!/usr/bin/env python
# encoding: utf-8

import datetime
import fnmatch
import os

import click

from hdx.api.configuration import Configuration, ConfigurationError
from hdx.data.dataset import Dataset
from hdx.data.organization import Organization


@click.group()
def hdx_toolkit() -> None:
    """Tools for Commandline interactions with HDX"""


@hdx_toolkit.command(name="list")
@click.option(
    "--organisation",
    is_flag=False,
    default="healthsites",
    help="an organisation name",
)
@click.option(
    "--key",
    is_flag=False,
    default="private",
    help="a single key to list alongside organisation",
)
@click.option(
    "--dataset_filter",
    is_flag=False,
    default="*",
    help="a dataset name or pattern on which to filter list",
)
@click.option(
    "--hdx_site",
    is_flag=False,
    default="stage",
    help="an hdx_site value {stage|prod}",
)
def list_datasets(
    organisation: str = "healthsites",
    key: str = "private",
    dataset_filter: str = "*",
    hdx_site: str = "stage",
):
    """List datasets in HDX"""
    print_banner("list")
    Configuration.create(
        user_agent_config_yaml=os.path.join(os.path.expanduser("~"), ".useragents.yaml"),
        user_agent_lookup="hdx-cli-toolkit",
        hdx_site=hdx_site,
    )
    organization = Organization.read_from_hdx(organisation)
    datasets = organization.get_datasets()
    n_datasets = len(datasets)
    print(Configuration.read().hdx_site, flush=True)
    print(
        f"Found {n_datasets} datasets for organisation '{organization['display_name']} ({organization['name']})':",
        flush=True,
    )

    print(f"{'dataset_name':<70.70}{key:<50.50}", flush=True)
    for dataset in datasets:
        if fnmatch.fnmatch(dataset["name"], dataset_filter):
            print(f"{dataset['name']:<70.70}{str(dataset[key]):<50.50}", flush=True)


def print_banner(action: str):
    title = f"HDX CLI toolkit - {action}"
    timestamp = f"Invoked at: {datetime.datetime.now().isoformat()}"
    width = max(len(title), len(timestamp))
    print((width + 4) * "*")
    print(f"* {title:<{width}} *")
    print(f"* {timestamp:<{width}} *")
    print((width + 4) * "*")

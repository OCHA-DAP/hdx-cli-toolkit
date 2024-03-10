#!/usr/bin/env python
# encoding: utf-8

import os
import json
from hdx.api.configuration import Configuration, ConfigurationError
from hdx.data.dataset import Dataset
from hdx.data.showcase import Showcase

from hdx_cli_toolkit.utilities import read_attributes


def add_showcase(showcase_name: str, hdx_site: str, attributes_file_path: str) -> list[str]:
    configure_hdx_connection(hdx_site)
    statuses = []
    showcase_attributes = read_attributes(showcase_name, attributes_filepath=attributes_file_path)
    showcase = Showcase(
        {
            "name": showcase_attributes["name"],
            "title": showcase_attributes["title"],
            "notes": showcase_attributes["notes"],
            "url": showcase_attributes["url"],
            "image_url": showcase_attributes["image_url"],
        }
    )
    added_tags, unadded_tags = showcase.add_tags(showcase_attributes["tags"])
    statuses.append(f"{len(added_tags)} of {len(showcase_attributes['tags'])} showcase tags added")
    if len(unadded_tags) != 0:
        statuses.append(f"Rejected showcase tags: {unadded_tags}")

    showcase.create_in_hdx()
    dataset = Dataset.read_from_hdx(showcase_attributes["parent_dataset"])
    showcase.add_dataset(dataset)
    statuses.append(f"Added dataset '{dataset['name']}' to showcase '{showcase_name}'")

    return statuses


def update_resource_in_hdx(
    dataset_name: str, resource_name: str, hdx_site: str, resource_file_path: str, dry_run: bool
):
    configure_hdx_connection(hdx_site)
    statuses = []
    dataset = Dataset.read_from_hdx(dataset_name)
    # Check we found a dataset
    if dataset is None:
        statuses.append(f"No dataset with the name '{dataset_name}' found on HDX site '{hdx_site}'")
        return statuses
    else:
        statuses.append(f"Found dataset with the name '{dataset_name}' on HDX site '{hdx_site}'")
    # Check we found a resource
    resources = dataset.get_resources()
    resource_to_update = None
    for resource in resources:
        if resource["name"] == resource_name:
            resource_to_update = resource

    if resource_to_update is None:
        statuses.append(
            f"No resource with the name '{resource_name}' found on dataset '{dataset_name}'"
        )
        # Make a new resource
        return statuses

    # Check the file provided is a reasonable alternative to the original
    if not os.path.exists(resource_file_path):
        statuses.append(f"No file found at file path '{resource_file_path}'")
        return statuses
    else:
        statuses.append(f"Found file to upload at '{resource_file_path}'")

    url = resource_to_update["url"]
    original_filename = url[(url.rfind("/") + 1) :]  # noqa: E203
    original_size = resource_to_update["size"]
    statuses.append(f"Original resource filename '{original_filename}' with size {original_size}")

    replacement_filename = os.path.basename(resource_file_path)
    file_stats = os.stat(resource_file_path)
    replacement_size = file_stats.st_size
    statuses.append(
        f"Replacement resource filename '{replacement_filename}' with size {replacement_size}"
    )

    resource_to_update.set_file_to_upload(resource_file_path, guess_format_from_suffix=True)

    if not dry_run:
        resource_to_update.update_in_hdx()
        statuses.append("Update to HDX successful")
        return statuses

    statuses.append("Dry run True so no update to HDX made, otherwise successful")
    return statuses


def configure_hdx_connection(hdx_site: str):
    try:
        Configuration.create(
            user_agent_config_yaml=os.path.join(os.path.expanduser("~"), ".useragents.yaml"),
            user_agent_lookup="hdx-cli-toolkit",
            hdx_site=hdx_site,
            hdx_read_only=False,
        )
    except ConfigurationError:
        pass

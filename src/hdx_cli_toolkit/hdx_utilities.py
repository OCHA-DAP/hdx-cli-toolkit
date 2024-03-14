#!/usr/bin/env python
# encoding: utf-8

import os
from hdx.api.configuration import Configuration, ConfigurationError
from hdx.data.dataset import Dataset
from hdx.data.showcase import Showcase
from hdx.data.resource import Resource

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
    dataset_name: str,
    resource_name: str,
    hdx_site: str,
    resource_file_path: str,
    live: bool,
    description: str = "new resource",
):
    configure_hdx_connection(hdx_site)
    statuses = []
    dataset = Dataset.read_from_hdx(dataset_name)
    # Check we found a dataset
    if dataset is None:
        statuses.append(f"No dataset with the name '{dataset_name}' found on HDX site '{hdx_site}'")
        return statuses

    statuses.append(f"Found dataset with the name '{dataset_name}' on HDX site '{hdx_site}'")
    # Check we found a resource
    resources = dataset.get_resources()
    resource_to_update = None
    for resource in resources:
        if resource["name"] == resource_name:
            resource_to_update = resource

    # Report on the characteristics of the selected file for upload cf the original if available
    if not os.path.exists(resource_file_path):
        statuses.append(f"No file found at file path '{resource_file_path}'")
        return statuses

    statuses.append(f"Found file to upload at '{resource_file_path}'")

    if resource_to_update is not None:
        url = resource_to_update["url"]
        original_filename = url[(url.rfind("/") + 1) :]  # noqa: E203
        original_size = resource_to_update["size"]
        statuses.append(
            f"Original resource filename '{original_filename}' with size {original_size}"
        )

    replacement_filename = os.path.basename(resource_file_path)
    file_stats = os.stat(resource_file_path)
    replacement_size = file_stats.st_size
    statuses.append(
        f"Replacement resource filename '{replacement_filename}' with size {replacement_size}"
    )

    # This is the "add resource branch"
    if resource_to_update is None:
        statuses.append(
            f"No resource with the name '{resource_name}' found on dataset '{dataset_name}', "
            "adding file as new resource."
        )
        # Make a new resource
        new_resource = Resource(
            {
                "name": resource_name,
                "description": description,
            }
        )

        new_resource.set_file_to_upload(resource_file_path, guess_format_from_suffix=True)
        resource_list = [new_resource]
        resource_list.extend(resources)
        dataset.add_update_resources(resource_list, ignore_datasetid=True)
        if live:
            # It seems this match_resource_order keyword is not respected
            dataset.update_in_hdx(match_resource_order=True)
            # So we reload the dataset from HDX and force a reorder
            revised_dataset = Dataset.read_from_hdx(dataset_name)
            resources_check = revised_dataset.get_resources()

            reordered_resource_ids = [
                x["id"] for x in resources_check if x["name"] == resource_name
            ]
            reordered_resource_ids.extend(
                [x["id"] for x in resources_check if x["name"] != resource_name]
            )

            revised_dataset.reorder_resources(resource_ids=reordered_resource_ids)
            statuses.append("Addition to HDX successful")
            return statuses
    else:
        resource_to_update.set_file_to_upload(resource_file_path, guess_format_from_suffix=True)
        if live:
            resource_to_update.update_in_hdx()
            statuses.append("Update to HDX successful")
            return statuses

    statuses.append("No '--live' flag supplied so no update to HDX made, otherwise successful")
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

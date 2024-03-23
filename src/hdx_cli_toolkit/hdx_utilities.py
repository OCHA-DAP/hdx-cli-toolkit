#!/usr/bin/env python
# encoding: utf-8


import fnmatch
import json
import os
from hdx.api.configuration import Configuration, ConfigurationError
from hdx.data.organization import Organization
from hdx.data.resource_view import ResourceView
from hdx.data.dataset import Dataset
from hdx.data.showcase import Showcase
from hdx.data.resource import Resource

from hdx_cli_toolkit.utilities import read_attributes


def get_filtered_datasets(
    organization: str = "",
    dataset_filter: str = "*",
    query: str = None,
    hdx_site: str = "stage",
    verbose: bool = True,
) -> list[Dataset]:
    """A function to return a list of datasets selected by some selection criteria based on
    organization, dataset name, or CKAN query strings. The verbose flag is provided so
    summary output can be suppressed for output to file in the print command.

    Keyword Arguments:
        organization {str} -- an organization name (default: {""})
        key {str} -- _description_ (default: {"private"})
        value {str} -- _description_ (default: {"value"})
        dataset_filter {str} -- a filter for dataset name, can contain wildcards (default: {"*"})
        query {str} -- a query string to use in the CKAN search API (default: {None})
        hdx_site {str} -- target HDX site {prod|stage} (default: {"stage"})
        verbose {bool} -- if True prints summary information (default: {True})

    Returns:
        list[Dataset] -- a list of datasets satisfying the selection criteria
    """
    configure_hdx_connection(hdx_site=hdx_site)

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


def decorate_dataset_with_extras(dataset: Dataset) -> dict:
    """A function to add resource, quickcharts (resource_view) and showcases keys to a dataset
    dictionary representation for the print command. fs_check_info and hxl_preview_config are
    converted from JSON objects serialised as single strings to dictionaries to make printed output
    more readable. This decoration means that the dataset dictionary cannot be uploaded to HDX.

    Arguments:
        dataset {Dataset} -- a Dataset object to process

    Returns:
        dict -- a dictionary containing the dataset metadata
    """
    output_dict = dataset.data
    resources = dataset.get_resources()
    output_dict["resources"] = []
    for resource in resources:
        resource_dict = resource.data
        if "fs_check_info" in resource_dict:
            resource_dict["fs_check_info"] = json.loads(resource_dict["fs_check_info"])
        dataset_quickcharts = ResourceView.get_all_for_resource(resource_dict["id"])
        resource_dict["quickcharts"] = []
        if dataset_quickcharts is not None:
            for quickchart in dataset_quickcharts:
                quickchart_dict = quickchart.data
                if "hxl_preview_config" in quickchart_dict:
                    quickchart_dict["hxl_preview_config"] = json.loads(
                        quickchart_dict["hxl_preview_config"]
                    )
                resource_dict["quickcharts"].append(quickchart_dict)
        output_dict["resources"].append(resource_dict)

    showcases = dataset.get_showcases()
    output_dict["showcases"] = [x.data for x in showcases]

    return output_dict


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

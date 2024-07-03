#!/usr/bin/env python
# encoding: utf-8

import csv
import fnmatch
import functools
import json
import os
import time
import traceback
import urllib3

from pathlib import Path
from typing import Optional

import ckanapi
import click
import urllib3.util
import yaml

from hdx.api.configuration import Configuration, ConfigurationError
from hdx.data.organization import Organization
from hdx.data.resource_view import ResourceView
from hdx.data.hdxobject import HDXError
from hdx.data.dataset import Dataset
from hdx.data.showcase import Showcase
from hdx.data.resource import Resource
from hdx.data.user import User
from hdx.utilities.path import script_dir_plus_file

from hdx_cli_toolkit.utilities import (
    read_attributes,
    make_conversion_func,
    write_dictionary,
    make_path_unique,
    print_dictionary_comparison,
)


def hdx_error_handler(f):
    @functools.wraps(f)
    def inner(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except (HDXError, ckanapi.errors.ValidationError):
            traceback_message = traceback.format_exc()
            message = parse_hdxerror_traceback(traceback_message)
            if message != "unknown":
                click.secho(
                    f"Could not perform operation on HDX because of an `{message}`",
                    fg="red",
                    color=True,
                )
            else:
                print(traceback_message, flush=True)

    return inner


def parse_hdxerror_traceback(traceback_message: str):
    message = "unknown"
    if "Authorization Error" in traceback_message:
        message = "Authorization Error"
    elif "extras" in traceback_message:
        message = "Extras Key Error"
    elif "KeyError: 'resources'" in traceback_message:
        message = "No Resources Error"
    elif "{'dataset_date': ['Invalid old HDX date" in traceback_message:
        message = "Invalid Dataset Date Error"

    return message


@hdx_error_handler
def get_filtered_datasets(
    organization: str = "",
    dataset_filter: str = "*",
    query: Optional[str] = None,
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
    configure_hdx_connection(hdx_site=hdx_site, verbose=False)

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


@hdx_error_handler
def update_values_in_hdx_from_file(
    hdx_site: str, from_file: str, undo: bool = False, output_path: Optional[str] = None
):
    configure_hdx_connection(hdx_site=hdx_site)
    # Open the file
    if not os.path.exists(from_file):
        print(f"The file provided for input ({from_file}) does not exist. Exiting.", flush=True)
        return
    with open(from_file, "r", encoding="utf-8") as update_file_handle:
        rows = list(csv.DictReader(update_file_handle))
        # Check the file has rows, and the required columns.
        if len(rows) == 0:
            print(
                f"The file provided for input ({from_file}) contains no rows. Exiting.", flush=True
            )
            return

        # For each row
        n_changed = 0
        n_failures = 0
        output_rows = []
        print(f"From_file {from_file} contains {len(rows)} entries", flush=True)
        print(
            f"{'dataset_name':<70.70}{'old value':<20.20}{'new value':<20.20}"
            f"{'Time to update/seconds':<25.25}",
            flush=True,
        )
        new_value_key = "new_value"
        if undo:
            new_value_key = "old_value"
        for row in rows:
            dataset = Dataset.read_from_hdx(row["dataset_name"])
            # Read in the dataset
            if not dataset:
                print(f"{row['dataset_name']} does not exist on {hdx_site}. Exiting", flush=True)
                return

            conversion_func, type_name = make_conversion_func(row[new_value_key])

            if conversion_func is None:
                print(f"Type name '{type_name}' is not recognised, aborting", flush=True)
                return

            dn_changed, dn_failures, doutput_rows = update_values_in_hdx(
                [dataset], row["key"], row[new_value_key], conversion_func, hdx_site=hdx_site
            )
            n_changed += dn_changed
            n_failures += dn_failures
            output_rows.extend(doutput_rows)

    # Make an output_path from the from_path?
    if output_path is not None:
        output_path = make_path_unique(output_path)
    else:
        filename, extension = os.path.splitext(from_file)
        output_path = f"{filename}-redo{extension}"
    status = write_dictionary(output_path, output_rows, append=False)
    print(status, flush=True)

    print(f"Changed {n_changed} values", flush=True)
    print(f"{n_failures} failures as evidenced by HDXError", flush=True)


# This function does its own error handling for hdx, so no @hdx_error_handler
def update_values_in_hdx(
    filtered_datasets: list[Dataset], key, value, conversion_func, hdx_site: str = "stage"
):
    n_changed = 0
    n_failures = 0
    row_template = {
        "dataset_name": None,
        "key": key,
        "old_value": None,
        "new_value": None,
        "message": None,
    }
    output_rows = []
    for dataset in filtered_datasets:
        t0 = time.time()
        try:
            old_value = str(dataset[key])
        except KeyError:
            old_value = ""
        new_value = conversion_func(value)
        dataset[key] = new_value

        row = row_template.copy()
        row["dataset_name"] = dataset["name"]
        row["old_value"] = old_value
        row["new_value"] = new_value

        if old_value == str(dataset[key]):
            message = "No update required"
            print(
                f"{dataset['name']:<70.70}{old_value:<20.20}{str(dataset[key]):<20.20} "
                f"{message:<25.25}",
                flush=True,
            )
            row["message"] = message
            output_rows.append(row)
            continue
        try:
            if "extras" in dataset.data:
                dataset.data.pop("extras")
            dataset.update_in_hdx(
                update_resources=False,
                hxl_update=False,
                operation="patch",
                batch_mode="KEEP_OLD",
                skip_validation=True,
                ignore_check=True,
            )
            message = f"{time.time() - t0:.2f}"
            print(
                f"{dataset['name']:<70.70}{old_value:<20.20}{str(dataset[key]):<20.20} "
                f"{message}",
                flush=True,
            )
            row["message"] = message
            output_rows.append(row)
            n_changed += 1
        except (HDXError, KeyError, ckanapi.errors.ValidationError):
            traceback_message = parse_hdxerror_traceback(traceback.format_exc())
            message = f"Could not update {dataset['name']} on '{hdx_site}' - {traceback_message}"
            n_failures += 1

            print(
                f"{dataset['name']:<70.70}{old_value:<20.20}{old_value:<20.20} {message}",
                flush=True,
            )
            row["message"] = message
            output_rows.append(row)

    return n_changed, n_failures, output_rows


@hdx_error_handler
def get_organizations_from_hdx(organization: str, hdx_site: str = "stage"):
    configure_hdx_connection(hdx_site)
    filtered_organizations = []
    all_organizations = Organization.get_all_organization_names(include_extras=True)
    for an_organization in all_organizations:
        if fnmatch.fnmatch(an_organization, f"*{organization}*"):
            organization_metadata = Organization.read_from_hdx(an_organization)
            filtered_organizations.append(organization_metadata)

    return filtered_organizations


@hdx_error_handler
def get_users_from_hdx(user: str, hdx_site: str = "stage"):
    configure_hdx_connection(hdx_site=hdx_site)

    user_list = User.get_all_users(q=user)

    return user_list


@hdx_error_handler
def decorate_dataset_with_extras(
    dataset: Dataset, hdx_site: str = "stage", verbose: bool = False
) -> dict:
    """A function to add resource, quickcharts (resource_view) and showcases keys to a dataset
    dictionary representation for the print command. fs_check_info and hxl_preview_config are
    converted from JSON objects serialised as single strings to dictionaries to make printed output
    more readable. This decoration means that the dataset dictionary cannot be uploaded to HDX.

    Arguments:
        dataset {Dataset} -- a Dataset object to process

    Returns:
        dict -- a dictionary containing the dataset metadata
    """
    configure_hdx_connection(hdx_site, verbose=verbose)
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


@hdx_error_handler
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


@hdx_error_handler
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


@hdx_error_handler
def add_quickcharts(dataset_name, hdx_site, resource_name, hdx_hxl_preview_file_path):
    configure_hdx_connection(hdx_site=hdx_site)
    status = "Successful"

    # read the json file
    with open(hdx_hxl_preview_file_path, "r", encoding="utf-8") as json_file:
        recipe = json.load(json_file)
    # extract appropriate keys
    processed_recipe = {
        "description": "",
        "title": "Quick Charts",
        "view_type": "hdx_hxl_preview",
        "hxl_preview_config": "",
    }

    # convert the configuration to a string
    stringified_config = json.dumps(
        recipe["hxl_preview_config"], indent=None, separators=(",", ":")
    )
    processed_recipe["hxl_preview_config"] = stringified_config
    # write out yaml to a temp file
    temp_yaml_path = f"{hdx_hxl_preview_file_path}.temp.yaml"
    with open(temp_yaml_path, "w", encoding="utf-8") as yaml_file:
        yaml.dump(processed_recipe, yaml_file)

    dataset = Dataset.read_from_hdx(dataset_name)
    dataset.generate_quickcharts(resource=resource_name, path=temp_yaml_path)
    dataset.update_in_hdx(update_resources=False, hxl_update=False)

    # delete the temp file
    if os.path.exists(temp_yaml_path):
        os.remove(temp_yaml_path)

    return status


@hdx_error_handler
def download_hdx_datasets(
    dataset_filter: str,
    resource_filter: str = "*",
    hdx_site: str = "stage",
    download_directory: Optional[str] = None,
):
    configure_hdx_connection(hdx_site=hdx_site)
    if download_directory is None:
        download_directory = os.path.join(os.path.dirname(__file__), "output")

    Path(download_directory).mkdir(parents=True, exist_ok=True)

    if resource_filter is None:
        resource_filter = "*"

    dataset = Dataset.read_from_hdx(dataset_filter)
    resources = dataset.get_resources()
    download_paths = []
    for resource in resources:
        if fnmatch.fnmatch(resource["name"], resource_filter):
            expected_file_path = os.path.join(download_directory, f"{resource['name']}")
            if os.path.exists(expected_file_path):
                print(f"Expected file {expected_file_path} is already present, continuing")
                download_paths.append(expected_file_path)
            else:
                print(f"Downloading {resource['name']}...")
                resource_url, resource_file = resource.download(folder=download_directory)
                print(f"...from {resource_url}")
                download_paths.append(resource_file)

    return download_paths


@hdx_error_handler
def configure_hdx_connection(hdx_site: str, verbose: bool = True):
    Configuration._configuration = None
    try:
        Configuration.create(
            user_agent_config_yaml=os.path.join(os.path.expanduser("~"), ".useragents.yaml"),
            user_agent_lookup="hdx-cli-toolkit",
            hdx_site=hdx_site,
            hdx_read_only=False,
        )
        if verbose:
            print(f"Connected to HDX site {Configuration.read().get_hdx_site_url()}", flush=True)
    except ConfigurationError:
        print(traceback.format_exc(), flush=True)


def get_approved_tag_list() -> list[str]:
    approved_tag_list = []
    default_hdx_config_yaml = script_dir_plus_file(
        "hdx_base_configuration.yaml", ConfigurationError
    )

    with open(default_hdx_config_yaml, "r", encoding="utf-8") as file_handle:
        hdx_config_dict = yaml.safe_load(file_handle)

    tags_list_url = hdx_config_dict["tags_list_url"]

    response = urllib3.request(
        "GET", tags_list_url, timeout=60, retries=urllib3.util.Retry(90, backoff_factor=1.0)
    )
    if response.status == 200:
        response_str = response.data.decode("utf-8")
        approved_tag_list = response_str.split("\n")
    else:
        print("Could not retrieve approved tag list", flush=True)
        print(f"The tag list url {tags_list_url} returned status {response.status}", flush=True)

    return approved_tag_list


@hdx_error_handler
def remove_extras_key_from_dataset(
    original_dataset: Dataset, hdx_site: str = "stage", verbose: bool = False
) -> dict:
    configure_hdx_connection(hdx_site=hdx_site)
    dataset_name = original_dataset["name"]
    output_row = {"dataset_name": dataset_name, "had_extras": False, "removed_successfully": True}
    original_dataset_preserved = original_dataset.copy()
    if "extras" in original_dataset.data:
        output_row["had_extras"] = True
        original_dataset.data.pop("extras")
        original_dataset.create_in_hdx(hxl_update=False, keys_to_delete=["extras"])
    else:
        # print(f"Extras key not found in {dataset_name} on {hdx_site}", flush=True)
        return output_row

    processed_dataset = Dataset.read_from_hdx(dataset_name)

    if "extras" in processed_dataset.data:
        output_row["removed_successully"] = False
    else:
        output_row["removed_successully"] = True
    if verbose:
        print_dictionary_comparison(
            original_dataset_preserved,
            processed_dataset,
            "original dataset",
            "dataset with extras removed",
            differences=False,
        )

    return output_row


def check_api_key(organization: str = "hdx", hdx_sites: Optional[str] = None) -> list[str]:
    if hdx_sites is None:
        hdx_sites = ["stage", "prod"]
    statuses = []
    for hdx_site in hdx_sites:
        configure_hdx_connection(hdx_site, verbose=True)
        result = User.check_current_user_organization_access(
            organization, permission="create_datasets"
        )
        if result:
            statuses.append(
                f"API key valid on '{hdx_site}' to create datasets for '{organization}'"
            )
        else:
            statuses.append(
                f"API key not valid on '{hdx_site}' to create datasets for '{organization}'"
            )

    return statuses

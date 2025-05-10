#!/usr/bin/env python
# encoding: utf-8

import json
import urllib3

from random import randrange
from hdx_cli_toolkit.ckan_utilities import fetch_data_from_ckan_package_search, get_hdx_url_and_key

CKAN_API_ROOT_URL = "https://data.humdata.org/api/action/"


def compile_data_quality_report(
    dataset_name: str, hdx_site: str = "stage", lucky_dip: bool = False
):
    print("Here in the compile_data_quality_function")

    if lucky_dip:
        metadata_dict = lucky_dip_search(hdx_site=hdx_site)

        dataset_name = metadata_dict["result"]["name"]

        print(f"Lucky dip search retreived dataset with name: {dataset_name}")

    else:
        # we assume dataset_filter contains no wildcards - maybe rename to "dataset_name"
        print(f"Retreiving metadata for {dataset_name}", flush=True)
        metadata_dict = read_metadata_from_hdx(dataset_name)
        # Need a call to package show here.

    print(json.dumps(metadata_dict, indent=4), flush=True)
    # print(json.dumps(response, indent=4), flush=True)

    # Really Data Quality is a resource level attribute, not dataset

    # Revelance
    # *Presence on HDX
    # *Data Grids
    # *Updated by script
    # *Signals
    # *Data series
    # *Crises
    #


def lucky_dip_search(hdx_site: str = "stage"):
    hdx_site_url, hdx_api_key, _ = get_hdx_url_and_key(hdx_site=hdx_site)
    package_search_url = f"{hdx_site_url}/api/action/package_search"
    query = {"fq": "*:*", "start": 0, "rows": 1}
    response = fetch_data_from_ckan_package_search(
        package_search_url, query, hdx_api_key=hdx_api_key, fetch_all=False
    )

    n_datasets = response["result"]["count"]

    # Now do a second query with a random start, using the first to get the range of offsets possible
    # Make a random offset in the range 0, n datasets
    random_start = randrange(0, n_datasets)
    # query with offset (start) = random, limit (rows) = 1
    random_offset_params = {
        "fq": "*:*",
        "start": random_start,
        "rows": 1,
    }
    response = fetch_data_from_ckan_package_search(
        package_search_url, random_offset_params, hdx_api_key=hdx_api_key, fetch_all=False
    )
    metadata_dict = {}
    metadata_dict["result"] = response["result"]["results"][0]
    reformat_metadata_keys(metadata_dict)

    return metadata_dict


# Borrowed from hdx-stable-schema 2025-05-10
def reformat_metadata_keys(metadata_dict):
    for resource in metadata_dict["result"]["resources"]:
        if "fs_check_info" in resource.keys():
            resource["fs_check_info"] = json.loads(resource["fs_check_info"])
        if "shape_info" in resource.keys():
            resource["shape_info"] = json.loads(resource["shape_info"])


# Borrowed from hdx-stable-schema 2025-05-10
def read_metadata_from_hdx(dataset_name: str) -> dict:
    query_url = f"{CKAN_API_ROOT_URL}package_show"
    params = {"id": dataset_name}

    response = urllib3.request("GET", query_url, fields=params)
    metadata_dict = {}
    if response.status == 200:
        metadata_dict = response.json()

    reformat_metadata_keys(metadata_dict)
    return metadata_dict

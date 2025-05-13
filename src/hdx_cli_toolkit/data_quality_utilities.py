#!/usr/bin/env python
# encoding: utf-8

import json
import urllib3
import csv

from pathlib import Path
from random import randrange
from hdx_cli_toolkit.ckan_utilities import fetch_data_from_ckan_package_search, get_hdx_url_and_key
from hdx_cli_toolkit.hapi_utilities import get_hapi_resource_ids

CKAN_API_ROOT_URL = "https://data.humdata.org/api/action/"


def compile_data_quality_report(
    dataset_name: str, hdx_site: str = "stage", lucky_dip: bool = False
):
    if lucky_dip:
        metadata_dict = lucky_dip_search(hdx_site=hdx_site)

        dataset_name = metadata_dict["result"]["name"]

        print(f"Lucky dip search retreived dataset with name: {dataset_name}")

    else:
        # we assume dataset_filter contains no wildcards - maybe rename to "dataset_name"
        metadata_dict = read_metadata_from_hdx(dataset_name)
        # Need a call to package show here.

    report = {}
    report["dataset_name"] = dataset_name
    report = add_relevance_entries(metadata_dict, report)
    print(json.dumps(report, indent=4), flush=True)
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
    return report


def add_relevance_entries(metadata_dict: dict | None, report: dict):
    dataset_name = metadata_dict["result"]["name"]
    report["relevance"] = {}
    if metadata_dict is None:
        report["relevance"]["in_hdx"] = False
        return report
    report["relevance"]["in_hdx"] = True
    report["relevance"]["in_dataseries"] = (
        metadata_dict["result"]["dataseries_name"]
        if "dataseries_name" in metadata_dict["result"].keys()
        else False
    )
    report["relevance"]["in_pipeline"] = (
        metadata_dict["result"]["updated_by_script"]
        if "updated_by_script" in metadata_dict["result"].keys()
        else False
    )
    report["relevance"]["in_cod"] = (
        metadata_dict["result"]["cod_level"]
        if "cod_level" in metadata_dict["result"].keys()
        else False
    )
    report["relevance"]["in_hapi_output"] = True if dataset_name.startswith("hdx-hapi-") else False
    report["relevance"]["in_signals"] = check_for_signals(metadata_dict)
    report["relevance"]["in_crisis"] = check_for_crisis(metadata_dict)

    report["relevance"]["in_hapi_input"] = check_for_hapi(metadata_dict)
    report["relevance"]["in_data_grids"] = check_for_datagrid(metadata_dict)

    return report


def check_for_hapi(metadata_dict: dict) -> str | bool:
    in_hapi_input = False
    hapi_resource_ids = get_hapi_resource_ids("hapi")

    n_resources = len(metadata_dict["result"]["resources"])
    n_in_hapi = 0
    for resource in metadata_dict["result"]["resources"]:
        if resource["id"] in hapi_resource_ids:
            n_in_hapi += 1

    if n_in_hapi != 0:
        in_hapi_input = f"{n_in_hapi} of {n_resources}"

    return in_hapi_input


def check_for_datagrid(metadata_dict: dict) -> str | bool:
    in_datagrid = False
    datagrid_filepath = Path(__file__).parent / "data" / "2025-05-13-datagrid-datasets.csv"
    with open(datagrid_filepath, newline="", encoding="utf-8") as csvfile:
        dataset_name_rows = csv.DictReader(csvfile)
        datagrid_datasets = [x["dataset_name"] for x in dataset_name_rows]

    if metadata_dict["result"]["name"] in datagrid_datasets:
        in_datagrid = True

    return in_datagrid


def check_for_signals(metadata_dict: dict) -> str | bool:
    in_signals = False

    # This list is derived from (on 2025-05-11) - needs checking regularly:
    # https://github.com/OCHA-DAP/hdx-ckan/blob/
    # e42f76e9ea204bbe4ec43c10088a756bb5ae8001/
    # ckanext-hdx_theme/ckanext/hdx_theme/helpers/ui_constants/landing_pages/signals.py#L40
    signals_datasets = [
        "asap-hotspots-monthly",
        "global-acute-food-insecurity-country-data",
        "inform-global-crisis-severity-index",
        "global-market-monitor",
    ]
    signals_organizations = ["acled", "international-displacement-monitoring-centre-idmc"]

    dataset_name = metadata_dict["result"]["name"]
    organization_name = metadata_dict["result"]["organization"]["name"]

    if dataset_name in signals_datasets:
        in_signals = True

    if organization_name in signals_organizations:
        in_signals = True

    return in_signals


def check_for_crisis(metadata_dict) -> list[str] | bool:
    in_crisis = False

    for tag in metadata_dict["result"]["tags"]:
        if tag["name"].startswith("crisis-"):
            if not in_crisis:
                in_crisis = [tag["name"]]
            else:
                in_crisis.append(tag["name"])

    return in_crisis


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
def reformat_metadata_keys(metadata_dict: dict | None):
    if metadata_dict is not None:
        for resource in metadata_dict["result"]["resources"]:
            if "fs_check_info" in resource.keys():
                resource["fs_check_info"] = json.loads(resource["fs_check_info"])
            if "shape_info" in resource.keys():
                resource["shape_info"] = json.loads(resource["shape_info"])


# Borrowed from hdx-stable-schema 2025-05-10
def read_metadata_from_hdx(dataset_name: str) -> dict | None:
    query_url = f"{CKAN_API_ROOT_URL}package_show"
    params = {"id": dataset_name}

    response = urllib3.request("GET", query_url, fields=params)
    metadata_dict = None
    if response.status == 200:
        metadata_dict = response.json()

    reformat_metadata_keys(metadata_dict)
    return metadata_dict

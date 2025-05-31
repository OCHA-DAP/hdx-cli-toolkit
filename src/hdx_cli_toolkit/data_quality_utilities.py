#!/usr/bin/env python
# encoding: utf-8

import datetime
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
        if metadata_dict:
            dataset_name = metadata_dict["result"]["name"]
            print(f"Lucky dip search retreived dataset with name: {dataset_name}")
    else:
        # we assume dataset_filter contains no wildcards - maybe rename to "dataset_name"
        metadata_dict = read_metadata_from_hdx(dataset_name)
        # Need a call to package show here.

    report = {}
    report["dataset_name"] = dataset_name
    report = add_relevance_entries(metadata_dict, report)
    report = add_timeliness_entries(metadata_dict, report)
    report = add_accessibility_entries(metadata_dict, report)
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


def add_relevance_entries(metadata_dict: dict | None, report: dict) -> dict:
    if metadata_dict:
        dataset_name = metadata_dict["result"]["name"]
    else:
        return report
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
    relevance_summary = [1 if v else 0 for k, v in report["relevance"].items()]
    report["relevance"]["score"] = sum(relevance_summary)

    return report


def add_timeliness_entries(metadata_dict: dict | None, report: dict) -> dict:
    report["timeliness"] = {}
    if metadata_dict is None:
        return report

    due_date = metadata_dict["result"].get("due_date", False)

    today = datetime.datetime.now().isoformat()[0:10]
    report["timeliness"]["is_fresh"] = metadata_dict["result"].get("is_fresh", False)
    report["timeliness"]["is_crisis_relevant"] = (
        True
        if check_for_crisis(metadata_dict)
        and metadata_dict["result"]["data_update_frequency"] == "0"
        else False
    )
    report["timeliness"]["has_correct_cadence"] = None

    report["timeliness"]["data_update_frequency"] = metadata_dict["result"]["data_update_frequency"]
    report["timeliness"]["due_date"] = due_date
    report["timeliness"]["dataset_date"] = metadata_dict["result"]["dataset_date"]
    report["timeliness"]["days_since_last_modified"] = (
        datetime.datetime.fromisoformat(today)
        - datetime.datetime.fromisoformat(metadata_dict["result"]["last_modified"][0:10])
    ).days

    # Frequency of update is respected
    # Publication time is relevant to crisis
    resource_changes = summarise_resource_changes(metadata_dict)
    report["timeliness"]["resources"] = []
    for resource in metadata_dict["result"]["resources"]:
        resource_report = {}
        resource_report["name"] = resource["name"]
        if due_date is not None:
            resource_report["is_fresh"] = (
                True if datetime.datetime.now().isoformat() < due_date else False
            )
        resource_report["days_since_last_modified"] = (
            datetime.datetime.fromisoformat(today)
            - datetime.datetime.fromisoformat(metadata_dict["result"]["last_modified"][0:10])
        ).days

        # Process fs_check_info
        checks = resource_changes[resource["name"]]["checks"]
        # Number of updates
        resource_report["n_updates"] = len(checks)
        # Days since last update
        if len(checks) != 0:
            resource_report["days_since_last_update"] = (
                datetime.datetime.fromisoformat(today)
                - datetime.datetime.fromisoformat(checks[-1][0:10])
            ).days
            # Days since last nrows change
            last_change_date = None
            for check in reversed(checks):
                if "nrows" in check:
                    last_change_date = check[0:10]
            if last_change_date is not None:
                resource_report["days_since_last_data_change"] = (
                    datetime.datetime.fromisoformat(today)
                    - datetime.datetime.fromisoformat(last_change_date)
                ).days
            else:
                resource_report["days_since_last_data_change"] = None
            # days between updates
            days_between_updates = []
            previous = datetime.datetime.fromisoformat(checks[0][0:10])
            for check in checks[1:]:
                current = datetime.datetime.fromisoformat(check[0:10])
                days = (current - previous).days
                days_between_updates.append(days)
                previous = current

            resource_report["cadence"] = days_between_updates
        report["timeliness"]["resources"].append(resource_report)
    return report


def add_accessibility_entries(metadata_dict: dict | None, report: dict) -> dict:
    report["accessibility"] = {}
    if metadata_dict is None:
        return report

    resource_changes = summarise_resource_changes(metadata_dict)
    report["accessibility"]["resources"] = []
    for resource in metadata_dict["result"]["resources"]:
        resource_report = {}
        resource_report["name"] = resource["name"]
        format_ = resource["format"].upper()
        if format_ in ["CSV", "JSON", "GEOJSON", "XML", "KML", "GEOTIFF", "GEOPACKAGE"]:
            format_score = 2
        elif format_ in ["XLSX", "XLS", "SHP"]:
            format_score = 1
        elif format_ in ["PDF", "DOC", "DOCX"]:
            format_score = 0
        else:
            print(f"Unknown resource format: {resource['format']}", flush=True)
            sys.exit()

        resource_report["format_score"] = f"{format_score} ({format_})"

        # # Number of updates
        # resource_report["n_updates"] = len(checks)
        # Days since last update
        resource_report["is_hxlated"] = False
        if "fs_check_info" in resource.keys():
            check, error_message = get_last_complete_check(resource, "fs_check_info")
            # print(json.dumps(check, indent=4), flush=True)
            if error_message == "Success":
                # print(json.dumps(check, indent=4), flush=True)
                for sheet in check["hxl_proxy_response"]["sheets"]:
                    if sheet["is_hxlated"]:
                        resource_report["is_hxlated"] = True
        # elif "shape_info" in resource.keys():
        #     check, error_message = get_last_complete_check(resource, "shape_info")

        report["accessibility"]["resources"].append(resource_report)
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
        print(f"Length of datagrid list {len(datagrid_datasets)}", flush=True)
        print(f"Length of datagrid set {len(set(datagrid_datasets))}", flush=True)

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

    # Now do a second query with a random start,
    # using the first to get the range of offsets possible
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

    dataset_name = response["result"]["results"][0]["name"]

    metadata_dict = read_metadata_from_hdx(dataset_name=dataset_name)

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


# Borrowed from hdx-stable-schema 2025-05-21
def summarise_resource_changes(metadata: dict) -> dict:
    resource_changes = {}
    for resource in metadata["result"]["resources"]:
        resource_changes[resource["name"]] = {}
        resource_changes[resource["name"]]["checks"] = []

        if "fs_check_info" in resource.keys():
            for check in resource["fs_check_info"]:
                if check["message"] == "File structure check completed":
                    if len(check["sheet_changes"]) != 0:
                        for change in check["sheet_changes"]:
                            change_indicator = f"{check['timestamp'][0:10]}"
                            if change["event_type"] == "spreadsheet-sheet-changed":
                                change_indicator += (
                                    f"* Schema changes in sheet "
                                    f"'{change['name']}' field: "
                                    f"{change['changed_fields'][0]['field']}"
                                )
                            else:
                                change_indicator += (
                                    f"* Schema changes in sheet "
                                    f"'{change['name']}' - "
                                    f"{change['event_type']}"
                                )

                            resource_changes[resource["name"]]["checks"].extend([change_indicator])
                    else:
                        change_indicator = f"{check['timestamp'][0:10]}"
                        resource_changes[resource["name"]]["checks"].extend([change_indicator])
        elif "shape_info" in resource.keys():
            previous_bounding_box = ""
            previous_headers = set()
            for check in resource["shape_info"]:
                change_indicator = ""
                first_check = True
                if isinstance(check, str):
                    continue
                if check["message"] == "Import successful":
                    # (json.dumps(check, indent=4), flush=True)
                    if "layer_fields" in check.keys():
                        headers = {x["field_name"] for x in check["layer_fields"]}
                    else:
                        headers = set()
                    bounding_box = check["bounding_box"]
                    if "timestamp" in check.keys():
                        change_indicator += f"{check['timestamp'][0:10]}"
                    else:
                        change_indicator += "1900-01-01"
                    if not first_check:
                        if (bounding_box != previous_bounding_box) or (previous_headers != headers):
                            change_indicator += "* "
                        if bounding_box != previous_bounding_box:
                            change_indicator += "bounding box change "
                        if previous_headers != headers:
                            change_indicator += "header change"

                    previous_headers = headers
                    previous_bounding_box = bounding_box
                    first_check = False
                    resource_changes[resource["name"]]["checks"].extend([change_indicator])
                # else:
                #     print(json.dumps(check, indent=4), flush=True)

    return resource_changes


# Borrowed from hdx-stable-schema 2025-05-21
def print_resource_summary(resource_summary, resource_changes, target_resource_name=None):
    if target_resource_name is None:
        resource_names = list(resource_changes.keys())
    else:
        resource_names = [target_resource_name]
    for i, resource_name in enumerate(resource_names, start=1):
        checks = resource_changes[resource_name]["checks"]
        print(f"\n{i:>2d}. {resource_name}", flush=True)
        print(
            f"\tFilename: {resource_summary[resource_name]['filename']} "
            f"\n\tFormat: {resource_summary[resource_name]['format']}"
            f"\n\tSheets: {', '.join(resource_summary[resource_name]['sheets'])}",
            flush=True,
        )
        if "bounding_box" in resource_summary[resource_name].keys():
            print(f"\tBounding box: {resource_summary[resource_name]['bounding_box']}", flush=True)
        if resource_summary[resource_name]["in_quarantine"]:
            print("\t**in quarantine**", flush=True)
        print(f"\tChecks ({len(checks)} file structure checks):", flush=True)
        for check in checks:
            print(f"\t\t{check}", flush=True)


# Borrowed from hdx-stable-schema 2025-05-21
def summarise_resource(metadata: dict) -> dict:
    resource_summary = {}
    error_message = "Neither fs_check_info nor shape_info found"
    for resource in metadata["result"]["resources"]:
        resource_summary[resource["name"]] = {}
        resource_summary[resource["name"]]["format"] = resource["format"]
        if "download_url" in resource.keys():
            resource_summary[resource["name"]]["filename"] = resource["download_url"].split("/")[-1]
        else:
            resource_summary[resource["name"]]["filename"] = ""
        resource_summary[resource["name"]]["in_quarantine"] = resource.get("in_quarantine", False)

        resource_summary[resource["name"]]["sheets"] = []

        if "fs_check_info" in resource.keys():
            check, error_message = get_last_complete_check(resource, "fs_check_info")
            if error_message == "Success":
                # print(json.dumps(check, indent=4), flush=True)
                for sheet in check["hxl_proxy_response"]["sheets"]:
                    sheet_name = sheet["name"]
                    nrows = sheet["nrows"]
                    ncols = sheet["ncols"]
                    resource_summary[resource["name"]]["sheets"].append(
                        f"{sheet_name} (n_columns:{ncols} x n_rows:{nrows})"
                    )
        elif "shape_info" in resource.keys():
            check, error_message = get_last_complete_check(resource, "shape_info")
            if error_message == "Success":
                sheet_name = "__DEFAULT__"
                ncols = len(check["layer_fields"])
                nrows = "N/A"
                resource_summary[resource["name"]]["sheets"].append(
                    f"{sheet_name} (n_columns:{ncols} x n_rows:{nrows})"
                )
                resource_summary[resource["name"]]["bounding_box"] = check["bounding_box"]

    if error_message != "Success":
        print(error_message, flush=True)

    return resource_summary


# Borrowed from hdx-stable-schema 2025-05-21
def get_last_complete_check(resource_metadata: dict, metadata_key: str) -> tuple[dict, str]:
    success = False
    error_message = "Success"
    fingerprint = (
        "Import successful" if metadata_key == "shape_info" else "File structure check completed"
    )
    if metadata_key not in resource_metadata:
        error_message = f"metadata_key '{metadata_key}' not found in resource metadata"
        check = {}
        return check, error_message

    for check in reversed(resource_metadata[metadata_key]):
        try:
            if check["message"] == fingerprint:
                # print(json.dumps(check, indent=4), flush=True)
                success = True
                break
        except TypeError:
            success = False

    if not success:
        error_message = (
            f"\nError, could not find an '{fingerprint}' check "
            f"for {resource_metadata['name']}\n"
            f"final message was '{check}'"
        )
        check = {}
    return check, error_message

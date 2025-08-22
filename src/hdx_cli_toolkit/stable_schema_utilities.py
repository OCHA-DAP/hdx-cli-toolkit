#!/usr/bin/env python
# encoding: utf-8

"""
This is a set of utilities borrowed from the stable-schema repo:
https://github.com/OCHA-DAP/hdx-stable-schema
"""

import json
import urllib3


CKAN_API_ROOT_URL = "https://data.humdata.org/api/action/"


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
                if isinstance(check, dict):
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

                                resource_changes[resource["name"]]["checks"].extend(
                                    [change_indicator]
                                )
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

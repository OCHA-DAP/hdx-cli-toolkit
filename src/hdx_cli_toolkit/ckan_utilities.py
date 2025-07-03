#!/usr/bin/env python
# encoding: utf-8

import json
import urllib3

from collections import Counter

import ckanapi

from hdx_cli_toolkit.hdx_utilities import get_hdx_url_and_key, configure_hdx_connection
from hdx_cli_toolkit.utilities import query_dict

DEFAULT_ROW_LIMIT = 100


def fetch_data_from_ckan_package_search(
    query_url: str, query: dict, hdx_api_key: str, fetch_all: bool = False
) -> dict:
    headers = {
        "Authorization": hdx_api_key,
        "Content-Type": "application/json",
    }

    start = 0
    if "start" not in query.keys():
        query["start"] = start
    if "rows" not in query.keys():
        query["rows"] = DEFAULT_ROW_LIMIT
    payload = json.dumps(query)
    i = 1
    # print(f"{i}. Querying {query_url} with {payload}", flush=True)
    response = urllib3.request("POST", query_url, headers=headers, json=query, timeout=20)
    full_response_json = json.loads(response.data)
    n_expected_result = full_response_json["result"]["count"]

    result_length = len(full_response_json["result"]["results"])

    if fetch_all:
        if result_length != n_expected_result:
            while result_length != 0:
                i += 1
                start += query["rows"]
                query["start"] = start
                payload = json.dumps(query)
                print(f"{i}. Querying {query_url} with {payload}", flush=True)
                new_response = urllib3.request(
                    "POST", query_url, headers=headers, json=query, timeout=20
                )
                new_response_json = json.loads(new_response.data)
                result_length = len(new_response_json["result"]["results"])
                full_response_json["result"]["results"].extend(
                    new_response_json["result"]["results"]
                )
        else:
            print(
                f"CKAN API returned all results ({result_length}) on first page of 100", flush=True
            )
        assert n_expected_result == len(full_response_json["result"]["results"])

    return full_response_json


def scan_survey(response: dict, key: str, verbose: bool = False) -> Counter:
    key_occurence_counter = Counter()
    list_of_keys = key.split(",")

    for dataset in response["result"]["results"]:
        output_row = {"dataset_name": dataset["name"]}
        for key_ in list_of_keys:
            output_row[key_] = f"{key_} key absent"
        output_rows = query_dict(list_of_keys, dataset, output_row)

        for row in output_rows:
            for key_ in list_of_keys:
                if "key absent" not in str(row[key_]):
                    key_occurence_counter[key_] += 1
                    if verbose:
                        if key_ != "resources.name":
                            comment = f"has {key_}"
                            if key_.startswith("resources.") and "resources.name" in row.keys():
                                print(
                                    f"{dataset['name']} Resource:{row['resources.name']} {comment}",
                                    flush=True,
                                )
                            else:
                                print(f"{dataset['name']} {comment}", flush=True)

    return key_occurence_counter


def scan_delete_key(
    response: dict, key: str, hdx_site: str = "stage", verbose: bool = False
) -> Counter:
    # Does not use query_dict because we want this to be as controlled as possible
    configure_hdx_connection(hdx_site, verbose=True)
    hdx_site_url, hdx_api_key, user_agent = get_hdx_url_and_key(hdx_site=hdx_site)
    ckan = ckanapi.RemoteCKAN(
        hdx_site_url,
        apikey=hdx_api_key,
        user_agent=user_agent,
    )

    key_occurence_counter = Counter()
    for i, dataset in enumerate(response["result"]["results"]):
        if key.startswith("resources."):
            resource_key = key.split(".")[1]
            for resource in dataset["resources"]:
                if resource_key in resource.keys():
                    resource.pop(resource_key)
                    assert key not in resource.keys()
                    ckan.action.resource_update(**resource)
                    key_occurence_counter[key] += 1
                    if verbose:
                        comment = f"has {key} - deleted"
                        print(dataset["name"], flush=True)
                        print(f"\t{resource['name']} {comment}", flush=True)
        else:
            if key in dataset.keys():
                dataset.pop(key)
                assert key not in dataset.keys()
                hdx_site_url, hdx_api_key, user_agent = get_hdx_url_and_key(hdx_site=hdx_site)
                ckan.action.package_update(**dataset)
                key_occurence_counter[key] += 1
                if verbose:
                    comment = f"has {key} - deleted"
                    print(f"{dataset['name']} {comment}", flush=True)

    return key_occurence_counter


def scan_distribution(response: dict, key: str, verbose: bool = False) -> Counter:
    value_occurence_counter = Counter()

    for i, dataset in enumerate(response["result"]["results"]):
        output_row = {key: ""}
        output_rows = query_dict([key], dataset, output_row)
        for row in output_rows:
            if "key absent" not in str(row[key]):
                value_occurence_counter[row[key]] += 1

    return value_occurence_counter

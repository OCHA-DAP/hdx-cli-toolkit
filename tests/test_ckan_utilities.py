#!/usr/bin/env python
# encoding: utf-8

from copy import deepcopy
import json
import os

from hdx_cli_toolkit.ckan_utilities import (
    scan_delete_key,
    scan_distribution,
    scan_survey,
    fetch_data_from_ckan_package_search,
)


def test_scan_survey(json_fixture):
    key = "resources._csrf_token,resources.in_quarantine"
    response = json_fixture("2024-08-24-hdx-snapshot-filtered.json")
    key_occurence_counter = scan_survey(response, key, verbose=False)

    assert key_occurence_counter == {"resources._csrf_token": 3, "resources.in_quarantine": 136}


def test_scan_delete_key(json_fixture):
    pass


def test_scan_distribution(json_fixture):
    key = "data_update_frequency"
    response = json_fixture("2024-08-24-hdx-snapshot-filtered.json")
    key_occurence_counter = scan_distribution(response, key, verbose=False)

    print(key_occurence_counter, flush=True)
    assert key_occurence_counter == {"-1": 15, "-2": 9, "365": 5, "180": 4, "0": 2, "14": 1}


def test_fetch_data_from_ckan_package_search():
    pass


def test_sneaky_filter_pretending_to_be_a_test():
    original_json_path = os.path.join(
        os.path.dirname(__file__), "fixtures", "2024-08-24-hdx-snapshot.json"
    )
    filtered_json_path = os.path.join(
        os.path.dirname(__file__), "fixtures", "2024-08-24-hdx-snapshot-filtered.json"
    )
    with open(original_json_path, encoding="utf-8") as json_file_handle:
        response = json.load(json_file_handle)

    filtered_response = deepcopy(response)

    filtered_response["result"]["results"] = []

    filter_count = 0
    for i, dataset in enumerate(response["result"]["results"]):
        # if i > 10:
        #     break
        for resource in dataset["resources"]:
            # print(resource.keys(), flush=True)
            if "_csrf_token" in resource.keys() or "in_quarantine" in resource.keys():
                filter_count += 1
                filtered_response["result"]["results"].append(dataset)
                break

    print(f"Selected {filter_count} datasets", flush=True)
    with open(filtered_json_path, "w", encoding="utf-8") as json_file_handle:
        json.dump(filtered_response, json_file_handle)
    assert False

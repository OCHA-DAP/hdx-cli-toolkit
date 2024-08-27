#!/usr/bin/env python
# encoding: utf-8

import json
from unittest import mock

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


@mock.patch("ckanapi.RemoteCKAN.call_action")
def test_scan_delete_key(mock_ckanapi, json_fixture):
    key = "resources._csrf_token"
    response = json_fixture("2024-08-24-hdx-snapshot-filtered.json")
    key_occurence_counter = scan_delete_key(response, key, verbose=False)

    assert key_occurence_counter == {"resources._csrf_token": 3}


def test_scan_distribution(json_fixture):
    key = "data_update_frequency"
    response = json_fixture("2024-08-24-hdx-snapshot-filtered.json")
    key_occurence_counter = scan_distribution(response, key, verbose=False)

    print(key_occurence_counter, flush=True)
    assert key_occurence_counter == {"-1": 15, "-2": 9, "365": 5, "180": 4, "0": 2, "14": 1}


@mock.patch("urllib3.request")
def test_fetch_data_from_ckan_package_search(mock_request, json_fixture):
    mock_response = json_fixture("2024-08-24-hdx-snapshot-filtered.json")
    mock_request.return_value.data = json.dumps(mock_response)
    package_search_url = "https://fake_hdx_site.org/api/action/package_search"
    query = {"fq": "*:*", "start": 0, "rows": 100}
    _ = fetch_data_from_ckan_package_search(
        package_search_url, query, hdx_api_key="", fetch_all=False
    )

    mock_request.assert_called_with(
        "POST",
        package_search_url,
        headers={
            "Authorization": "",
            "Content-Type": "application/json",
        },
        json=query,
        timeout=20,
    )

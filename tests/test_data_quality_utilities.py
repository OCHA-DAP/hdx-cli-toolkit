#!/usr/bin/env python
# encoding: utf-8

import json
import os

import pytest

from hdx_cli_toolkit.data_quality_utilities import compile_data_quality_report

TEST_DATASET_NAMES = [
    # A couple that I generate which are not on data grid/signals/events:
    "explosive-weapons-use-affecting-aid-access-education-and-healthcare-services",
    "climada-litpop-dataset",
    # Data grid datasets
    "unhcr-population-data-for-afg",
    "democratic-republic-of-the-congo-acute-food-insecurity-country-data",
    # HDX Signals
    "sierra-leone-acled-conflict-data",  # – not on a data grid
    "inform-global-crisis-severity-index",
    # HDX HAPI – the initial release follows the Data Grid datasets
    "hdx-hapi-rainfall",  # this one is derived from HDX HAPI
    "global-iom-dtm-from-api",
    # Should we include COD (Common operational datasets?) – cod_level key
    "cod-ps-global",
    "cod-ab-ecu",
    # Crisis datasets
    "gdacs-rss-information",
    "hdx-hapi-mmr",
]

TEST_DATA_QUALITY_REPORT_FILE_PATH = os.path.join(
    os.path.dirname(__file__), "fixtures", "gibraltar-healthsites-data-quality-report.json"
)


@pytest.mark.skip(reason="This is an integration test that makes live calls to HDX stage")
def test_dataset_data_quality():
    for dataset_name in TEST_DATASET_NAMES:
        print(dataset_name, flush=True)
        report = compile_data_quality_report(dataset_name)
        print(json.dumps(report, indent=4), flush=True)

    assert False


def test_handle_a_nonexistent_dataset():
    report = compile_data_quality_report("thing")
    assert report == {"dataset_name": "thing", "relevance": {"in_hdx": False}}


def test_with_gibraltar_metadata(json_fixture):
    dataset_dict = json_fixture("gibraltar_with_extras.json")[0]
    report_dict = json_fixture("gibraltar-healthsites-data-quality-report.json")
    metadata_dict = {}
    metadata_dict["result"] = dataset_dict
    report = compile_data_quality_report(
        dataset_name="gibraltar-healthsites", metadata_dict=metadata_dict
    )

    assert report == report_dict

    assert report["dataset_name"] == "gibraltar-healthsites"
    assert report["relevance_score"] == 5
    assert report["timeliness_score"] == 0
    assert report["accessibility_score"] == 5
    assert report["interpretability_score"] == 0
    assert report["interoperability_score"] == 0
    assert report["findability_score"] == 0
    assert report["total_score"] == 10

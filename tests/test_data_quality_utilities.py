#!/usr/bin/env python
# encoding: utf-8

import os
import json
from hdx_cli_toolkit.data_quality_utilities import compile_data_quality_report

TEST_DATASETS_NAMES = [
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

SAMPLE_REPORTS_FILEPATH = os.path.join(
    os.path.dirname(__file__), "fixtures", "2025-05-15-sample-data-quality_reports.json"
)
with open(SAMPLE_REPORTS_FILEPATH, encoding="utf-8") as SAMPLE_HANDLE:
    TEST_DATASETS = json.load(SAMPLE_HANDLE)


def test_dataset_data_quality():
    for dataset in TEST_DATASETS:
        print(dataset["dataset_name"], flush=True)
        report = compile_data_quality_report(dataset["dataset_name"])
        print(json.dumps(report, indent=4), flush=True)

        assert dataset["dataset_name"] in TEST_DATASETS_NAMES

    assert False


def test_handle_a_nonexistent_dataset():
    report = compile_data_quality_report("thing")
    assert report == {"dataset_name": "thing", "relevance": {"in_hdx": False}}

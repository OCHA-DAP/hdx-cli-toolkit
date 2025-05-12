#!/usr/bin/env python
# encoding: utf-8

from hdx_cli_toolkit.data_quality_utilities import compile_data_quality_report

TEST_DATASETS = [
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


def test_dataset_data_quality():
    for dataset_name in TEST_DATASETS:
        print(dataset_name, flush=True)
        report = compile_data_quality_report(dataset_name)

    assert False


def test_handle_a_nonexistent_dataset():
    report = compile_data_quality_report("thing")
    assert report == {"dataset_name": "thing", "relevance": {"in_hdx": False}}

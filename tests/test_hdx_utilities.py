#!/usr/bin/env python
# encoding: utf-8

import os
from unittest import mock
from unittest.mock import patch

from hdx_cli_toolkit.hdx_utilities import add_showcase, get_filtered_datasets


@patch("hdx_cli_toolkit.hdx_utilities.Showcase")
@patch("hdx.data.dataset.Dataset.read_from_hdx")
def test_add_showcase(mock_hdx, mock_showcase):
    attributes_file_path = os.path.join(os.path.dirname(__file__), "fixtures", "attributes.csv")
    showcase_name = "climada-litpop-showcase"
    mock_showcase().add_tags.return_value = (mock.MagicMock(), mock.MagicMock())
    mock_hdx.return_value = {"name": "mock name"}

    statuses = add_showcase(showcase_name, "stage", attributes_file_path)

    assert statuses == [
        "0 of 3 showcase tags added",
        "Added dataset 'mock name' to showcase 'climada-litpop-showcase'",
    ]
    mock_showcase().add_tags.assert_called_with(
        ["economics", "gross domestic product-gdp", "population"]
    )
    mock_showcase().create_in_hdx.assert_called_with()
    mock_showcase().add_dataset.assert_called_with({"name": "mock name"})


@patch("hdx.data.dataset.Dataset.search_in_hdx")
def test_get_filtered_datasets_1(mock_hdx):
    _ = get_filtered_datasets(
        organization="",
        dataset_filter="*",
        query="archived:true",
    )

    mock_hdx.assert_called_with(query="archived:true")


@patch("hdx.data.organization.Organization.read_from_hdx")
def test_get_filtered_datasets_2(mock_hdx):
    _ = get_filtered_datasets(
        organization="healthsites",
        dataset_filter="*",
        query=None,
    )

    mock_hdx.assert_called_with("healthsites")


@patch("hdx.data.dataset.Dataset.read_from_hdx")
def test_get_filtered_datasets_3(mock_hdx):
    _ = get_filtered_datasets(
        organization="",
        dataset_filter="a-full-dataset-name",
        query=None,
    )

    mock_hdx.assert_called_with("a-full-dataset-name")

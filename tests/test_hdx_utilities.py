#!/usr/bin/env python
# encoding: utf-8

import os
from unittest import mock
from unittest.mock import patch

from hdx_cli_toolkit.hdx_utilities import add_showcase


# @patch("hdx.data.showcase.Showcase")
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

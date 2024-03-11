#!/usr/bin/env python
# encoding: utf-8

# These tests interact directly with the staging HDX instance

import json
import os

import pytest

from hdx.data.dataset import Dataset
from hdx.data.resource import Resource

from hdx_cli_toolkit.hdx_utilities import update_resource_in_hdx, configure_hdx_connection


DATASET_NAME = "hdx_cli_toolkit_test"
TEST_RESOURCE_NAME = "test_resource_1"


@pytest.fixture(autouse=True)
def setup_and_teardown_dataset_in_hdx():
    # This is pytest setup
    configure_hdx_connection(hdx_site="stage")
    dataset = Dataset.read_from_hdx(DATASET_NAME)
    if dataset:
        dataset.delete_from_hdx()
    title = "HDX CLI toolkit test"
    dataset = Dataset({"name": DATASET_NAME, "title": title})
    dataset.update_from_yaml(
        os.path.join(os.path.dirname(__file__), "fixtures", "hdx_dataset_static.yaml")
    )
    countryiso3s = ["AFG", "PSE", "SYR", "YEM"]
    dataset.add_country_locations(countryiso3s)
    tags = ["conflict-violence", "displacement", "hxl"]
    dataset.add_tags(tags)

    resource = Resource(
        {
            "name": TEST_RESOURCE_NAME,
            "description": "Test Resource 1",
        }
    )
    resource_file_path = os.path.join(os.path.dirname(__file__), "fixtures", "test.csv")
    resource.set_format("csv")
    resource.set_file_to_upload(resource_file_path)

    dataset.add_update_resource(resource)

    dataset.create_in_hdx(hxl_update=False, updated_by_script="hdx_cli_toolkit_ignore")

    assert Dataset.read_from_hdx(DATASET_NAME) is not None
    # This is pytest teardown
    yield
    dataset = Dataset.read_from_hdx(DATASET_NAME)
    if dataset:
        dataset.delete_from_hdx()


def test_update_resource():
    # This is the test of update_resource_in_hdx
    dataset = Dataset.read_from_hdx(DATASET_NAME)
    original_resources = dataset.get_resources()

    new_resource_file_path = os.path.join(os.path.dirname(__file__), "fixtures", "test-2.csv")
    statuses = update_resource_in_hdx(
        DATASET_NAME, TEST_RESOURCE_NAME, "stage", new_resource_file_path, live=True
    )

    for status in statuses:
        print(status, flush=True)

    assert len(statuses) == 5

    revised_dataset = Dataset.read_from_hdx(DATASET_NAME)
    revised_resources = revised_dataset.get_resources()

    assert len(original_resources) == len(revised_resources)
    assert original_resources[0].data["name"] == "test_resource_1"
    assert revised_resources[0].data["name"] == "test_resource_1"

    assert original_resources[0].data["url"].endswith("test.csv")
    assert revised_resources[0].data["url"].endswith("test-2.csv")

    assert revised_resources[0].data["size"] > original_resources[0].data["size"]


def test_add_resource():
    # This is the test of update_resource_in_hdx
    dataset = Dataset.read_from_hdx(DATASET_NAME)
    original_resources = dataset.get_resources()
    new_resource_name = "test_resource_2"

    new_resource_file_path = os.path.join(os.path.dirname(__file__), "fixtures", "test-2.csv")
    statuses = update_resource_in_hdx(
        DATASET_NAME, new_resource_name, "stage", new_resource_file_path, live=True
    )

    for status in statuses:
        print(status, flush=True)

    assert len(statuses) == 5

    revised_dataset = Dataset.read_from_hdx(DATASET_NAME)
    revised_resources = revised_dataset.get_resources()

    assert len(original_resources) == 1
    assert len(revised_resources) == 2

    assert revised_resources[0].data["name"] == "test_resource_2"
    assert revised_resources[1].data["name"] == "test_resource_1"

    assert revised_resources[0].data["url"].endswith("test-2.csv")
    assert revised_resources[1].data["url"].endswith("test.csv")

    assert revised_resources[0].data["size"] > original_resources[0].data["size"]

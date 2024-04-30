#!/usr/bin/env python
# encoding: utf-8

# These tests interact directly with the staging HDX instance

import json
import os

import pytest

from hdx.data.dataset import Dataset
from hdx.data.resource import Resource
from hdx.data.resource_view import ResourceView

from hdx_cli_toolkit.hdx_utilities import (
    update_resource_in_hdx,
    configure_hdx_connection,
    update_values_in_hdx,
    add_showcase,
    add_quickcharts,
    get_approved_tag_list,
    update_values_in_hdx_from_file,
)

from hdx_cli_toolkit.utilities import make_conversion_func


DATASET_NAME = "hdx_cli_toolkit_test"
TEST_RESOURCE_NAME = "test_resource_1"
HDX_SITE = "stage"


@pytest.fixture(scope="module", autouse=True)
def setup_and_teardown_dataset_in_hdx():
    # This is pytest setup
    configure_hdx_connection(hdx_site=HDX_SITE)
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

    showcases = dataset.get_showcases()
    for showcase in showcases:
        showcase.delete_from_hdx()

    if dataset:
        dataset.delete_from_hdx()


def test_update_resource():
    dataset = Dataset.read_from_hdx(DATASET_NAME)
    original_resources = dataset.get_resources()

    new_resource_file_path = os.path.join(os.path.dirname(__file__), "fixtures", "test-2.csv")
    statuses = update_resource_in_hdx(
        DATASET_NAME, TEST_RESOURCE_NAME, HDX_SITE, new_resource_file_path, live=True
    )

    for status in statuses:
        print(status, flush=True)

    assert len(statuses) == 5

    revised_dataset = Dataset.read_from_hdx(DATASET_NAME)
    revised_resources = revised_dataset.get_resources()

    for revised_ in revised_resources:
        print(revised_["name"], revised_["url"], flush=True)

    assert len(original_resources) == len(revised_resources)
    assert original_resources[0].data["name"] == "test_resource_1"
    assert revised_resources[0].data["name"] == "test_resource_1"

    assert original_resources[0].data["url"].endswith("test.csv")
    assert revised_resources[0].data["url"].endswith("test-2.csv")

    assert revised_resources[0].data["size"] > original_resources[0].data["size"]


def test_add_resource():
    dataset = Dataset.read_from_hdx(DATASET_NAME)
    original_resources = dataset.get_resources()
    new_resource_name = "inserted_resource"

    new_resource_file_path = os.path.join(os.path.dirname(__file__), "fixtures", "test-3.csv")
    statuses = update_resource_in_hdx(
        DATASET_NAME, new_resource_name, HDX_SITE, new_resource_file_path, live=True
    )

    for status in statuses:
        print(status, flush=True)

    assert len(statuses) == 5

    revised_dataset = Dataset.read_from_hdx(DATASET_NAME)
    revised_resources = revised_dataset.get_resources()
    for revised_ in revised_resources:
        print(revised_["name"], revised_["url"], flush=True)

    assert len(original_resources) == 1
    assert len(revised_resources) == 2

    assert revised_resources[0].data["name"] == "inserted_resource"
    assert revised_resources[1].data["name"] == "test_resource_1"

    assert revised_resources[0].data["url"].endswith("test-3.csv")


def test_update_key():
    dataset = Dataset.read_from_hdx(DATASET_NAME)
    key = "notes"
    value = "new notes"
    conversion_func, _ = make_conversion_func(value)
    n_changed, n_failures, output_rows = update_values_in_hdx(
        [dataset], key, value, conversion_func, hdx_site=HDX_SITE
    )

    assert n_changed == 1
    assert n_failures == 0
    assert len(output_rows) == 1
    dataset = Dataset.read_from_hdx(DATASET_NAME)

    assert dataset["notes"] == "new notes"


def test_add_showcase():
    attributes_file_path = os.path.join(os.path.dirname(__file__), "fixtures", "attributes.csv")
    showcase_name = "climada-litpop-showcase"

    statuses = add_showcase(showcase_name, HDX_SITE, attributes_file_path)

    assert statuses == [
        "3 of 3 showcase tags added",
        "Added dataset 'hdx_cli_toolkit_test' to showcase 'climada-litpop-showcase'",
    ]

    dataset = Dataset.read_from_hdx(DATASET_NAME)
    showcases = dataset.get_showcases()

    assert len(showcases) == 1
    assert showcases[0]["title"] == "CLIMADA LitPop Methodology Documentation"


def test_add_quickcharts():
    resource_name = "admin1-summaries-flood.csv"
    new_resource_file_path = os.path.join(os.path.dirname(__file__), "fixtures", resource_name)
    _ = update_resource_in_hdx(
        DATASET_NAME, resource_name, HDX_SITE, new_resource_file_path, live=True
    )

    hdx_hxl_preview_file_path = os.path.join(
        os.path.dirname(__file__), "fixtures", "quickchart-flood.json"
    )

    status = add_quickcharts(DATASET_NAME, HDX_SITE, resource_name, hdx_hxl_preview_file_path)

    assert status == "Successful"

    dataset = Dataset.read_from_hdx(DATASET_NAME)
    resources = dataset.get_resources()

    quickchart_dicts = []
    for resource in resources:
        resource_dict = resource.data
        if resource_dict["name"] != resource_name:
            continue
        dataset_quickcharts = ResourceView.get_all_for_resource(resource_dict["id"])
        if dataset_quickcharts is not None:
            for quickchart in dataset_quickcharts:
                quickchart_dict = quickchart.data
                if "hxl_preview_config" in quickchart_dict:
                    quickchart_dict["hxl_preview_config"] = json.loads(
                        quickchart_dict["hxl_preview_config"]
                    )
                quickchart_dicts.append(quickchart_dict)

    assert len(quickchart_dicts) == 2
    assert quickchart_dicts[1]["title"] == "Quick Charts"


def test_get_approved_tag_list():
    approved_tags = get_approved_tag_list()
    assert len(approved_tags) == 140


def test_update_values_in_hdx_from_file():
    update_file_path = os.path.join(os.path.dirname(__file__), "fixtures", "update-from-file.csv")
    update_values_in_hdx_from_file(HDX_SITE, update_file_path)
    dataset = Dataset.read_from_hdx(DATASET_NAME)
    assert dataset["caveats"] == "Second value from_file"

    redo_file_path = os.path.join(
        os.path.dirname(__file__), "fixtures", "update-from-file-redo.csv"
    )
    assert os.path.exists(redo_file_path)
    if os.path.exists(redo_file_path):
        os.remove(redo_file_path)


def test_error_handling(capfd):
    dataset = Dataset.read_from_hdx(DATASET_NAME)
    key = "extras"
    value = "Extras key is illegal"
    conversion_func, _ = make_conversion_func(value)
    n_changed, n_failures, output_rows = update_values_in_hdx(
        [dataset], key, value, conversion_func, hdx_site=HDX_SITE
    )

    assert n_changed == 0
    assert n_failures == 1
    assert len(output_rows) == 1
    output, _ = capfd.readouterr()

    assert "Could not update hdx_cli_toolkit_test on 'stage' - Extras Key Error" in output

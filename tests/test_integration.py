#!/usr/bin/env python
# encoding: utf-8

# These tests interact directly with the staging HDX instance

import json
import os

from hdx.data.dataset import Dataset
from hdx.data.resource import Resource

from hdx_cli_toolkit.hdx_utilities import update_resource_in_hdx, configure_hdx_connection


class TestHDXToolkit:
    def test_update_resource(
        self,
    ):
        configure_hdx_connection("stage")
        dataset_name = "hdx_cli_toolkit_test"
        dataset = Dataset.read_from_hdx(dataset_name)
        if dataset:
            dataset.delete_from_hdx()
        title = "HDX CLI toolkit test"
        dataset = Dataset({"name": dataset_name, "title": title})
        dataset.update_from_yaml(
            os.path.join(os.path.dirname(__file__), "fixtures", "hdx_dataset_static.yaml")
        )
        countryiso3s = ["AFG", "PSE", "SYR", "YEM"]
        dataset.add_country_locations(countryiso3s)
        tags = ["conflict-violence", "displacement", "hxl"]
        dataset.add_tags(tags)

        test_resource_name = "test_resource_1"
        resource = Resource(
            {
                "name": test_resource_name,
                "description": "Test Resource 1",
            }
        )
        resource_file_path = os.path.join(os.path.dirname(__file__), "fixtures", "test.csv")
        resource.set_format("csv")
        resource.set_file_to_upload(resource_file_path)

        dataset.add_update_resource(resource)

        dataset.create_in_hdx(hxl_update=False, updated_by_script="hdx_cli_toolkit_ignore")

        assert Dataset.read_from_hdx(dataset_name) is not None

        # This is the test of update_resource_in_hdx
        original_resources = dataset.get_resources()

        new_resource_file_path = os.path.join(os.path.dirname(__file__), "fixtures", "test-2.csv")
        statuses = update_resource_in_hdx(
            dataset_name, test_resource_name, "stage", new_resource_file_path, dry_run=False
        )

        assert statuses[0] == "Update successful"

        revised_dataset = Dataset.read_from_hdx(dataset_name)
        revised_resources = revised_dataset.get_resources()

        assert len(original_resources) == len(revised_resources)
        assert original_resources[0].data["name"] == "test_resource_1"
        assert revised_resources[0].data["name"] == "test_resource_1"

        assert original_resources[0].data["url"].endswith("test.csv")
        assert revised_resources[0].data["url"].endswith("test-2.csv")

        assert revised_resources[0].data["size"] > original_resources[0].data["size"]

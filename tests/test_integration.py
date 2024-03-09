#!/usr/bin/env python
# encoding: utf-8

# These tests interact directly with the staging HDX instance

import os

from hdx.data.dataset import Dataset
from hdx.data.resource import Resource

from hdx_cli_toolkit.hdx_utilities import update_resource_in_hdx, configure_hdx_connection


class TestHDXToolkit:
    def test_update_resource(
        self,
    ):
        configure_hdx_connection("stage")
        name = "hdx_cli_toolkit_test"
        dataset = Dataset.read_from_hdx(name)
        if dataset:
            dataset.delete_from_hdx()
        title = "HDX CLI toolkit test"
        dataset = Dataset({"name": name, "title": title})
        dataset.update_from_yaml(
            os.path.join(os.path.dirname(__file__), "fixtures", "hdx_dataset_static.yaml")
        )

        # dataset.set_time_period(today)
        # dataset.set_expected_update_frequency("Every week")
        # dataset.set_subnational(True)
        countryiso3s = ["AFG", "PSE", "SYR", "YEM"]
        dataset.add_country_locations(countryiso3s)
        tags = ["conflict-violence", "displacement", "hxl"]
        dataset.add_tags(tags)

        resource = Resource(
            {
                "name": "test_resource_1",
                "description": "Test Resource 1",
            }
        )
        resource_file_path = os.path.join(os.path.dirname(__file__), "fixtures", "test.csv")
        resource.set_format("csv")
        resource.set_file_to_upload(resource_file_path)

        dataset.add_update_resource(resource)

        dataset.create_in_hdx(hxl_update=False, updated_by_script="hdx_cli_toolkit_ignore")

        assert Dataset.read_from_hdx(name) is not None

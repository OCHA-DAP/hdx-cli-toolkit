#!/usr/bin/env python
# encoding: utf-8

import csv
import os

from hdx_cli_toolkit.utilities import (
    write_dictionary,
    censor_secret,
    read_attributes,
    print_banner,
    print_table_from_list_of_dicts,
    str_to_bool,
    make_conversion_func,
    make_path_unique,
)


ATTRIBUTES_FILE_PATH = os.path.join(os.path.dirname(__file__), "fixtures", "attributes.csv")
DATASET_NAME = "climada-litpop-showcase"
REFERENCE_ATTRIBUTES = read_attributes(DATASET_NAME, attributes_filepath=ATTRIBUTES_FILE_PATH)


def test_write_dictionary_to_local_file():
    temp_file_path = os.path.join(os.path.dirname(__file__), "fixtures", "test.csv")
    if os.path.isfile(temp_file_path):
        os.remove(temp_file_path)

    dict_list = [
        {"a": 1, "b": 2, "c": 3},
        {"a": 4, "b": 5, "c": 6},
        {"a": 7, "b": 8, "c": 9},
    ]

    status = write_dictionary(temp_file_path, dict_list)

    with open(temp_file_path, "r", encoding="utf-8") as file_handle:
        rows_read = list(csv.DictReader(file_handle))

    assert len(rows_read) == 3
    assert rows_read[0] == {"a": "1", "b": "2", "c": "3"}
    assert "New file" in status
    assert "is being created" in status


def test_censor_short_secret():
    censored_secret = censor_secret("012345")
    assert censored_secret == "******"


def test_censor_long_secret():
    censored_secret = censor_secret("ABCDEF0123456789")
    assert censored_secret == "******0123456789"


def test_read_attributes():
    assert len(REFERENCE_ATTRIBUTES["tags"]) == 3
    assert REFERENCE_ATTRIBUTES["parent_dataset"] == "hdx_cli_toolkit_test"


def test_read_attributes_json():
    attributes_json_file_path = os.path.join(
        os.path.dirname(__file__), "fixtures", "attributes-single.json"
    )

    dataset_attributes = read_attributes(
        DATASET_NAME, attributes_filepath=attributes_json_file_path
    )

    assert REFERENCE_ATTRIBUTES == dataset_attributes


def test_read_attributes_json_list():
    attributes_json_list_file_path = os.path.join(
        os.path.dirname(__file__), "fixtures", "attributes-list.json"
    )

    dataset_attributes = read_attributes(
        DATASET_NAME, attributes_filepath=attributes_json_list_file_path
    )

    assert REFERENCE_ATTRIBUTES == dataset_attributes


def test_print_banner(capfd):
    print_banner("test_action")
    output, errors = capfd.readouterr()

    assert errors == ""
    parts = output.split("\n")
    assert len(parts) == 5
    for part in parts:
        if len(part) == 0:
            continue
        assert len(part) == 42


def test_print_table_from_list_of_dicts(capfd):
    test_data = [{"column a": "a", "column b": "b", "column c": "c"}]
    print_table_from_list_of_dicts(test_data)

    output, errors = capfd.readouterr()
    assert errors == ""
    parts = output.split("\n")
    assert len(parts) == 6
    for part in parts:
        if len(part) == 0:
            continue
        assert len(part) in [29, 31]


def test_str_to_bool():
    result = str_to_bool("true")

    assert result
    result = str_to_bool("False")

    assert not result


def test_make_conversion_func():
    test_values = [(1, "int"), ("string", "str"), (1.4, "float"), (True, "bool")]
    for test_value in test_values:
        _, func_name = make_conversion_func(test_value[0])
        assert func_name == test_value[1]


def test_make_path_unique():
    input_path = __file__
    unique_path = make_path_unique(input_path)
    input_filename = os.path.basename(input_path)

    assert os.path.basename(unique_path) == input_filename.replace(".py", "-1.py")

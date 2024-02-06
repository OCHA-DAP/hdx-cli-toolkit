#!/usr/bin/env python
# encoding: utf-8

import csv
import os

from hdx_cli_toolkit.utilities import write_dictionary, censor_secret


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

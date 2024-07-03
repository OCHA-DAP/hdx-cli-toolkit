#!/usr/bin/env python
# encoding: utf-8

import csv
import dataclasses
import datetime
import json
import math
import os

from collections.abc import Callable
from typing import Any

import click


def write_dictionary(
    output_filepath: str, output_rows: list[dict[str, Any]], append: bool = True
) -> str:
    """Write a list of dictionaries to a CSV file

    Arguments:
        output_filepath {str} -- a file path for the output CSV file
        output_rows {list[dict[str, Any]]} -- a list of dictionaries

    Keyword Arguments:
        append {bool} -- if True rows are appended to an existing file (default: {True})

    Returns:
        str -- a status message
    """
    keys = list(output_rows[0].keys())
    newfile = not os.path.isfile(output_filepath)

    if not append and not newfile:
        os.remove(output_filepath)
        newfile = True

    with open(output_filepath, "a", encoding="utf-8", errors="ignore") as output_file:
        dict_writer = csv.DictWriter(
            output_file,
            keys,
            lineterminator="\n",
        )
        if newfile:
            dict_writer.writeheader()
        dict_writer.writerows(output_rows)

    status = _make_write_dictionary_status(append, output_filepath, newfile)

    return status


def _make_write_dictionary_status(append: bool, filepath: str, newfile: bool) -> str:
    """A simple helper function to generate a status message for write_dictionary

    Arguments:
        append {bool} -- the append flag
        filepath {str} -- the file path for the output CSV file
        newfile {bool} -- a file indicating whether a newfile was created

    Returns:
        str -- a status string
    """
    status = ""
    if not append and not newfile:
        status = f"Append is False, and {filepath} exists therefore file is being deleted"
    elif not newfile and append:
        status = f"Append is True, and {filepath} exists therefore data is being appended"
    else:
        status = f"New file {filepath} is being created"
    return status


def print_table_from_list_of_dicts(
    column_data_rows: list[dict[str, str]],
    excluded_fields: None | list[str] = None,
    included_fields: None | list[str] = None,
    truncate_width: int = 130,
    max_total_width: int = 150,
) -> None:
    """A helper function to print a list of dictionaries as a table

    Arguments:
        column_data_rows {list[dict]} -- the list of dictionaries to print

    Keyword Arguments:
        excluded_fields {None|list} -- any fields to be ommitted, none excluded by default
                                        (default: {None})
        included_fields {None|list} -- any fields to be included, all included by default
                                        (default: {None})
        truncate_width {int} -- width at which to truncate a column (default: {130})
        max_total_width {int} -- total width of the table (default: {150})
    """
    if (len(column_data_rows)) == 0:
        return
    if dataclasses.is_dataclass(column_data_rows[0]):
        temp_data = []
        for row in column_data_rows:
            temp_data.append(dataclasses.asdict(row))
        column_data_rows = temp_data

    if excluded_fields is None:
        excluded_fields = []

    if included_fields is None:
        included_fields = list(column_data_rows[0])

    column_table_header_dict = {}
    for field in included_fields:
        widths = [len(str(x[field])) for x in column_data_rows]
        widths.append(len(field))  # .append(len(field))
        max_field_width = max(widths)

        column_table_header_dict[field] = max_field_width + 1
        if max_field_width > truncate_width:
            column_table_header_dict[field] = truncate_width

    total_width = (
        sum(v for k, v in column_table_header_dict.items() if k not in excluded_fields)
        + len(column_table_header_dict)
        - 1
    )

    if total_width > max_total_width:
        print(
            f"\nCalculated total_width of {total_width} "
            f"exceeds proposed max_total_width of {max_total_width}. "
            "The resulting table may be unattractive.",
            flush=True,
        )

    print("-" * total_width, flush=True)

    for k in included_fields:
        if k not in excluded_fields:
            width = column_table_header_dict[k]
            print(f"|{k:<{width}.{width}}", end="", flush=True)
    print("|", flush=True)
    print("-" * total_width, flush=True)

    for row in column_data_rows:
        for k in included_fields:
            value = row[k]

            if k not in excluded_fields:
                width = column_table_header_dict[k]
                print(f"|{str(value):<{width}.{width}}", end="", flush=True)
        print("|", flush=True)

    print("-" * total_width, flush=True)


def censor_secret(secret: str) -> str:
    """A function to censor a string containing a secret. If the length of the string is less than
    10 characters then all characters are censored, if it is more then the last 10 characters are
    left in place.

    Arguments:
        secret {str} -- a secret

    Returns:
        str -- the secret, censored
    """
    if len(secret) < 10:
        censored_secret = len(secret) * "*"
    else:
        censored_secret = (len(secret) - 10) * "*" + secret[-10:]
    return censored_secret


def str_to_bool(x: str) -> bool:
    """A function that converts a string into a boolean using the formalism that any casing of the
    string "True" is True and all other strings are False. The default behaviour of the builtin
    bool is that any non-empty string is True

    Arguments:
        x {str} -- a string

    Returns:
        bool -- a boolean representation of the string
    """
    return x.lower() == "true"


def make_conversion_func(value: Any) -> tuple[Callable | None, str]:
    """A function that takes a value of Any type and returns the function that will convert a string
     to that type. Used to take values from dataset attributes and work out how to convert a string
     from the commandline.

    Arguments:
        value {Any} -- a value of Any type

    Returns:
        tuple[Callable | None, str] -- a function that will convert a string to the provided type,
                                       and the name of that type
    """
    value_type = type(value)
    if value_type.__name__ == "bool":
        conversion_func = str_to_bool
    elif value_type.__name__ == "int":
        conversion_func = int
    elif value_type.__name__ == "float":
        conversion_func = float
    elif value_type.__name__ == "str":
        conversion_func = str
    else:
        conversion_func = None

    return conversion_func, value_type.__name__


def read_attributes(dataset_name: str, attributes_filepath: str) -> dict[str, str]:
    """A function for reading attributes from a standard attributes.csv file with columns:
    dataset_name,timestamp,attribute,value,secondary_value or a JSON format file containing
    either a list or a single dictionary.

    Arguments:
        dataset_name {str} -- the name of the dataset for which attributes are required
        attributes_filepath {str} -- path to attributes file either CSV or JSON

    Returns:
        dict -- a dictionary containing the attributes
    """

    if attributes_filepath.lower().endswith(".csv"):
        with open(attributes_filepath, "r", encoding="UTF-8") as attributes_filehandle:
            attribute_rows = csv.DictReader(attributes_filehandle)
            attributes = {}

            for row in attribute_rows:
                if list(row.keys()) != [
                    "dataset_name",
                    "timestamp",
                    "attribute",
                    "value",
                    "secondary_value",
                ]:
                    print(
                        "Attributes.csv file must have columns: "
                        "dataset_name, timestamp, attribute, value, secondary_value"
                    )
                    return attributes

                if row["dataset_name"] != dataset_name:
                    continue
                if row["attribute"] in ["resource", "skip_country", "showcase", "tags"]:
                    if row["attribute"] not in attributes:
                        attributes[row["attribute"]] = [row["value"]]
                    else:
                        attributes[row["attribute"]].append(row["value"])
                else:
                    attributes[row["attribute"]] = row["value"]
    elif attributes_filepath.lower().endswith(".json"):
        with open(attributes_filepath, "r", encoding="utf-8") as attributes_filehandle:
            raw_json = json.load(attributes_filehandle)
            if isinstance(raw_json, dict):
                attributes = raw_json
            elif isinstance(raw_json, list):
                for entry in raw_json:
                    if entry["name"] == dataset_name:
                        attributes = entry
                        break

    else:
        print(f"File type at {attributes_filepath} is not recognised")
        attributes = {}

    return attributes


def print_banner(action: str):
    """Simple function to output a banner to console, uses click's secho command but not colour
    because the underlying colorama does not output correctly to git-bash terminals.

    Arguments:
        action {str} -- _description_
    """
    title = f"HDX CLI toolkit - {action}"
    timestamp = f"Invoked at: {datetime.datetime.now().isoformat()}"
    width = max(len(title), len(timestamp))
    click.secho((width + 4) * "*", bold=True)
    click.secho(f"* {title:<{width}} *", bold=True)
    click.secho(f"* {timestamp:<{width}} *", bold=True)
    click.secho((width + 4) * "*", bold=True)


def make_path_unique(input_path: str) -> str:
    filename, extension = os.path.splitext(input_path)
    counter = 1

    unique_path = input_path
    while os.path.exists(unique_path):
        unique_path = f"{filename}-{str(counter)}{extension}"
        counter += 1

    return unique_path


def print_dictionary_comparison(
    dict1: dict[str, str],
    dict2: dict[str, str],
    name1: str = "first_dict",
    name2: str = "second_dict",
    differences: bool = False,
) -> None:
    unified_keys = set(list(dict1.keys()))
    unified_keys.update(list(dict2.keys()))
    total_width = 100
    key_width = max(len(x) for x in unified_keys) + 1

    dict_one_width = math.floor((total_width - 4 - key_width) / 2)
    dict_two_width = dict_one_width
    total_width = key_width + dict_one_width + dict_two_width + 4

    print("\n", flush=True)
    print("-" * total_width, flush=True)
    print(
        f"|{'key':<{key_width}}|{name1:<{dict_one_width}}|{name2:<{dict_two_width}}|",
        flush=True,
    )
    print("-" * total_width, flush=True)

    for key in unified_keys:
        value1 = dict1.get(key, "Not present")
        value2 = dict2.get(key, "Not present")

        if differences and value1 == value2:
            continue
        else:
            print(
                f"|{key:<{key_width}}"
                f"|{str(value1):<{dict_one_width}.{dict_one_width}}"
                f"|{str(value2):<{dict_two_width}.{dict_two_width}}|",
                flush=True,
            )
    print("-" * total_width, flush=True)


def query_dict(
    keys: list[str],
    dataset_dict: dict[str, Any],
    output_row: dict[str, Any],
) -> list[dict[str, Any]]:
    """This function takes a list of key definitions which can be simple (i.e. archived) or nested
    (resource.name). Nested keys can access simple dictionaries or the same key in each element
    of a list. Key depth is limited to 2, and list.list nested keys are not handled.

    Arguments:
        keys {list[str]} -- a list of key definitions
        dataset_dict {dict[str, Any]} -- the dictionary from which data is to be extracted
        output_row {dict[str, Any]} -- a dictionary containing any prefix information

    Returns:
        list[dict[str, Any]] -- a list of dictionaries which contain the prefix and extracted values
    """
    output = []
    # Handle non-list keys - generates 1 output row
    for key_ in keys:
        list_keys = []
        if "." not in key_:
            # Handles simple keys
            output_row[key_] = dataset_dict.get(key_, f"'{key_}' key absent")
        else:
            # Handles nested keys
            try:
                key1, key2 = key_.split(".")
            except ValueError:
                print(
                    f"'{key_}' is nested to depth {len(key_.split('.'))}, maximum depth is 2",
                    flush=True,
                )
                output_row[key_] = "Maximum key depth is 2"
                continue
            intermediate_value = dataset_dict.get(key1, f"'{key1}' key absent")
            if isinstance(intermediate_value, dict):
                output_row[key_] = intermediate_value.get(key2, f"'{key2}' key absent")
            elif isinstance(intermediate_value, list):
                list_keys.append((key1, key2, intermediate_value))

    if len(list_keys) != 0:
        # Handle list keys
        for item in list_keys:
            for element in item[2]:
                tmp_row = output_row.copy()
                tmp_row[key_] = element.get(item[1], f"'{item[1]}' key absent")
                output.append(tmp_row)
    else:
        output.append(output_row)

    return output


def traverse(keys, dictionary, value_list=None):
    if value_list is None:
        value_list = []

    if len(keys) == 0:
        return value_list

    if isinstance(dictionary, list):
        for item in dictionary:
            value = traverse(keys, item)
            value_list.append(value[0])
        return value_list
    else:
        value = dictionary.get(keys[0], f"{keys[0]} absent")

    if value == f"{keys[0]} absent":
        value_list.append(value)
        return value_list

    if len(keys[1:]) == 0:
        value_list.append(value)
        return value_list

    return traverse(keys[1:], value, value_list)

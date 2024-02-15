#!/usr/bin/env python
# encoding: utf-8

import csv
import dataclasses
import os

from collections.abc import Callable
from typing import Any, Optional


def write_dictionary(
    output_filepath: str, output_rows: list[dict[str, Any]], append: bool = True
) -> str:
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
    status = ""
    if not append and not newfile:
        status = f"Append is False, and {filepath} exists therefore file is being deleted"
    elif not newfile and append:
        status = f"Append is True, and {filepath} exists therefore data is being appended"
    else:
        status = f"New file {filepath} is being created"
    return status


def print_table_from_list_of_dicts(
    column_data_rows: list[dict],
    excluded_fields: Optional[list] = None,
    included_fields: Optional[list] = None,
    truncate_width: int = 130,
    max_total_width: int = 150,
) -> None:
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
    if len(secret) < 10:
        censored_secret = len(secret) * "*"
    else:
        censored_secret = (len(secret) - 10) * "*" + secret[-10:]
    return censored_secret


def str_to_bool(x: str) -> bool:
    return x == "True"


def make_conversion_func(value: Any) -> tuple[Callable | None, str]:
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

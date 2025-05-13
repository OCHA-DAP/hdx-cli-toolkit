#!/usr/bin/env python
# encoding: utf-8

# This is a script to extract the dataset names from the data grid recipes
# To use,
# 1. clone this repo: https://github.com/OCHA-DAP/data-grid-recipes
# 2. run this script in the root of the repo
# 3. copy the output datagrid-datasets.csv to the data subdirectory of hdx_cli_toolkit
# Ian Hopkinson 2025-05-13
# Setup a venv with:
# python -m venv venv
# source venv/Scripts/activate
# pip install pyyaml

# For some reason the virtual env didn't work and I ended up using the global environment
import csv
import pathlib
import yaml


def main():
    dataset_list = []
    row_template = {"filename": "", "category": "", "raw_rules": "", "dataset_name": ""}
    files = [f for f in pathlib.Path().glob("*.yml")]
    for file in files:
        if "template" not in str(file):
            print(file, flush=True)

            with open(file, "r") as f:
                data = yaml.load(f, Loader=yaml.SafeLoader)

            # Print the values as a dictionary
            for category in data["categories"]:
                print(category["name"], flush=True)
                for data_series in category["data_series"]:
                    try:
                        for part in data_series["rules"]["include"]:
                            row = row_template.copy()
                            row["filename"] = file
                            row["category"] = category["name"]
                            row["raw_rules"] = part
                            row["dataset_name"] = part.replace("(name:", "").replace(")", "")
                            dataset_list.append(row)
                    except TypeError:
                        pass

            # print(json.dumps(data, indent=4), flush=True)

    keys = dataset_list[0].keys()

    with open("datagrid-datasets.csv", "w", newline="") as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(dataset_list)


if __name__ == "__main__":
    main()

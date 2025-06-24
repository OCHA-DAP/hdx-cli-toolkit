#!/usr/bin/env python
# encoding: utf-8

"""
This script calculates data quality scores for the datasets on this spreadsheet compiled by Melanie
Rabier:
https://docs.google.com/spreadsheets/d/1Wj_wa2L_4BR6bzN7MQg0b0gInIcPby9DQlio69Sr55g/edit?gid=196764264#gid=196764264

The data from the "Review Quality" are processed manually to produce the csv
"2025-06-24-data-quality-mr.csv".
"""
import csv
import os

from hdx_cli_toolkit.data_quality_utilities import compile_data_quality_report

BENCHMARK_FILE_PATH = os.path.join(os.path.dirname(__file__), "2025-06-24-data-quality-mr.csv")


def main():
    with open(BENCHMARK_FILE_PATH, encoding="utf-8") as benchmark_file:
        rows = csv.DictReader(benchmark_file)

        for row in rows:
            report = compile_data_quality_report(dataset_name=row["dataset_name"])
            total_score = (
                report["relevance_score"]
                + report["timeliness_score"]
                + report["accessibility_score"]
            )
            print(row["dataset_name"], row["Total"], total_score, flush=True)


if __name__ == "__main__":
    main()

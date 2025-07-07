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
    with (
        open(BENCHMARK_FILE_PATH, encoding="utf-8") as benchmark_file,
        open(
            "2025-07-07-benchmark-mr-comparison.csv",
            "a",
            encoding="utf-8",
            newline="\n",
        ) as output_file,
    ):
        output = csv.writer(output_file)
        rows = csv.DictReader(benchmark_file)
        print("dataset_name, manual_score, automated_score, equivalent_score", flush=True)
        output.writerow(["dataset_name", "manual_score", "automated_score", "equivalent_score"])
        for row in rows:
            report = compile_data_quality_report(dataset_name=row["dataset_name"])
            try:
                equivalent_score = calculate_equivalent_score(report)
            except KeyError:
                equivalent_score = 0
            total_score = (
                report["relevance_score"]
                + report["timeliness_score"]
                + report["accessibility_score"]
                + report["interpretability_score"]
                + report["interoperability_score"]
                + report["findability_score"]
            )
            print(
                f'{row["dataset_name"]}, {row["Total"]}, {total_score}, {equivalent_score}',
                flush=True,
            )
            output.writerow([report["dataset_name"], row["Total"], total_score, equivalent_score])


def calculate_equivalent_score(report: dict) -> int:
    total_score = 0

    # Relevance - Data Grid, Data Series, Signals
    for feature in ["in_dataseries", "in_data_grids", "in_signals"]:
        if report["relevance"][feature]:
            total_score += 1

    # Timeliness - is_fresh
    if report["timeliness"]["is_fresh"]:
        total_score += 1
    # Accessibility - we can't do "comes up in search" so assume 1 for that, rest OK
    if report["accessibility"]["n_tags"] >= 2:
        total_score += 1
    format_score = 0
    in_hapi = 0
    is_hxlated = 0
    stable_schema = 0
    for resource in report["accessibility"]["resources"]:
        if int(resource["format_score"][0]) >= 1:
            format_score = 1
        if resource["in_hapi"]:
            in_hapi = 1
        if resource["is_hxlated"]:
            is_hxlated = 1
        if resource["n_schema_changes"] == 0:
            stable_schema = 1

    total_score += format_score + in_hapi + is_hxlated + stable_schema

    # Interpretability - going to assume 3 for this since we can't measure any of it
    total_score += 3
    # Interoperability - we have a "p-coded flag", for APIs just assume 1
    p_coded = 0
    for resource in report["interoperability"]["resources"]:
        if resource["p_coded"]:
            p_coded = 1
    total_score += p_coded
    # Findability - not really implemented yet, just assume 1
    total_score += 1
    return total_score


if __name__ == "__main__":
    main()

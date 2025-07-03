#!/usr/bin/env python
# encoding: utf-8

"""
This script calculates data quality scores for random datasets, writing them to file
"""

from hdx_cli_toolkit.data_quality_utilities import compile_data_quality_report

import csv


def main():
    with open(
        "2025-07-03-benchmark-lucky_dip.csv",
        "a",
        encoding="utf-8",
        newline="\n",
    ) as output_file:
        output = csv.writer(output_file)
        while True:
            report = compile_data_quality_report(dataset_name="", lucky_dip=True)
            total_score = (
                report["relevance_score"]
                + report["timeliness_score"]
                + report["accessibility_score"]
                + report["interpretability_score"]
                + report["interoperability_score"]
                + report["findability_score"]
            )
            print(f"{report['dataset_name']}, {total_score}", flush=True)
            output.writerow(
                [
                    report["dataset_name"],
                    report["relevance_score"],
                    report["timeliness_score"],
                    report["accessibility_score"],
                    report["interpretability_score"],
                    report["interoperability_score"],
                    report["findability_score"],
                    total_score,
                ]
            )


if __name__ == "__main__":
    main()

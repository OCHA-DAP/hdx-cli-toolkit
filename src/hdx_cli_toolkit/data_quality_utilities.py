#!/usr/bin/env python
# encoding: utf-8

import csv
import datetime
import statistics
import sys


from typing import Optional
from pathlib import Path
from random import randrange
from hxl.input import hash_row
from hdx_cli_toolkit.ckan_utilities import fetch_data_from_ckan_package_search, get_hdx_url_and_key
from hdx_cli_toolkit.hapi_utilities import get_hapi_resource_ids
from hdx_cli_toolkit.stable_schema_utilities import (
    read_metadata_from_hdx,
    summarise_resource_changes,
    get_last_complete_check,
)

CKAN_API_ROOT_URL = "https://data.humdata.org/api/action/"
HAPI_RESOURCE_IDS = None
FORMAT_GOLD_2 = ["CSV", "JSON", "GEOJSON", "XML", "KML", "GEOTIFF", "GEOPACKAGE", "TXT"]
FORMAT_SILVER_1 = ["XLSX", "XLS", "SHP", "GEODATABASE", "GEOSERVICE"]
FORMAT_BRONZE_0 = ["PDF", "DOC", "DOCX", "WEB APP", "GARMIN IMG", "EMF", "PNG"]

GEODENOMINATION_HXL = ["#geo+lat", "#geo+lon", "#geo+coord", "#country+code", "#country+v_iso3"]

SIGNALS_DATASETS = [
    "asap-hotspots-monthly",
    "global-acute-food-insecurity-country-data",
    "inform-global-crisis-severity-index",
    "global-market-monitor",
]
SIGNALS_ORGANIZATIONS = ["acled", "international-displacement-monitoring-centre-idmc"]


SHAPE_INFO_DATA_TYPE_LOOKUP = {
    "character varying": "string",
    "integer": "integer",
    "bigint": "integer",
    "numeric": "float",
    "USER-DEFINED": "user-defined",
    "timestamp with time zone": "timestamp",
    "date": "date",
    "ARRAY": "list",
    "boolean": "boolean",
}


def compile_data_quality_report(
    dataset_name: str | None,
    hdx_site: Optional[str] = "stage",
    lucky_dip: bool | None = False,
    metadata_dict: dict | None = None,
):
    global HAPI_RESOURCE_IDS
    HAPI_RESOURCE_IDS = get_hapi_resource_ids("hapi")
    if metadata_dict is None:
        if lucky_dip:
            metadata_dict = lucky_dip_search(hdx_site=hdx_site)
            if metadata_dict:
                dataset_name = metadata_dict["result"]["name"]
        else:
            if dataset_name is not None:
                metadata_dict = read_metadata_from_hdx(dataset_name)

    if metadata_dict is None:
        print(f"Dataset '{dataset_name}' not found on HDX", flush=True)
        return {"dataset_name": dataset_name, "relevance": {"in_hdx": False}}

    report = {}
    report["dataset_name"] = dataset_name
    report["relevance_score"] = 0
    report["timeliness_score"] = 0
    report["accessibility_score"] = 0
    report["interpretability_score"] = 0
    report["interoperability_score"] = 0
    report["findability_score"] = 0
    report["total_score"] = 0
    report["priority_score"] = 0
    report["normalized_score"] = 0

    report = add_relevance_entries(metadata_dict, report)
    report = add_timeliness_entries(metadata_dict, report)
    report = add_accessibility_entries(metadata_dict, report)
    report = add_interpretability_entries(metadata_dict, report)
    report = add_interoperability_entries(metadata_dict, report)
    report = add_findability_entries(metadata_dict, report)

    report["total_score"] = (
        report["relevance_score"]
        + report["timeliness_score"]
        + report["accessibility_score"]
        + report["interpretability_score"]
        + report["interoperability_score"]
        + report["findability_score"]
    )

    report["priority_score"] = (
        report["relevance"]["downloads_score"]
        + int(report["relevance"]["in_signals"])
        + int(report["findability_score"])
        + int(report["timeliness"]["is_fresh"])
        + int(report["timeliness"]["cadence_score"])
        + int(report["interpretability"]["has_data_dictionary"])
        + int(report["interoperability"]["has_standard_geodenomination"])
        + int(report["accessibility"]["has_stable_schema"])
    )

    max_total_score = 0
    for dimension in [
        "Relevance",
        "Timeliness",
        "Accessibility",
        "Interpretability",
        "Interoperability",
        "Findability",
    ]:
        max_score = report[dimension.lower()]["max_score"]
        max_total_score = max_total_score + 1
        report["normalized_score"] += report[f"{dimension.lower()}_score"] / max_score

    report["normalized_score"] = round(report["normalized_score"], 2)

    # if report["priority_score"] < 4:
    #     report["medal"] = "Bronze"
    # elif report["priority_score"] >= 7:
    #     report["medal"] = "Gold"
    # else:
    #     report["medal"] = "Silver"

    return report


def make_resource_centric_report(report) -> list[dict]:
    resource_reports_dict = {}
    for dimension in [
        "relevance",
        "timeliness",
        "accessibility",
        "interpretability",
        "interoperability",
        "findability",
    ]:
        # findability does not have a resources key
        if dimension == "findability":
            continue
        for resource in report[dimension]["resources"]:
            if resource["name"] not in resource_reports_dict.keys():
                resource_reports_dict[resource["name"]] = {}
            for key, value in resource.items():
                if key == "name":
                    continue
                resource_reports_dict[resource["name"]][key] = value

    resource_reports_list = []
    for key in resource_reports_dict.keys():
        new_resource_report = {}
        new_resource_report["datatime"] = datetime.datetime.now().isoformat()
        new_resource_report["dataset_name"] = report["dataset_name"]
        new_resource_report["resource_name"] = key
        new_resource_report["resource_score"] = 0
        resource_score = 0
        for key, value in resource_reports_dict[key].items():
            new_resource_report[key] = value
            resource_score += int(value)

        new_resource_report["resource_score"] = resource_score
        resource_reports_list.append(new_resource_report)

    sorted_resource_reports_list = sorted(
        resource_reports_list, key=lambda x: x["resource_score"], reverse=True
    )

    return sorted_resource_reports_list


def add_relevance_entries(metadata_dict: dict | None, report: dict) -> dict:
    if metadata_dict:
        dataset_name = metadata_dict["result"]["name"]
    else:
        return report
    report["relevance"] = {}
    if metadata_dict is None:
        report["relevance"]["in_hdx"] = False
        return report
    report["relevance"]["in_hdx"] = True
    report["relevance"]["in_dataseries"] = (
        True if "dataseries_name" in metadata_dict["result"].keys() else False
    )
    report["relevance"]["in_pipeline"] = (
        True if "updated_by_script" in metadata_dict["result"].keys() else False
    )
    report["relevance"]["in_cod"] = (
        metadata_dict["result"]["cod_level"]
        if "cod_level" in metadata_dict["result"].keys()
        else False
    )

    report["relevance"]["in_hapi_output"] = True if dataset_name.startswith("hdx-hapi-") else False
    report["relevance"]["in_signals"] = check_for_signals(metadata_dict)
    report["relevance"]["in_crisis"] = check_for_crisis(metadata_dict)

    report["relevance"]["in_hapi_input"] = check_for_hapi(metadata_dict)
    report["relevance"]["in_data_grids"] = check_for_datagrid(metadata_dict)

    relevance_summary = [1 if v else 0 for k, v in report["relevance"].items()]
    report["relevance"]["downloads_score"] = calculate_downloads_score(
        metadata_dict["result"]["total_res_downloads"]
    )
    report["relevance_score"] = sum(relevance_summary) + report["relevance"]["downloads_score"]

    max_score = sum([1 for k, v in report["relevance"].items()]) + 3  # 3 for the downloads score
    report["relevance"]["resources"] = []
    for resource in metadata_dict["result"]["resources"]:
        resource_report = {}
        resource_report["name"] = resource["name"]
        if resource["id"] in HAPI_RESOURCE_IDS:
            resource_report["in_hapi_input"] = True
        else:
            resource_report["in_hapi_input"] = False
        report["relevance"]["resources"].append(resource_report)

    report["relevance"]["max_score"] = max_score
    return report


def calculate_downloads_score(n_downloads: str) -> int:
    # 0 – 0 – 100 no downloads
    # 1 –  1-100 downloads – catches the obvious peak
    # 2 – 101-300 downloads and above
    # 3 – 301 upwards

    n_downloads_int = int(n_downloads)

    score = 0
    if n_downloads_int == 0:
        score = 0
    elif n_downloads_int >= 1 and n_downloads_int <= 100:
        score = 1
    elif n_downloads_int >= 101 and n_downloads_int <= 300:
        score = 2
    elif n_downloads_int >= 301:
        score = 3

    return score


def add_timeliness_entries(metadata_dict: dict | None, report: dict) -> dict:
    report["timeliness"] = {}
    if metadata_dict is None:
        return report

    due_date = metadata_dict["result"].get("due_date", False)

    report["timeliness"]["is_fresh"] = metadata_dict["result"].get("is_fresh", False)
    report["timeliness"]["is_crisis_relevant"] = (
        True
        if check_for_crisis(metadata_dict)
        and metadata_dict["result"]["data_update_frequency"] == "0"
        else False
    )
    report["timeliness"]["cadence_score"] = None

    resource_changes = summarise_resource_changes(metadata_dict)
    report["timeliness"]["resources"] = []
    expected_cadence = metadata_dict["result"]["data_update_frequency"]

    for resource in metadata_dict["result"]["resources"]:
        cadence_score = 0
        resource_report = {}
        resource_report["name"] = resource["name"]
        if due_date is not None:
            resource_report["is_fresh"] = (
                True if datetime.datetime.now().isoformat() < due_date else False
            )
        # Process fs_check_info
        checks = resource_changes[resource["name"]]["checks"]
        # Number of updates
        # resource_report["n_updates"] = len(checks)
        # Days since last update
        if len(checks) != 0:
            days_between_updates = []
            previous = datetime.datetime.fromisoformat(checks[0][0:10])
            for check in checks[1:]:
                current = datetime.datetime.fromisoformat(check[0:10])
                days = (current - previous).days
                days_between_updates.append(days)
                previous = current
            # Cadence is as advertised
            # resource_report["cadence_mean_ratio"] = None
            # resource_report["cadence_std_ratio"] = None
            resource_report["cadence_score"] = cadence_score
            if float(expected_cadence) > 0 and len(days_between_updates) > 1:
                average_interval = statistics.mean(days_between_updates)
                cadence_mean_ratio = round(average_interval / float(expected_cadence), 2)
                # Updates are regular
                std_interval = statistics.stdev(days_between_updates)
                cadence_std_ratio = round(std_interval / float(expected_cadence), 2)
                if abs(cadence_mean_ratio - 1) < 0.1:
                    cadence_score += 1
                if cadence_std_ratio < 0.1:
                    cadence_score += 1
            resource_report["cadence_score"] = cadence_score
            resource_report["has_fs_check_or_shape_info"] = True
        else:
            resource_report["cadence_score"] = 0
            resource_report["has_fs_check_or_shape_info"] = False

        report["timeliness"]["resources"].append(resource_report)

    # Compile cadence score
    # 2 if cadence is correct (mean ratio~1) and stable (std ratio ~0) for at least 1 resource
    # 1 if one of mean ratio and std ratio is good for at least 1 resource
    # 0 if neither
    cadence_score = max(x.get("cadence_score", 1) for x in report["timeliness"]["resources"])
    report["timeliness"]["cadence_score"] = cadence_score

    timeliness_summary = cadence_score
    if report["timeliness"]["is_fresh"]:
        timeliness_summary += 1
    if report["timeliness"]["is_crisis_relevant"]:
        timeliness_summary += 1
    report["timeliness_score"] = timeliness_summary
    report["timeliness"]["max_score"] = 2 + 1 + 1  # Cadence + is_fresh + is_crisis_relevant)

    return report


def add_accessibility_entries(metadata_dict: dict | None, report: dict) -> dict:
    report["accessibility"] = {}
    report["accessibility"]["is_hxlated"] = 0
    report["accessibility"]["format_score"] = 0
    report["accessibility"]["has_stable_schema"] = 0

    if metadata_dict is None:
        return report

    n_tags = len(metadata_dict["result"]["tags"])
    n_countries = len(metadata_dict["result"]["groups"])
    if n_tags > 0 and n_countries > 0:
        report["accessibility"]["has_tags_and_countries"] = True
    else:
        report["accessibility"]["has_tags_and_countries"] = False
    resource_changes = summarise_resource_changes(metadata_dict)
    report["accessibility"]["resources"] = []
    max_resource_score = 0
    best_resource_is_hxlated = 0
    best_resource_n_schema_changes = 0
    best_resource_format_score = 0
    for resource in metadata_dict["result"]["resources"]:
        resource_score = 0
        resource_report = {}
        resource_report["name"] = resource["name"]
        format_ = resource["format"].upper()
        if format_ in FORMAT_GOLD_2:
            format_score = 2
        elif format_ in FORMAT_SILVER_1:
            format_score = 1
        elif format_ in FORMAT_BRONZE_0:
            format_score = 0
        else:
            print(f"Unknown resource format: {resource['format']}", flush=True)
            print("Ceasing execution")
            sys.exit()

        resource_report["format_score"] = format_score

        resource_score += format_score
        resource_report["in_hapi"] = False
        if resource["id"] in HAPI_RESOURCE_IDS:
            resource_report["in_hapi"] = True
            resource_score += 1
        resource_report["is_hxlated"] = False
        if "fs_check_info" in resource.keys():
            check, error_message = get_last_complete_check(resource, "fs_check_info")
            if error_message == "Success":
                if "sheets" in check["hxl_proxy_response"].keys():
                    for sheet in check["hxl_proxy_response"]["sheets"]:
                        if sheet["is_hxlated"]:
                            resource_report["is_hxlated"] = True
                            resource_score += 1
        # elif "shape_info" in resource.keys():
        #     check, error_message = get_last_complete_check(resource, "shape_info")

        # Check for schema changes
        resource_checks = resource_changes[resource["name"]]["checks"]
        n_schema_changes = 0
        if len(resource_checks) != 0:
            resource_report["has_fs_check_or_shape_info"] = True
        else:
            resource_report["has_fs_check_or_shape_info"] = False

        for resource_check in resource_checks:
            if "*" in resource_check and "nrows" not in resource_check:
                n_schema_changes += 1
        if n_schema_changes == 0:
            resource_score += 1
            resource_report["has_stable_schema"] = True
        else:
            resource_report["has_stable_schema"] = False
        report["accessibility"]["resources"].append(resource_report)
        if resource_score > max_resource_score:
            max_resource_score = resource_score
            best_resource_is_hxlated = resource_report["is_hxlated"]
            best_resource_n_schema_changes = n_schema_changes
            best_resource_format_score = format_score

    report["accessibility_score"] = max_resource_score
    if n_tags > 0 and n_countries > 0:
        report["accessibility_score"] += 1

    report["accessibility"]["is_hxlated"] = best_resource_is_hxlated
    report["accessibility"]["format_score"] = best_resource_format_score
    if best_resource_n_schema_changes == 0:
        report["accessibility"]["has_stable_schema"] = True
    else:
        report["accessibility"]["has_stable_schema"] = False
    report["accessibility"]["max_score"] = 2 + 1 + 1 + 1  # format_score + in_hapi + is_hxlated +
    # stable_schema
    return report


def add_interpretability_entries(metadata_dict: dict | None, report: dict) -> dict:
    report["interpretability"] = {}
    report["interpretability"]["has_data_dictionary"] = False
    report["interpretability"]["resources"] = []

    has_data_dictionary = False
    if metadata_dict is not None:
        for resource in metadata_dict["result"]["resources"]:
            resource_report = {}
            resource_report["name"] = resource["name"]
            if resource.get("datastore_active", False):
                resource_report["has_data_dictionary"] = True
                has_data_dictionary = True
            else:
                resource_report["has_data_dictionary"] = False

            if not resource_report["has_data_dictionary"]:
                if "dictionary" in resource["name"].lower() and "data" in resource["name"].lower():
                    resource_report["has_data_dictionary"] = True
                    has_data_dictionary = True
                else:
                    resource_report["has_data_dictionary"] = False

            report["interpretability"]["resources"].append(resource_report)

    report["interpretability"]["has_data_dictionary"] = has_data_dictionary
    report["interpretability_score"] = int(has_data_dictionary)
    report["interpretability"]["max_score"] = 1  # has_data_dictionary

    return report


def add_interoperability_entries(metadata_dict: dict | None, report: dict) -> dict:
    report["interoperability"] = {}
    report["interoperability"]["has_standard_geodenomination"] = False
    report["interoperability"]["resources"] = []

    has_standard_geodenomination = False
    if metadata_dict is not None:
        for resource in metadata_dict["result"]["resources"]:
            resource_report = {}
            resource_report["name"] = resource["name"]
            resource_report["has_standard_geodenomination"] = resource.get("p_coded", False)
            if resource.get("p_coded", False):
                has_standard_geodenomination = True

            #
            schemas = summarise_schema(resource)
            if not has_standard_geodenomination:
                has_geodenomation_hxl = check_schemas(schemas)
                if has_geodenomation_hxl:
                    has_standard_geodenomination = True
                    resource_report["has_standard_geodenomination"] = True
                else:
                    resource_report["has_standard_geodenomination"] = False
            report["interoperability"]["resources"].append(resource_report)

    report["interoperability"]["has_standard_geodenomination"] = has_standard_geodenomination
    report["interoperability_score"] = int(has_standard_geodenomination)
    report["interoperability"]["max_score"] = 1  # has_standard_geodenomination

    return report


def add_findability_entries(metadata_dict: dict | None, report: dict) -> dict:
    report["findability"] = {}
    report["findability"]["has_glide_number"] = False
    report["findability"]["has_gdacs_number"] = False
    report["findability"]["has_doi_number"] = False

    # Check for DOI, GDACS, Glide - methodology, caveats, comments
    # Glide definition - https://glidenumber.net/glide/public/search/search.jsp
    #           example https://data.humdata.org/dataset/turkey-earthquake
    # DOI definition - https://www.doi.org/the-identifier/what-is-a-doi/
    #           example https://data.humdata.org/dataset/social-capital-atlas
    # GDACS definition - https://www.gdacs.org/
    #                   unosat-live-web-map-lewotobi-volcanic-eruption-indonesia

    if metadata_dict is not None:
        # Detecting Glide - Glide: EQ-2023-000015-TUR
        glide_fingerprint = ["glide:"]
        report["findability"]["has_glide_number"] = check_for_uid_fingerprint(
            metadata_dict, glide_fingerprint
        )

        # Detecting GDACS - GDACS ID: 1000099
        gdacs_fingerprint = ["gdacs id:"]
        report["findability"]["has_gdacs_number"] = check_for_uid_fingerprint(
            metadata_dict, gdacs_fingerprint
        )

        # Detecting DOI -  https://doi.org/10.1038/s41586-022-04997-3
        # or https://www.pnas.org/doi/10.1073/pnas.2409418122
        doi_fingerprint = ["doi.org/", "/doi/", "doi:"]
        report["findability"]["has_doi_number"] = check_for_uid_fingerprint(
            metadata_dict, doi_fingerprint
        )

    report["findability_score"] = (
        1
        if any(
            [
                report["findability"]["has_doi_number"],
                report["findability"]["has_glide_number"],
                report["findability"]["has_gdacs_number"],
            ]
        )
        else 0
    )

    report["findability"]["max_score"] = 1  # has a uid
    return report


def check_for_uid_fingerprint(metadata_dict: dict, fingerprints: list[str]) -> bool:
    result = False
    for fingerprint in fingerprints:
        for metadata_key in ["caveats" "methodology_other", "methodology", "notes"]:
            if (
                metadata_key in metadata_dict["result"]
                and fingerprint.lower() in metadata_dict["result"][metadata_key].lower()
            ):
                result = True
                break
        if result:
            break

    return result


# This is summarise_schema borrowed from hdx-stable-schema 2025-07-02
def summarise_schema(resource: dict) -> dict:
    schemas = {}
    error_message = "Neither fs_check_info nor shape_info found"
    if "fs_check_info" in resource.keys():
        check, error_message = get_last_complete_check(resource, "fs_check_info")

        if error_message == "Success":
            if "sheets" in check["hxl_proxy_response"].keys():
                for sheet in check["hxl_proxy_response"]["sheets"]:
                    header_hash = sheet["header_hash"]
                    if header_hash not in schemas:
                        schemas[header_hash] = {}
                        schemas[header_hash]["sheet"] = sheet["name"]
                        schemas[header_hash]["shared_with"] = [resource["name"]]
                        if "headers" in sheet.keys() and sheet["headers"] is not None:
                            schemas[header_hash]["headers"] = sheet["headers"]
                            schemas[header_hash]["data_types"] = [""] * len(sheet["headers"])
                        else:
                            schemas[header_hash]["headers"] = [""]
                            schemas[header_hash]["data_types"] = [""]
                        if "hxl_headers" in sheet.keys():
                            schemas[header_hash]["hxl_headers"] = sheet["hxl_headers"]
                        else:
                            schemas[header_hash]["hxl_headers"] = [""]
                    else:
                        schemas[header_hash]["shared_with"].append(resource["name"])
    elif "shape_info" in resource.keys():
        check, error_message = get_last_complete_check(resource, "shape_info")

        if error_message == "Success":
            headers = [x["field_name"] for x in check["layer_fields"]]
            data_types = [
                SHAPE_INFO_DATA_TYPE_LOOKUP[x["data_type"]] for x in check["layer_fields"]
            ]
            header_hash = hash_row(headers)
            if header_hash not in schemas:
                schemas[header_hash] = {}
                schemas[header_hash]["sheet"] = "__DEFAULT__"
                schemas[header_hash]["shared_with"] = [resource["name"]]
                schemas[header_hash]["headers"] = headers
                schemas[header_hash]["hxl_headers"] = [""] * len(headers)
                schemas[header_hash]["data_types"] = data_types
            else:
                schemas[header_hash]["shared_with"].append(resource["name"])

    return schemas


def check_schemas(schemas: dict) -> bool:
    has_geodenomination_hxl = False
    # https://hxlstandard.org/standard/1-1final/dictionary/#geo
    for schema_hash in schemas.keys():
        if schemas[schema_hash]["hxl_headers"] is not None:
            for hxl_tag in schemas[schema_hash]["hxl_headers"]:
                if hxl_tag.replace(" ", "").lower() in GEODENOMINATION_HXL:
                    has_geodenomination_hxl = True
                    break

    return has_geodenomination_hxl


def check_for_hapi(metadata_dict: dict) -> str | bool:
    in_hapi_input = False

    n_resources = len(metadata_dict["result"]["resources"])
    n_in_hapi = 0
    for resource in metadata_dict["result"]["resources"]:
        if resource["id"] in HAPI_RESOURCE_IDS:
            n_in_hapi += 1

    if n_in_hapi != 0:
        in_hapi_input = f"{n_in_hapi} of {n_resources}"

    return in_hapi_input


def check_for_datagrid(metadata_dict: dict) -> str | bool:
    in_datagrid = False
    datagrid_filepath = Path(__file__).parent / "data" / "datagrid-datasets.csv"
    with open(datagrid_filepath, newline="", encoding="utf-8") as csvfile:
        dataset_name_rows = csv.DictReader(csvfile)
        datagrid_datasets = [x["dataset_name"] for x in dataset_name_rows]

    if metadata_dict["result"]["name"] in datagrid_datasets:
        in_datagrid = True

    return in_datagrid


def check_for_signals(metadata_dict: dict) -> str | bool:
    in_signals = False

    # This list is derived from (on 2025-05-11) - needs checking regularly:
    # https://github.com/OCHA-DAP/hdx-ckan/blob/
    # e42f76e9ea204bbe4ec43c10088a756bb5ae8001/
    # ckanext-hdx_theme/ckanext/hdx_theme/helpers/ui_constants/landing_pages/signals.py#L40

    dataset_name = metadata_dict["result"]["name"]
    organization_name = metadata_dict["result"]["organization"]["name"]

    if dataset_name in SIGNALS_DATASETS:
        in_signals = True

    if organization_name in SIGNALS_ORGANIZATIONS:
        in_signals = True

    return in_signals


def check_for_crisis(metadata_dict) -> list[str] | bool:
    in_crisis = False

    for tag in metadata_dict["result"]["tags"]:
        if tag["name"].startswith("crisis-"):
            if not in_crisis:
                in_crisis = [tag["name"]]
            else:
                in_crisis.append(tag["name"])

    return in_crisis


def lucky_dip_search(hdx_site: str | None = "stage"):
    if hdx_site is None:
        hdx_site = "stage"
    hdx_site_url, hdx_api_key, _ = get_hdx_url_and_key(hdx_site=hdx_site)
    package_search_url = f"{hdx_site_url}/api/action/package_search"
    query = {"fq": "*:*", "start": 0, "rows": 1}
    response = fetch_data_from_ckan_package_search(
        package_search_url, query, hdx_api_key=hdx_api_key, fetch_all=False
    )

    n_datasets = response["result"]["count"]

    # Now do a second query with a random start,
    # using the first to get the range of offsets possible
    # Make a random offset in the range 0, n datasets
    random_start = randrange(0, n_datasets)
    random_offset_params = {
        "fq": "*:*",
        "start": random_start,
        "rows": 1,
    }
    response = fetch_data_from_ckan_package_search(
        package_search_url, random_offset_params, hdx_api_key=hdx_api_key, fetch_all=False
    )

    dataset_name = response["result"]["results"][0]["name"]

    metadata_dict = read_metadata_from_hdx(dataset_name=dataset_name)

    return metadata_dict

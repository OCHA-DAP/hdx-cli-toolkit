#!/usr/bin/env python
# encoding: utf-8

# These functions borrowed from https://github.com/OCHA-DAP/hdx-hapi-scheduled-tasks 2025-05-12

import urllib3


def get_hapi_resource_ids(hapi_site: str) -> set:
    # print("Creating app identifier", flush=True)
    theme = "metadata/resource"
    hapi_app_identifier = get_app_identifier(
        "hapi",
        email_address="hello@deva-data.co.uk",
        app_name="HDXINTERNAL_hdx_cli_toolkit",
    )
    # print("Fetching data from HAPI resources endpoint", flush=True)
    query_url = (
        f"https://{hapi_site}.humdata.org/api/v1/{theme}?"
        f"output_format=json"
        f"&app_identifier={hapi_app_identifier}"
    )

    hapi_results = fetch_data_from_hapi(query_url, limit=1000)

    hapi_resource_ids = {x["resource_hdx_id"] for x in hapi_results}
    # print(f"Found {len(hapi_results)} resources in HAPI", flush=True)
    return hapi_resource_ids


def get_app_identifier(
    hapi_site: str,
    email_address: str = "ian.hopkinson%40humdata.org",
    app_name="HDXINTERNAL_hapi_scheduled",
) -> str:
    app_identifier_url = (
        f"https://{hapi_site}.humdata.org/api/v1/"
        f"encode_app_identifier?application={app_name}&email={email_address}"
    )
    app_identifier_response = fetch_data_from_hapi(app_identifier_url)

    app_identifier = app_identifier_response["encoded_app_identifier"]
    return app_identifier


def fetch_data_from_hapi(query_url: str, limit: int = 1000) -> dict | list[dict]:
    """
    Fetch data from the provided query_url with pagination support.

    Args:
    - query_url (str): The query URL to fetch data from.
    - limit (int): The number of records to fetch per request.

    Returns:
    - list: A list of fetched results.
    """

    if "encode_app_identifier" in query_url:
        json_response = urllib3.request("GET", query_url).json()

        return json_response

    idx = 0
    results = []

    while True:
        offset = idx * limit
        url = f"{query_url}&offset={offset}&limit={limit}"

        # print(f"Getting results {offset} to {offset+limit-1}", flush=True)

        response = urllib3.request("GET", url)
        json_response = response.json()

        results.extend(json_response["data"])
        # If the returned results are less than the limit,
        # it's the last page
        if len(json_response["data"]) < limit:
            break
        idx += 1

    return results

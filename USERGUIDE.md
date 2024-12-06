# User Guide

## Overview

This toolkit provides a commandline interface to the [Humanitarian Data Exchange](https://data.humdata.org/) (HDX) to allow for bulk modification operations and other administrative activities such as getting `id` values for users and organization. It is useful for those managing HDX and developers building data pipelines for HDX. The currently supported commands are as follows:

```
  configuration              Print configuration information to terminal
  download                   Download dataset resources from HDX
  get_organization_metadata  Get an organization id and other metadata
  get_user_metadata          Get user id and other metadata
  list                       List datasets in HDX
  print                      Print datasets in HDX to the terminal
  quickcharts                Upload QuickChart JSON description to HDX
  remove_extras_key          Remove extras key from a dataset
  scan                       Scan all of HDX and perform an action
  showcase                   Upload showcase to HDX
  update                     Update datasets in HDX
  update_resource            Update a resource in HDX
```

## Installation (from README.md)
`hdx-cli-toolkit` is a Python application published to the PyPI package repository, therefore it can be installed easily with:

```pip install hdx_cli_toolkit```

Users may prefer to make a global, isolated installation using [pipx](https://pypi.org/project/pipx/) which will make the `hdx-toolkit` commands available across their projects:

```pipx install hdx_cli_toolkit```

`hdx-cli-toolkit` uses the `hdx-python-api` library, this requires the following to be added to a file called `.hdx_configuration.yaml` in the user's home directory.

```
hdx_key_stage: "[hdx_key from the staging HDX site]"
hdx_key: "[hdx_key from the prod HDX site]"
```

A user agent (`hdx_cli_toolkit_*`) is specified in the `~/.useragents.yaml` file with the * replaced with the users initials.
```
hdx-cli-toolkit:
    preprefix: [YOUR_ORGANIZATION]
    user_agent: hdx_cli_toolkit_ih
```

## Getting Help

Once installed we can get help for the commands available in the `hdx-toolkit` using:

```
hdx-toolkit --help
```

Or for a specific command:
```
hdx-toolkit list --help
```

## HDX Configuration

Understanding the `Configuration` used by `hdx-python-api` can be challenging for new users, so the `configuration` command will echo the relevant local values (censoring any secrets):

```
hdx-toolkit configuration
```

The `configuration` command can also be used to list the approved dataset tags using:

`hdx-toolkit configuration --approved_tag_list`

This produces an output containing only the tags with no boilerplate, it can be piped into a file
or `grep` to find particular tags.

The `configuration` command will check the `stage` and `prod` API keys it holds are valid. 

## List and Update

The `list` and `update` commands are designed to be used together, using `list` to check what a potentially destructive `update` will do, and then simply repeating the same commandline with `list` replaced with `update`. This commandline selects a single dataset, `mali-healthsites`:

```shell
hdx-toolkit list --organization=healthsites --dataset_filter=mali-healthsites --hdx_site=stage --key=private --value=True
```

The command to `update` these datasets with the supplied `--value` is simply: 

```shell
hdx-toolkit update --organization=healthsites --dataset_filter=mali-healthsites --hdx_site=stage --key=private --value=True
```

For this action an organization is required unless an exact dataset name is supplied.

The `list` and `update` commands take wildcard arguments, i.e.:

```shell
hdx-toolkit list --organization=healthsites --dataset_filter=*la* --hdx_site=stage --key=private --value=True
```

which selects 29 datasets matching the filter `*la*`, or
```shell
hdx-toolkit list --organization=healthsites --dataset_filter=* --hdx_site=stage --key=private --value=True
```
which selects all the datasets of an organization. Note that the filters acts on dataset names (used in URL), not the titles (shown in the HDX dataset page). The `update` command can provide an output file listing the changes made which can subsequently be used in an `undo` operation:
```shell
hdx-toolkit update --organization=healthsites --dataset_filter=somalia-healthsites --hdx_site=stage --key=caveats --value="test entry" --output_path=2024-04-29-undo-test.csv
```
The generated output file, `2024-04-29-undo-test.csv` looks like this:
```csv
dataset_name,key,old_value,new_value,message
somalia-healthsites,caveats,Read the Healthsites concept note http://bit.ly/2ocL2KY,test entry,9.27
```

The `undo` operation is then executed with:
```shell
hdx-toolkit update --organization=healthsites --dataset_filter=somalia-healthsites --hdx_site=stage --key=caveats --value="test entry" --from_path=2024-04-29-undo-test.csv --undo
```
The `from_path` argument indicates the update is to be done from a file, `undo` indicates that the value to be updated is in the `old_value` column. The `from_path` operation can be done without the `undo` flag in which case the file indicated needs to contain, at least, a `dataset_name` column, a `key` column and a `new_value` column.

The `list` command can output multiple comma separated keys to a table, and also to a CSV file specified using the `--output_path` keyword. 

```shell
hdx-toolkit list --organization=international-organization-for-migration --key=data_update_frequency,dataset_date --output_path=2024-02-05-iom-dtm.csv
```

`list` can also output the value of nested keys such as `organization.name` or lists of values such as `tags.name` or `groups.name`. If the `--with_extras` flag is applied then keys within `resources`, `resource_views` and `showcases` can also be seen. The `--with_extras` flag forces multiple queries to HDX per dataset and can be slow, therefore it should only be used if necessary and only for small numbers of datasets at a time. An example of this is as follows:

```shell
hdx-toolkit list --organization=healthsites --dataset_filter=gibraltar-healthsites --hdx_site=stage --key=resources.name --value=True --with_extras
```

Functionality to access multiple keys and nested keys is not available to the `update` command.

If the `query` keyword is supplied then `organization` and `dataset_filter` keywords are ignored and the `query` is passed to CKAN:

```shell
hdx-toolkit list --query=archived:true --key=owner_org
```
There is a guide to the CKAN query language [here](https://github.com/OCHA-DAP/hdx-ckan/blob/dev/ckanext-hdx_theme/docs/search/package_search.rst).

## Organization and User metadata

Another pain point for me is getting an organization id, the `get_organization_metadata` command fixes this. We can just get the id with an organization name, note wildcards are implicit in the organization specification since this is how the CKAN API works:

```shell
hdx-toolkit get_organization_metadata --organization=zurich
```

We can get the full organization record using the `--verbose` flag:

```shell
hdx-toolkit get_organization_metadata --organization=eth-zurich-weather-and-climate-risks --verbose
```

Similarly we can get user ids:

```shell
hdx-toolkit get_user_metadata --user=hopkinson
```

And see the complete records:

```shell
hdx-toolkit get_user_metadata --user=hopkinson --verbose
```

Note I first joined HDX in March 2015!

Finally, you can print the metadata for a dataset:

```shell
hdx-toolkit print --dataset_filter=climada-litpop-dataset
```

This output is valid JSON and can be piped into a file to use as a test fixture or template.

It is possible to include resource, showcase and QuickChart (resource_view) metadata into the `print` view using the `--with_extras` flag:

```shell
hdx-toolkit print --dataset_filter=wfp-food-prices-for-nigeria --with_extras
```

This adds resources under a `resources` key which includes a `quickcharts` key and showcases under a `showcases` key. These new keys mean that the output JSON cannot be created directly in HDX. The `fs_check_info`, `shape_info` and `hxl_preview_config` keys which previously contained a JSON object serialised as a single string are expanded as dictionaries so that they are printed out in an easy to read format.

## Quick Charts

A Quick Chart can be uploaded from a JSON file using a commandline like where the `dataset_filter` specifies a single dataset and the `resource_name` specifies the resource to which the Quick Chart is attached:

```
 hdx-toolkit quickcharts --dataset_filter=climada-flood-dataset --hdx_site=stage --resource_name=admin1-summaries-flood.csv --hdx_hxl_preview_file_path=quickchart-flood.json
```

The `hdx_hxl_preview_file_path` points to a JSON format file with the key `hxl_preview_config` which contains the Quick Chart definition. This file is converted to a single string via a temporary yaml file so should be easily readable. Quick Chart recipe documentation can be found [here](https://github.com/OCHA-DAP/hxl-recipes?tab=readme-ov-file). There is an example file in the `hdx-cli-toolkit` [repo](https://github.com/OCHA-DAP/hdx-cli-toolkit/blob/main/tests/fixtures/quickchart-flood.json).

## Showcases

A showcase can be uploaded from attributes found in either a CSV format file like this:
```
dataset_name,timestamp,attribute,value,secondary_value
climada-litpop-showcase,2024-02-21T08:11:10.725670,entity_type,"showcase",
climada-litpop-showcase,2024-02-21T08:11:10.725670,name,"climada-litpop-showcase",
climada-litpop-showcase,2024-02-21T08:11:10.725670,parent_dataset,"climada-litpop-dataset",
climada-litpop-showcase,2024-02-21T08:11:10.725670,title,"CLIMADA LitPop Methodology Documentation",
climada-litpop-showcase,2024-02-21T08:11:10.725670,notes,"Click the image to go to the original source for this data",
climada-litpop-showcase,2024-02-21T08:11:10.725670,url,https://climada-python.readthedocs.io/en/stable/tutorial/climada_entity_LitPop.html,
climada-litpop-showcase,2024-02-21T08:11:10.725670,image_url,https://github.com/OCHA-DAP/hdx-scraper-climada/blob/main/src/hdx_scraper_climada/output/litpop/litpop-haiti-showcase.png,
climada-litpop-showcase,2024-02-21T08:11:10.725670,tags,"economics",
climada-litpop-showcase,2024-02-21T08:11:10.725670,tags,"gross domestic product-gdp",
climada-litpop-showcase,2024-02-21T08:11:10.725670,tags,"population",
```

or a JSON file:

```
{
    "entity_type": "showcase",
    "name": "climada-litpop-showcase",
    "parent_dataset": "climada-litpop-dataset",
    "title": "CLIMADA LitPop Methodology Documentation",
    "notes": "Click the image to go to the original source for this data",
    "url": "https://climada-python.readthedocs.io/en/stable/tutorial/climada_entity_LitPop.html",
    "image_url": "https://github.com/OCHA-DAP/hdx-scraper-climada/blob/main/src/hdx_scraper_climada/output/litpop/litpop-haiti-showcase.png",
    "tags": [
        "economics",
        "gross domestic product-gdp",
        "population"
    ]
}
```

Using a commandline like:
```
hdx-toolkit showcase --showcase_name=climada-litpop-showcase --hdx_site=stage --attributes_file_path=attributes.csv
```

An individual resource can be updated with a commandline like:
```
hdx-toolkit update_resource --dataset_name=hdx_cli_toolkit_test --resource_name="test_resource_1" --hdx_site=stage --resource_file_path=test-2.csv --live
```

Without the `--live` flag no update on HDX is made.

## Downloading Data
The resources of a dataset can be downloaded with a commandline like:

```shell
hdx-toolkit download --dataset=bangladesh-bgd-attacks-on-protection --resource_filter=* --hdx_site=stage
```
by default files are downloaded to a subdirectory `output` with no download if a file already exists.

## Scan
The `scan` command takes the dataset and resource information returned by the CKAN `package_search` endpoint for all the datasets in HDX and then applies an action to them. The downloaded information can be cached and reloaded from a specified JSON file. This is useful because the full catalogue is approximately 865MB and takes 10 minutes to download.

The supported actions are:
1. `survey` - count the number of occurrences of a key or list of keys across
  datasets in HDX
2. `distribution` - calculate the histogram of values for a key across
  datasets in HDX
3. `delete_key` - delete occurrences of a key across all datasets in HDX, this
  is currently configured so that it only accepts "extras" and
  "resource._csrf_token" as valid keys to delete
4. `list` - replicates the list command, providing a table of datasets with values
  of selected keys

Examples of invocations of the scan command are as follows:
```
hdx-toolkit scan --hdx_site="stage" --action=survey --key=resources._csrf_token output_path=output/2024-08-25-hdx-snapshot.json --verbose
hdx-toolkit scan --hdx_site="stage" --action=distribution --key=data_update_frequency
hdx-toolkit scan --hdx_site="stage" --input_path=output/2024-08-24-hdx-snapshot.json --action=delete_key --key=extras --verbose
hdx-toolkit scan --hdx_site="stage" --action=list --key=organization.name,data_update_frequency --rows=100
```
## Miscellaneous

There is an issue with some datasets where a key, `extras` is found which is not valid, it prevents
the dataset being updated. The `extras` key be removed from a set of datasets with the `remove_extras_key` command:

```
hdx-toolkit remove_extras_key --organization=healthsites --dataset_filter=*al*-healthsites --hdx_site=stage --output_path=temp.csv
```


## Future Work

Potential new features can be found in the [GitHub issue tracker](https://github.com/OCHA-DAP/hdx-cli-toolkit/issues)

## Collected commands
 
```
hdx-toolkit --help
hdx-toolkit list --help
hdx-toolkit configuration
hdx-toolkit configuration --approved_tag_list
hdx-toolkit list --organization=healthsites --dataset_filter=*al*-healthsites --hdx_site=stage --key=private --value=True --output_path=2024-04-24-update-details.csv
hdx-toolkit list --organization=healthsites --dataset_filter=*al*-healthsites --hdx_site=stage --key=organization.name --value=True
 hdx-toolkit list --organization=healthsites --dataset_filter=gibraltar-healthsites --hdx_site=stage --key=resources.name --value=True --with_extras
hdx-toolkit list --organization=international-organization-for-migration --key=data_update_frequency,dataset_date --output_path=2024-02-05-iom-dtm.csv
hdx-toolkit list --query=archived:true --key=owner_org --output_path=2024-02-08-archived-datasets.csv
 hdx-toolkit list --query="cod_level:(cod-standard and cod-enhanced) +dataseries_name:COD\ -\ Subnational\ Population\ Statistics" --key=title,organization.name,dataset_date --output_path=2024-08-15-COD-export.csv
hdx-toolkit get_organization_metadata --organization=zurich
hdx-toolkit get_organization_metadata --organization=eth-zurich-weather-and-climate-risks --verbose
hdx-toolkit get_user_metadata --user=hopkinson
hdx-toolkit get_user_metadata --user=hopkinson --verbose
hdx-toolkit print --dataset_filter=climada-litpop-dataset
hdx-toolkit print --dataset_filter=wfp-food-prices-for-nigeria --with_extras
hdx-toolkit print --dataset_filter=geoboundaries-admin-boundaries-for-nepal --with_extras
hdx-toolkit quickcharts --dataset_filter=climada-flood-dataset --hdx_site=stage --resource_name=admin1-summaries-flood.csv --hdx_hxl_preview_file_path=quickchart-flood.json
hdx-toolkit showcase --showcase_name=climada-litpop-showcase --hdx_site=stage --attributes_file_path=attributes.csv
hdx-toolkit update_resource --dataset_name=hdx_cli_toolkit_test --resource_name="test_resource_1" --hdx_site=stage --resource_file_path=test-2.csv --live
hdx-toolkit download --dataset=bangladesh-bgd-attacks-on-protection --hdx_site=stage
hdx-toolkit remove_extras_key --organization=healthsites --dataset_filter=*al*-healthsites --hdx_site=stage --output_path=temp.csv
hdx-toolkit scan --hdx_site="stage" --action=survey --key=resources._csrf_token output_path=output/2024-08-25-hdx-snapshot.json --verbose
hdx-toolkit scan --hdx_site="stage" --action=distribution --key=data_update_frequency
hdx-toolkit scan --hdx_site="stage" --input_path=output/2024-08-24-hdx-snapshot.json --action=delete_key --key=extras --verbose
hdx-toolkit scan --hdx_site="stage" --action=list --key=organization.name,data_update_frequency --rows=100
hdx-toolkit scan --hdx_site="stage" --action=list --key=data_update_frequency --input_path=output/2024-08-24-hdx-snapshot.json --result_path=output/2024-09-03-scan-results.csv
```

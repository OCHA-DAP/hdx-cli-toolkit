# Demo Script

## Motivations

1. Request for DPT to do a bulk quarantine action
2. Requirement to grab various pieces of HDX data as text for developing pipelines (organization, maintainer ids, datasets as JSON, lists of datasets for organizations...)
3. One stop shop for "how do I do this?" - GitHub Actions, Pytest fixtures, mocks, Click CLI.

## Walkthrough

Installed using Python `pip` command, requires some config files and environment variables as for `hdx-python-api`.

We can get the help for the library with

```
hdx-toolkit --help
```

Or for a specific command:
```
hdx-toolkit list --help
```

Understanding the `Configuration` used by `hdx-python-api` can be challenging for new users, so the `configuration` command will echo the relevant local values (censoring any secrets):

```
hdx-toolkit configuration
```

The `configuration` command can also be used to list the approved dataset tags using:

`hdx-toolkit configuration --approved_tag_list`

This produces an output containing only the tags with no boilerplate, it can be piped into a file
or `grep` to find particular tags.

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
hdx-toolkit list --organization=healthsites--dataset_filter=* --hdx_site=stage --key=private --value=True
```
which selects all the datasets of an organization.


The `list` command can output multiple comma separated keys to a table, and also to a CSV file specified using the `--output_path` keyword.

```shell
hdx-toolkit list --organization=international-organization-for-migration --key=data_update_frequency,dataset_date --output_path=2024-02-05-iom-dtm.csv
```

If the `query` keyword is supplied then `organization` and `dataset_filter` keywords are ignored and the `query` is passed to CKAN:

```shell
hdx-toolkit list --query=archived:true --key=owner_org
```

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

This adds resources under a `resources` key which includes a `quickcharts` key and showcases under a `showcases` key. These new keys mean that the output JSON cannot be created directly in HDX. The `fs_check_info` and `hxl_preview_config` keys which previously contained a JSON object serialised as a single string are expanded as dictionaries so that they are printed out in an easy to read format.

A Quick Chart can be uploaded from a JSON file using a commandline like where the `dataset_filter` specifies a single dataset and the `resource_name` specifies the resource to which the Quick Chart is attached:

```
 hdx-toolkit quickcharts --dataset_filter=climada-flood-dataset --hdx_site=stage --resource_name=admin1-summaries-flood.csv --hdx_hxl_preview_file_path=quickchart-flood.json
```

The `hdx_hxl_preview_file_path` points to a JSON format file with the key `hxl_preview_config` which contains the Quick Chart definition. This file is converted to a single string via a temporary yaml file so should be easily readable. Quick Chart recipe documentation can be found [here](https://github.com/OCHA-DAP/hxl-recipes?tab=readme-ov-file). There is an example file in the `hdx-cli-toolkit` [repo](https://github.com/OCHA-DAP/hdx-cli-toolkit/blob/main/tests/fixtures/quickchart-flood.json).

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

The resources of a dataset can be downloaded with a commandline like:

```shell
hdx-toolkit download --dataset=bangladesh-bgd-attacks-on-protection --resource_filter=* --hdx_site=stage
```
by default files are downloaded to a subdirectory `output` with no download if a file already exists.

## Future Work

Potential new features can be found in the [GitHub issue tracker](https://github.com/OCHA-DAP/hdx-cli-toolkit/issues)

## Collected commands
 
```
hdx-toolkit --help
hdx-toolkit list --help
hdx-toolkit configuration
hdx-toolkit configuration --approved_tag_list
hdx-toolkit list --organization=healthsites --dataset_filter=*al*-healthsites --hdx_site=stage --key=private --value=True
hdx-toolkit list --organization=international-organization-for-migration --key=data_update_frequency,dataset_date --output_path=2024-02-05-iom-dtm.csv
hdx-toolkit list --query=archived:true --key=owner_org --output_path=2024-02-08-archived-datasets.csv
hdx-toolkit get_organization_metadata --organization=zurich
hdx-toolkit get_organization_metadata --organization=eth-zurich-weather-and-climate-risks --verbose
hdx-toolkit get_user_metadata --user=hopkinson
hdx-toolkit get_user_metadata --user=hopkinson --verbose
hdx-toolkit print --dataset_filter=climada-litpop-dataset
hdx-toolkit print --dataset_filter=wfp-food-prices-for-nigeria --with_extras
hdx-toolkit quickcharts --dataset_filter=climada-flood-dataset --hdx_site=stage --resource_name=admin1-summaries-flood.csv --hdx_hxl_preview_file_path=quickchart-flood.json
hdx-toolkit showcase --showcase_name=climada-litpop-showcase --hdx_site=stage --attributes_file_path=attributes.csv
hdx-toolkit update_resource --dataset_name=hdx_cli_toolkit_test --resource_name="test_resource_1" --hdx_site=stage --resource_file_path=test-2.csv --live
 hdx-toolkit download --dataset=bangladesh-bgd-attacks-on-protection --hdx_site=stage
```
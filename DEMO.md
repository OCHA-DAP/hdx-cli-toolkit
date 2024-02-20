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

The `list` and `update` commands are designed to be used together, using `list` to check what a potentially destructive `update` will do, and then simply repeating the same commandline with `list` replaced with `update`:

```
hdx-toolkit list --organization=healthsites --dataset_filter=mali-healthsites --hdx_site=stage --key=private --value=True
```

For this action an organization is required unless an exact dataset name is supplied.

The `list` command can output multiple comma separated keys to a table, and also to a CSV file specified using the `--output_path` keyword.

```
hdx-toolkit list --organization=international-organization-for-migration --key=data_update_frequency,dataset_date --output_path=2024-02-05-iom-dtm.csv
```

If the `query` keyword is supplied then `organization` and `dataset_filter` keywords are ignored and the `query` is passed to CKAN:

```
hdx-toolkit list --query=archived:true --key=owner_org
```

Another pain point for me is getting an organization id, the `get_organization_metadata` command fixes this. We can just get the id with an organization name, note wildcards are implicit in the organization specification since this is how the CKAN API works:

```
hdx-toolkit get_organization_metadata --organization=zurich
```

We can get the full organization record using the `--verbose` flag:

```
hdx-toolkit get_organization_metadata --organization=eth-zurich-weather-and-climate-risks --verbose
```

Similarly we can get user ids:

```
hdx-toolkit get_user_metadata --user=hopkinson
```

And see the complete records:

```
hdx-toolkit get_user_metadata --user=hopkinson --verbose
```

Note I first joined HDX in March 2015!

Finally, you can print the metadata for a dataset:

```
hdx-toolkit print --dataset_filter=climada-litpop-dataset
```

This output is valid JSON and can be piped into a file to use as a test fixture or template.

It is possible to include resource, showcase and QuickChart (resource_view) metadata into the `print` view using the `--with_extras` flag:

```
hdx-toolkit print --dataset_filter=wfp-food-prices-for-nigeria --with_extras
```

This adds resources under a `resources` key which includes a `quickcharts` key and showcases under a `showcases` key. These new keys mean that the output JSON cannot be created directly in HDX. The `fs_check_info` and `hxl_preview_config` keys which previously contained a JSON object serialised as a single string are expanded as dictionaries so that they are printed out in an easy to read format.

A Quick Chart can be uploaded from a JSON file using a commandline like where the `dataset_filter`
specifies a single dataset and the `resource_name specifies` the resource to which the Quick Chart is attached:

```
 hdx-toolkit quickcharts --dataset_filter=climada-flood-dataset --hdx_site=stage --resource_name=admin1-summaries-flood.csv --hdx_hxl_preview_file_path=quickchart-flood.json
```

The hdx_hxl_preview_file_path points to a JSON format file with the key `hxl_preview_config` which
contains the Quick Chart definition. This file is converted to a single string via a temporary yaml file so should be easily readable. Example Quick Chart recipes can be found [here](https://github.com/OCHA-DAP/hxl-recipes?tab=readme-ov-file)

## Future Work

Potential new features can be found in the [GitHub issue tracker](https://github.com/OCHA-DAP/hdx-cli-toolkit/issues)

## Collected commands
 
```
hdx-toolkit --help
hdx-toolkit list --help
hdx-toolkit configuration
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
```
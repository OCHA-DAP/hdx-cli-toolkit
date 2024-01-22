# Demo Script

## Motivations

1. Request for DPT to do a bulk quarantine action
2. Requirement to grab various pieces of HDX data as text for developing pipelines (organisation, maintainer ids, datasets as JSON, lists of datasets for organisations...)
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

The `list` and `update` commands are designed to be used together, using `list` to check what a potentially destructive `update` will do, and then simply repeating the same commandline with `list` replaced with `update`:

```
hdx-toolkit list --organisation=healthsites --dataset_filter=mali-healthsites --hdx_site=stage --key=private --value=True
```

For this action an organisation is required unless an exact dataset name is supplied.

Another pain point for me is getting an organisation id, the `get_organisation_metadata` fixes
this. We can just get the id with an organisation name, note wildcards are implicit in the organisation specification since this is how the CKAN API works:

```
hdx-toolkit get_organisation_metadata --organisation=zurich
```

We can get the full organisation record using the `--verbose` flag:

```
hdx-toolkit get_organisation_metadata --organisation=eth-zurich-weather-and-climate-risks --verbose
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

## Future Work

Add support for listing resources to a dataset
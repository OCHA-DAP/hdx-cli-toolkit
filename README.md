# HDX CLI Toolkit

## Overview

This toolkit provides a commandline interface to the [Humanitarian Data Exchange](https://data.humdata.org/) (HDX) to allow for bulk modification operations and other administrative activities such as getting `id` values for users and organization. It is useful for those managing HDX and developers building data pipelines for HDX. The currently supported commands are as follows:

```
  configuration              Print configuration information to terminal
  data_quality_report        Compile a data quality report
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

In the most part it is a thin wrapper to the [hdx-python-api](https://github.com/OCHA-DAP/hdx-python-api) library written by Mike Rans.

The library requires some configuration, described below, to authenticate to the HDX instance.

## Installation
`hdx-cli-toolkit` is a Python application published to the PyPI package repository, therefore it can be installed easily with:

```pip install hdx_cli_toolkit```

Users may prefer to make a global, isolated installation using [pipx](https://pypi.org/project/pipx/) which will make the `hdx-toolkit` commands available across their projects:

```pipx install hdx_cli_toolkit```

`hdx-toolkit` can then be updated with:

```pipx install --force hdx_cli_toolkit```

`hdx-cli-toolkit` uses the `hdx-python-api` library, this requires the following to be added to a file called `.hdx_configuration.yaml` in the user's home directory.

```
hdx_key_stage: "[an HDX API token from the staging HDX site]"
hdx_key: "[an HDX API token from the prod HDX site]"
default_organization: "[your organization]"
```

The `default_organization` is required for the `configuration` command and can be supplied using the `--organization=` commandline parament. If not defined it will default to `hdx`. 

A user agent (`hdx_cli_toolkit_*`) is specified in the `~/.useragents.yaml` file with the * replaced with the users initials.
```
hdx-cli-toolkit:
    preprefix: [YOUR_ORGANIZATION]
    user_agent: hdx_cli_toolkit_ih
```

## Usage

The `hdx-toolkit` is built using the Python `click` library. Details of the currently implemented commands can be revealed by running `hdx-toolkit --help`, and details of the arguments for a command can be found using `hdx-toolkit [COMMAND] --help`

A detailed guide can be found in the [USERGUIDE.md](https://github.com/OCHA-DAP/hdx-cli-toolkit/blob/main/USERGUIDE.md) file

## Maintenance 

For the `data_quality_report` data on membership of Data Grid and HDX Signals is hard-coded. To update the coding for datasets in Data Grids:

1. clone this repo: https://github.com/OCHA-DAP/data-grid-recipes
2. Run the script `scripts/data_grid_recipes_compiler.py` script in the root of the data-grid-recipes repo
3. copy the output datagrid-datasets.csv to `src/hdx_cli_toolkit/data`

To update the coding for datasets in HDX Signals, check the code at this path in the `hdx-ckan` repo:
`hdx-ckan/ckanext-hdx_theme/ckanext/hdx_theme/helpers/ui_constants/landing_pages/signals.py`

for the `DATA_COVERAGE_CONSTANTS` note that the `links` in this constant are sometimes to datasets and sometimes to organizations. Copy the dataset and organization names to the `SIGNALS_DATASETS` and `SIGNALS_ORGANIZATIONS` constants in `src\hdx_cli_toolkit\data_quality_utilities.py` in this repo.

These are ugly methods for identifying Data Grid and HDX Signals datasets but with the current HDX implementation they are the most straightforward.

## Contributions

For developers the code should be cloned installed from the [GitHub repo](https://github.com/OCHA-DAP/hdx-cli-toolkit), and a virtual enviroment created:

```shell
python -m venv venv
source venv/Scripts/activate
```

And then an editable installation created:

```shell
pip install -e .
```

The library is then configured, as described above.

This project uses a GitHub Action to run tests and linting. It requires the following environment variables/secrets to be set in the `test` environment:

```
HDX_KEY - secret. Value: fake secret
HDX_KEY_STAGE - secret. Value: a live API token for the stage server
HDX_SITE - environment variable. Value: stage
USER_AGENT - environment variable. Value: hdx_cli_toolkit_gha
PREPREFIX - - environment variable. Value: [YOUR_organization]
```

Most tests use mocking in place of HDX, although the `test_integration.py` suite runs against the `stage` server.

New features should be developed against a GitHub issue on a separate branch with a name starting `GH[issue number]_`. `PULL_REQUEST_TEMPLATE.md` should be used in preparing pull requests. Versioning is updated manually in `pyproject.toml` and is described in the template, in brief it is CalVer `YYYY.MM.Micro`.

## Publication

Publication to PyPI is done automatically when a release is created.

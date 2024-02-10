# HDX CLI Toolkit

## Overview

This toolkit is intended to provide a commandline interface to HDX to allow for bulk modification operations and other administrative activities, in the first instance to carry out a bulk quarantine action on all the datasets in an organisation. It is inspired by [hdx-update-cods-level](https://github.com/b-j-mills/hdx-update-cods-level/tree/main).

## Installation
`hdx-cli-toolkit` is a Python application. 

For developers the code should be cloned installed from the [GitHub repo](https://github.com/OCHA-DAP/hdx-cli-toolkit), and a virtual enviroment created:

```shell
python -m venv venv
source venv/Scripts/activate
```

And then an editable installation created:

```shell
pip install -e .
```

For users the best route is probably to use [pipx](https://pypi.org/project/pipx/) to install which will provide `hdx-toolkit` globally in its own environment.

In either case there is a small amount of configuration required.

`hdx-cli-toolkit` uses the `hdx-python-api` library, configuration for which is done in the usual way [described here](https://hdx-python-api.readthedocs.io/en/latest/). 

The user agent (`hdx_cli_toolkit_ih`) is specified in the `~/.useragents.yaml` file the suffix _ih should be replaced with the users initials.
```
hdx-cli-toolkit:
    preprefix: [YOUR_ORGANISATION]
    user_agent: hdx_cli_toolkit_ih
```


## Usage

The `hdx-toolkit` is built using the Python `click` library. Details of the currently implemented commands can be revealed by running:

```
$ hdx-toolkit --help
Usage: hdx-toolkit [OPTIONS] COMMAND [ARGS]...

  Tools for Commandline interactions with HDX

Options:
  --help  Show this message and exit.

Commands:
  get_organisation_metadata  Get an organisation id and other metadata
  get_user_metadata          Get user id and other metadata
  list                       List datasets in HDX
  print                      Print datasets in HDX to the terminal
  update                     Update datasets in HDX
```

The output from the `print` command is designed to be piped to file to make a valid JSON fixture.

And details of the arguments for a command can be found using:

```
hdx-toolkit [COMMAND] --help
```

`update` is clearly an operation with potential negative side-effects. Commands can be tested on the HDX `stage` site by setting `--hdx_site=stage`. In addition the `list` command can be used to check the datasets to be affected since `list` and `update` both take the same arguments and use the same filtering function although for `list` the `--value` argument is ignored:

The original purpose of the `hdx-cli-toolkit` was to quarantine the Healthsites datasets, for which the process was a cautious single dataset update
```shell
hdx-toolkit list --organisation=healthsites --dataset_filter=mali-healthsites --hdx_site=stage --key=private --value=True
hdx-toolkit update --organisation=healthsites --dataset_filter=mali-healthsites --hdx_site=stage --key=private --value=True
```

A slightly more adventurous update that selects 29 datasets using the `*la*` wildcard:

```shell
hdx-toolkit list --organisation=healthsites --dataset_filter=*la* --hdx_site=stage --key=private --value=True
hdx-toolkit update --organisation=healthsites --dataset_filter=*la* --hdx_site=stage --key=private --value=True
```

Then applying to all the datasets in the Organisation, those already updated are skipped:

```shell
hdx-toolkit list --organisation=healthsites--dataset_filter=* --hdx_site=stage --key=private --value=True
hdx-toolkit update --organisation=healthsites --dataset_filter=* --hdx_site=stage --key=private --value=True
```
The initial update takes approximately 10 seconds but subsequent updates in a list take only a couple of seconds.

## Contributions

This project users a GitHub Action to run tests and linting. It requires the following environment variables/secrets to be set in the `test` environment:

```
HDX_KEY - secret. Value: fake secret
HDX_SITE - environment variable. Value: stage
USER_AGENT - environment variable. Value: hdx_cli_toolkit_gha
PREPREFIX - - environment variable. Value: [YOUR_ORGANISATION]
```

Testing uses a mock for the HDX so a live HDX_KEY is not required.




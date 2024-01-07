# HDX CLI Toolkit

## Overview

This toolkit is intended to provide a commandline interface to HDX to allow for bulk modification operations, in the first instance to carry out a bulk quarantine action on all the datasets in an organisation. It is inspired by [hdx-update-cods-level](https://github.com/b-j-mills/hdx-update-cods-level/tree/main).



## Installation
First a virtual enviroment is created and activated

```shell
python -m venv venv
source venv/Scripts/activate
```

The code is installed with

```shell
pip install -e .
```

Configuration for `hdx-python-api` is done in the usual way [described here](https://hdx-python-api.readthedocs.io/en/latest/). 

The user agent (`hdx_cli_toolkit_ih`) is specified in the `~/.useragents.yaml` file the suffix _ih should be replaced with the users initials.
```
hdx-cli-toolkit:
    preprefix: HDXINTERNAL
    user_agent: hdx_cli_toolkit_ih
```


## Usage

The `hdx-toolkit` is built using the Python `click` library. Details of the currently implemented commands can be revealed by running:

```
hdx-toolkit --help
```

And details of the arguments for a command can be found using

```
hdx-toolkit [COMMAND] --help
```

`update` is clearly an operation with potential negative side-effects. Commands can be tested on the `stage` site by setting `--hdx_site=stage`. In addition the `list` command can be used to check the datasets to be effected since `list` and `update` both take the same arguments and use the same filtering function although for `list` the `--value` argument is ignored:

The default orgnaisation is `healthsites` reflecting the original 
```shell
hdx-toolkit list --organisation=healthsites --dataset_filter=mali-healthsites --hdx_site=stage --key=private --value=True
hdx-toolkit update --organisation=healthsites --dataset_filter=mali-healthsites --hdx_site=stage --key=private --value=False
hdx-toolkit update --organisation=healthsites --dataset_filter=mali-healthsites --hdx_site=stage --key=private --value=True
```

We can then be slightly more adventurous:

```shell
hdx-toolkit list --organisation=healthsites --dataset_filter=*la* --hdx_site=stage --key=private --value=True
hdx-toolkit update --organisation=healthsites --dataset_filter=*la* --hdx_site=stage --key=private --value=True
```

Then go mad:

```shell
hdx-toolkit list --organisation=healthsites--dataset_filter=* --hdx_site=stage --key=private --value=True
hdx-toolkit update --organisation=healthsites --dataset_filter=* --hdx_site=stage --key=private --value=True
```

## Contributions

This project users a GitHub Action to run tests and linting. It requires the following environment variables/secrets to be set in the `test` environment:

```
HDX_KEY - secret. value: fake secret
HDX_SITE - environment variable value: stage
USER_AGENT - environment variable: hdx_cli_toolkit_gha
PREPREFIX - - environment variable: HDXINTERNAL
```




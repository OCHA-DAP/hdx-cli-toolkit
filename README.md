# HDX CLI Toolkit

## Overview

This toolkit is intended to provide a commandline interface to HDX to allow for bulk modification operations and other administrative activities, in the first instance to carry out a bulk quarantine action on all the datasets in an organization. It is inspired by [hdx-update-cods-level](https://github.com/b-j-mills/hdx-update-cods-level/tree/main).

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

`hdx-cli-toolkit` uses the `hdx-python-api` library, this requires the following to be added to a file called `.hdx_configuration.yaml` in the user's home directory.

```
hdx_key_stage: "[hdx_key from the staging HDX site]"
hdx_key: "[hdx_key from the prod HDX site]"
```

A user agent (`hdx_cli_toolkit_*`) is specified in the `~/.useragents.yaml` file the * replaced with the users initials.
```
hdx-cli-toolkit:
    preprefix: [YOUR_ORGANIZATION]
    user_agent: hdx_cli_toolkit_ih
```


## Usage

The `hdx-toolkit` is built using the Python `click` library. Details of the currently implemented commands can be revealed by running `hdx-toolkit --help`:

```
$ hdx-toolkit --help
Usage: hdx-toolkit [OPTIONS] COMMAND [ARGS]...

  Tools for Commandline interactions with HDX

Options:
  --help  Show this message and exit.

Commands:
  configuration              Print configuration information to terminal
  get_organization_metadata  Get an organization id and other metadata
  get_user_metadata          Get user id and other metadata
  list                       List datasets in HDX
  print                      Print datasets in HDX to the terminal
  quickcharts                Upload QuickChart JSON description to HDX
  showcase                   Upload showcase to HDX
  update                     Update datasets in HDX
  update_resource            Update a resource in HDX
```

And details of the arguments for a command can be found using:

```shell
hdx-toolkit [COMMAND] --help
```

A detailed walk through of commands can be found in the [DEMO.md](DEMO.md) file

## Contributions

This project users a GitHub Action to run tests and linting. It requires the following environment variables/secrets to be set in the `test` environment:

```
HDX_KEY - secret. Value: fake secret
HDX_KEY_STAGE - secret. Value: a live API key for the stage server
HDX_SITE - environment variable. Value: stage
USER_AGENT - environment variable. Value: hdx_cli_toolkit_gha
PREPREFIX - - environment variable. Value: [YOUR_organization]
```

Testing uses a mock for the HDX so a live HDX_KEY is not required.

New features should be developed against a GitHub issue on a separate branch with a name starting GH[issue number]_ . `PULL_REQUEST_TEMPLATE.md` should be used in preparing pull requests. Versioning is updated manually in `pyproject.toml` and is described in the template, in brief it is CalVer `YYYY.MM.Micro`.




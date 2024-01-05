# HDX CLI Toolkit

## Overview

This toolkit is intended to provide a commandline interface to HDX to allow for bulk modification 
operations. It is inspired by [hdx-update-cods-level](https://github.com/b-j-mills/hdx-update-cods-level/tree/main)

In the first instance to carry out a bulk quarantine action on all the datasets in an organisation.

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

This is added to ~/.useragents.yaml
```
hdx-cli-toolkit:
    preprefix: HDXINTERNAL
    user_agent: hdx_cli_toolkit_ih
```




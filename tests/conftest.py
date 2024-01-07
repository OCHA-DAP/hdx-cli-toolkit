#!/usr/bin/env python
# encoding: utf-8

import json
import os
import pytest

FIXTURES_DIRECTORY = os.path.join(
    os.path.abspath(os.path.dirname(__file__)),
    "fixtures",
)


@pytest.fixture
def json_fixture():
    def _json_fixture(filename):
        with open(
            os.path.join(FIXTURES_DIRECTORY, filename),
            encoding="utf-8-sig",
        ) as json_file:
            json_payload = json.load(json_file)
        return json_payload

    return _json_fixture

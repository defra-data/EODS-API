import eodslib
import os
from pathlib import Path
import pytest
import hashlib


def test_query(setup):
    conn = {
    'domain': os.getenv("HOST"),
    'username': os.getenv("API_USER"),
    'access_token': os.getenv("API_TOKEN"),
    }

    eods_params = {
    'output_dir':setup,
    'title':'keep_api_test_create_group',
    #'limit':1,
    }

    list_of_layers, df = eodslib.query_catalog(conn, **eods_params)

    errors = []

    response_json = eodslib.create_layer_group(
                                              conn,
                                              list_of_layers,
                                              'eodslib-create-layer-test',
                                              abstract='some description of the layer group'
                                              )

    print(response_json)

    # content checks
    assert 4 == 5
    # assert not errors

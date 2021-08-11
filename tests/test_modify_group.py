import eodslib
import os
from pathlib import Path
import pytest
import hashlib


def test_modify(set_output_dir, modify_id_list, unique_run_string):
    output_dir = set_output_dir
    conn = {
    'domain': os.getenv("HOST"),
    'username': os.getenv("API_USER"),
    'access_token': os.getenv("API_TOKEN"),
    }

    eods_params = {
    'output_dir':output_dir,
    'title':'keep_api_test_update_group',
    'verify': False,
    #'limit':1,
    }

    list_of_layers, _ = eodslib.query_catalog(conn, **eods_params)

    os.rename(output_dir / 'eods-query-all-results.csv', output_dir / 'eods-query-all-results-modify-group-test.csv')

    errors = []

    response_json = eodslib.modify_layer_group(
                                              conn,
                                              list_of_layers,
                                              modify_id_list[0],
                                              abstract='update the abstract ' + unique_run_string
                                              )

    # content checks
    if len(response_json['layers']) != 1:
        errors.append(f"Content Error: \'layers\' in response text should contain only 1 layers, got {len(response_json['layers'])} layers")

    if response_json['layers'][0] != 'geonode:keep_api_test_update_group':
        errors.append(f"Content Error: 1st layer of \'layers\' in response text should be \'geonode:keep_api_test_update_group\', it was \'{response_json['layers'][0]}\'")

    if response_json['abstract'] != 'update the abstract ' + unique_run_string:
        errors.append(f"Content Error: \'abstract\' in response text should be \'update the abstract {unique_run_string}\', it was \'{response_json['abstract']}\'")

    assert not errors

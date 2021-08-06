import eodslib
import os
from pathlib import Path
import pytest
import os


def test_create(setup, modify_id_list, unique_run_string):
    conn = {
    'domain': os.getenv("HOST"),
    'username': os.getenv("API_USER"),
    'access_token': os.getenv("API_TOKEN"),
    }

    eods_params = {
    'output_dir':setup,
    'title':'keep_api_test_create_group',
    'verify': False,
    #'limit':1,
    }

    list_of_layers, _ = eodslib.query_catalog(conn, **eods_params)

    os.rename(setup / 'eods-query-all-results.csv', setup / 'eods-query-all-results-create-group-test.csv')

    errors = []

    response_json = eodslib.create_layer_group(
                                              conn,
                                              list_of_layers,
                                              'eodslib-create-layer-test-' + unique_run_string,
                                              abstract='some description of the layer group ' + unique_run_string
                                              )

    if not modify_id_list:
        modify_id_list.append(response_json['id'])

    lower_unique_run_string = unique_run_string.lower().replace('-', '_').replace('/', '').replace(':', '')

    # content checks
    if len(response_json['layers']) != 1:
        errors.append(f"Content Error: \'layers\' in response text should contain only 1 layers, got {len(response_json['layers'])} layers")

    if response_json['layers'][0] != 'geonode:keep_api_test_create_group':
        errors.append(f"Content Error: 1st layer of \'layers\' in response text should be \'geonode:keep_api_test_create_group\', it was \'{response_json['layers'][0]}\'")

    if response_json['abstract'] != 'some description of the layer group ' + unique_run_string:
        errors.append(f"Content Error: \'abstract\' in response text should be \'some description of the layer group {unique_run_string}\', it was \'{response_json['abstract']}\'")

    if response_json['alternate'] != 'geonode:eodslib_create_layer_test_' + lower_unique_run_string:
        errors.append(f"Content Error: \'alternate\' in response text should be \'geonode:eodslib_create_layer_test_{lower_unique_run_string}\', it was \'{response_json['alternate']}\'")

    if response_json['name'] != 'eodslib_create_layer_test_' + lower_unique_run_string:
        errors.append(f"Content Error: \'name\' in response text should be \'eodslib_create_layer_test_{lower_unique_run_string}\', it was \'{response_json['name']}\'")

    if response_json['title'] != 'eodslib-create-layer-test-' + unique_run_string:
        errors.append(f"Content Error: \'title\' in response text should be \'eodslib-create-layer-test-{unique_run_string}\', it was \'{response_json['title']}\'")

    assert not errors

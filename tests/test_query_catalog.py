import eodslib
import os
import pytest
import pandas as pd


def test_query(set_output_dir):
    output_dir = set_output_dir
    conn = {
    'domain': os.getenv("HOST"),
    'username': os.getenv("API_USER"),
    'access_token': os.getenv("API_TOKEN"),
    }

    eods_params = {
    'output_dir':output_dir,
    'title':'keep_api_test_create_group',
    'verify': False,
    # 'sat_id': 2,
    # 'find_least_cloud': True
    # 'limit':1,
    }

    list_of_layers, df = eodslib.query_catalog(conn, **eods_params)
    print(list_of_layers)
    with pd.option_context('display.max_rows', None, 'display.max_columns', None):
        print(df)

    os.rename(output_dir / 'eods-query-all-results.csv', output_dir / 'eods-query-all-results-query-test.csv')

    errors = []

    # content checks

    if len(list_of_layers) != 1:
        errors.append(f'Content Error: list_of_layers should contain only 1 item, got {len(list_of_layers)} items')

    if list_of_layers[0] != 'geonode:keep_api_test_create_group':
        errors.append(f"Content Error: 1st item of list_of_layers should be \'geonode:keep_api_test_create_group\', it was \'{list_of_layers[0]}\'")

    if len(df.index) != 1:
        errors.append(f'Content Error: df should contain only 1 row, got {len(df.index)} rows')

    if len(df.index) > 0:
        if df.iloc[0]['title'] != 'keep_api_test_create_group':
            errors.append(f"Content Error: 1st row of df should have \'title\' of \'keep_api_test_create_group\', it was \'{df.iloc[0]['title']}\'")

        if df.iloc[0]['alternate'] != 'geonode:keep_api_test_create_group':
            errors.append(f"Content Error: 1st row of df should have \'alternate\' of \'geonode:keep_api_test_create_group\', it was \'{df.iloc[0]['alternate']}\'")

    assert not errors

    # content type (tested implictly by json.loads()), response code (tested explicitly in eodslib)

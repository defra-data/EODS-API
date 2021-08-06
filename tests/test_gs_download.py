import eodslib
import os
from pathlib import Path
import pytest
import hashlib
from io import BytesIO
import zipfile
import pandas as pd

def test_download(setup):
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
    
    errors = []
    list_of_results = []

    config_wpsprocess = {'template_xml':'gsdownload_template.xml',
        'xml_config':{
            'template_layer_name':list_of_layers[0],
            'template_outputformat':'image/tiff',
            'template_mimetype':'application/zip'
                },
        'dl_bool':True
    }

    output_dir = setup

    execution_dict = eodslib.run_wps(conn, config_wpsprocess, output_dir=output_dir, verify=False)

    list_of_results.append(execution_dict)

    eodslib.output_log(list_of_results)

    os.rename(setup / 'wps-log.csv', setup / 'wps-log-gs-dl-test.csv')
    os.rename(setup / 'eods-query-all-results.csv', setup / 'eods-query-all-results-gs-dl-test.csv')

    df = pd.read_csv(setup / 'wps-log-gs-dl-test.csv')

    if len(df.index) != 1:
        errors.append(f'Content Error: output log should contain only 1 row, got {len(df.index)} rows')

    if df.iloc[0]['layer_name'] != 'geonode:keep_api_test_create_group':
        errors.append(f"Content Error: 1st row of output log should have \'layer_name\' of \'geonode:keep_api_test_create_group\', it was \'{df.iloc[0]['layer_name']}\'")

    if df.iloc[0]['filename_stub'] != 'keep_api_test_create_group':
        errors.append(f"Content Error: 1st row of df should have \'filename_stub\' of \'keep_api_test_create_group\', it was \'{df.iloc[0]['filename_stub']}\'")


    hash = hashlib.sha3_256()
    with open(setup / 'keep_api_test_create_group.tiff', "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash.update(chunk)
    hash_out = hash.hexdigest()

    checksum = '8deb950fb6aa34b6529735d7bc2dc7b2fa4b83806b6dcd9a027f3c184b598da4'
    if hash_out != checksum:
        errors.append("Checksum Error: Expected " + checksum + ", got " + hash_out)

    assert not errors

import eodslib
import os
from pathlib import Path
import pytest
import hashlib
from io import BytesIO
import zipfile
import pandas as pd

def test_crop(setup):
    conn = {
    'domain': os.getenv("HOST"),
    'username': os.getenv("API_USER"),
    'access_token': os.getenv("API_TOKEN"),
    }

    cutting_geom_osgbwkt = 'POLYGON((372400 213749, 372400 209750, 376487 209750, 376487 213749, 372400 213749))'

    eods_params = {
    'output_dir':setup,
    'title':'keep_api_test_create_group',
    'verify': False,
    }

    list_of_layers, df = eodslib.query_catalog(conn, **eods_params)

    wkt = df.loc[(df['alternate'] == list_of_layers[0])]['csw_wkt_geometry'].item()
    lower_left, upper_right = eodslib.get_bbox_corners_from_wkt(wkt,27700)
    
    errors = []
    list_of_results = []

    config_wpsprocess = {
         'template_xml':'rascropcoverage_template.xml',
         'xml_config':{
            'template_layer_name':list_of_layers[0],
            'template_mimetype':'image/tiff',
            'template_ll':str(lower_left.x) + ' ' + str(lower_left.y),
            'template_ur':str(upper_right.x) + ' ' + str(upper_right.y),
            'template_clip_geom':cutting_geom_osgbwkt
            },
         'dl_bool':True
        }

    output_dir = setup

    execution_dict = eodslib.run_wps(conn, config_wpsprocess, output_dir=output_dir, verify=False)

    list_of_results.append(execution_dict)

    eodslib.output_log(list_of_results)

    os.rename(setup / 'wps-log.csv', setup / 'wps-log-gs-rcrop-test.csv')
    os.rename(setup / 'eods-query-all-results.csv', setup / 'eods-query-all-results-gs-rcrop-test.csv')
    os.rename(setup / 'keep_api_test_create_group.tiff', setup / 'keep_api_test_create_group_gs_rcrop.tiff')


    log_df = pd.read_csv(setup / 'wps-log-gs-rcrop-test.csv')

    if len(log_df.index) != 1:
        errors.append(f'Content Error: output log should contain only 1 row, got {len(log_df.index)} rows')

    if log_df.iloc[0]['layer_name'] != 'geonode:keep_api_test_create_group':
        errors.append(f"Content Error: 1st row of output log should have \'layer_name\' of \'geonode:keep_api_test_create_group\', it was \'{log_df.iloc[0]['layer_name']}\'")

    if log_df.iloc[0]['filename_stub'] != 'keep_api_test_create_group':
        errors.append(f"Content Error: 1st row of df should have \'filename_stub\' of \'keep_api_test_create_group\', it was \'{log_df.iloc[0]['filename_stub']}\'")


    hash = hashlib.sha3_256()
    with open(setup / 'keep_api_test_create_group_gs_rcrop.tiff', "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash.update(chunk)
    hash_out = hash.hexdigest()

    checksum = 'e62d562ca491d56c0b2a6c327a612f30390a3bbe103c8e72d9e874e6609651d4'
    if hash_out != checksum:
        errors.append("Checksum Error: Expected " + checksum + ", got " + hash_out)

    assert not errors

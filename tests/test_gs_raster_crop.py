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

    # cat_search_geom_wgs84wkt_query = 'POLYGON((-1.18 50.94, -1.18 50.86, -1.06 50.86, -1.06 50.94, -1.18 50.94))'
    # cutting_geom_osgbwkt = 'POLYGON((455556 114292, 455556 106403, 467913 106403, 467913 114292, 455556 114292))'
    cutting_geom_osgbwkt = 'POLYGON((372400 213749, 372400 209750, 376487 209750, 376487 213749, 372400 213749))'

    eods_params = {
    'output_dir':setup,
    'title':'keep_api_test_create_group',
    #'geom':cat_search_geom_wgs84wkt_query,
    }

    list_of_layers, df = eodslib.query_catalog(conn, **eods_params)

    print(df)

    wkt = df.loc[(df['alternate'] == list_of_layers[0])]['csw_wkt_geometry'].item()
    lower_left, upper_right = eodslib.get_bbox_corners_from_wkt(wkt,27700)
    
    errors = []
    list_of_results = []


    """config_wpsprocess = {'template_xml':'gsdownload_template.xml',
        'xml_config':{
            'template_layer_name':list_of_layers[0],
            'template_outputformat':'image/tiff',
            'template_mimetype':'application/zip'
                },
        'dl_bool':True
    }"""

    print(lower_left, upper_right)

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

    output_dir = eodslib.make_output_dir(Path.cwd() / 'output')

    execution_dict = eodslib.run_wps(conn, config_wpsprocess, output_dir=output_dir)

    list_of_results.append(execution_dict)

    eodslib.output_log(list_of_results)

    log_df = pd.read_csv(Path.cwd() / 'output' / 'wps-log.csv')

    if len(log_df.index) != 1:
        errors.append(f'Content Error: output log should contain only 1 row, got {len(log_df.index)} rows')

    if log_df.iloc[0]['layer_name'] != 'geonode:keep_api_test_create_group':
        errors.append(f"Content Error: 1st row of output log should have \'layer_name\' of \'geonode:keep_api_test_create_group\', it was \'{log_df.iloc[0]['layer_name']}\'")

    if log_df.iloc[0]['filename_stub'] != 'keep_api_test_create_group':
        errors.append(f"Content Error: 1st row of df should have \'filename_stub\' of \'keep_api_test_create_group\', it was \'{log_df.iloc[0]['filename_stub']}\'")


    hash = hashlib.sha3_256()
    with open(Path.cwd() / 'output' / 'keep_api_test_create_group.tiff', "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash.update(chunk)
    hash_out = hash.hexdigest()

    checksum = 'e62d562ca491d56c0b2a6c327a612f30390a3bbe103c8e72d9e874e6609651d4' # SND value
    if hash_out != checksum:
        errors.append("Checksum Error: Expected " + checksum + ", got " + hash_out)



    """bytes = BytesIO(response.content)
    zip = zipfile.ZipFile(bytes)
    image_name = zip.namelist()[0]
    image_file = zip.open(image_name)
    with image_file:
        check_out = hashlib.sha3_256(image_file.read()).hexdigest()
    zip.close()
    if check_out == self.test['checksum']:
        checksum_result = 'PASS'
    else:
        checksum_result = 'FAIL'
        errors.append("Checksum Error: Expected " + self.test['checksum'] + ", got " + check_out)
"""
    assert not errors

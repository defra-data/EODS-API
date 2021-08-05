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

    # hash = hashlib.sha3_256(df.to_json().encode()).hexdigest()

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

    """checksum = 'bb8def29cc79f4d23f571915d21b13ef9f815655c52e80c620e59b222d5a7c61' # SND value

    if hash != checksum:
        errors.append("Checksum Error: Expected " + checksum + ", got " + hash)"""

    """expected_df_csv = '4,abstract,alternate,category,category__gn_description,cloud_cover,csw_type,csw_wkt_geometry,date,detail_url,id,keywords,location,owner__username,popular_count,rating,regions,share_count,split_granule,srid,subtype,supplemental_information,thumbnail_url,title,type,uuid\n0,No abstract provided.,geonode:keep_api_test_create_group,,,,dataset,"POLYGON((-2.4591467333 51.7495497809,-2.4591467333 51.8218717504,-2.34253580452 51.8218717504,-2.34253580452 51.7495497809,-2.4591467333 51.7495497809))",2021-07-30T15:11:34,/layers/geonode:keep_api_test_create_group,392,"[\'geotiff\', \'keep_api_test_create_group\', \'wcs\']","{u\'type\': u\'Polygon\', u\'coordinates\': [[[-2.4591467333, 51.7495497809], [-2.4591467333, 51.8218717504], [-2.34253580452, 51.8218717504], [-2.34253580452, 51.7495497809], [-2.4591467333, 51.7495497809]]]}",r_1_19_7_dm,0,0,"[\'Global\', \'Europe\', \'United Kingdom\', \'Pacific\']",0,,EPSG:27700,raster,No information provided,https://eob-snd1.azure.defra.cloud/uploaded/thumbs/layer-6d735926-f148-11eb-9a6f-000d3ad8c622-thumb.png,keep_api_test_create_group,layer,6d735926-f148-11eb-9a6f-000d3ad8c622\n'
    
    if df.to_csv() != expected_df_csv:
        errors.append("Content Errors: Body of data frame expected to be " + repr(expected_df_csv) + ", got " + repr(str(df.to_csv())))"""

    assert not errors

    # content type (tested implictly by json.loads()), response code (tested explicitly in eodslib)

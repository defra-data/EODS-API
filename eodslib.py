"""
module of library functions for programatic interaction with Defra's Earth 
Observation Data Service (EODS)

    AUTHOR          : Sam Franklin, CGI
    LAST UPDATED    : 2021-04-20
"""

import os
import sys
import pandas as pd
pd.options.mode.chained_assignment = None
from pandas import json_normalize
from datetime import datetime
from pathlib import Path
import requests
from requests.exceptions import ConnectionError
import json
import time
import xmltodict
from zipfile import ZipFile
import shapely
import shapely.wkt
from shapely.ops import transform
import pyproj
import shutil
import numpy as np
os.environ['PROJ_NETWORK'] = 'OFF'

def run_wps(conn, config_wpsprocess, **kwargs):
    """
    primary function to orchestrate running the wps job from submission to download (if required)

    Parameters:
    -----------
        conn: dict,
            Connection parameters
            Example: conn = {'domain': 'https://earthobs.defra.gov.uk',
                             'username': '<insert-username>',
                             'access_token': '<insert-access-token>'}

        config_wpsprocess: list or dict,
            list of dictionaries for individual wps submission requests.
            users can generate a list of multiple dictionaries, one dict per wps job
            with "xml_config", this is dict of variables that templated into the xml
            payload for the WPS request submission            
            Example:
                config_wpsprocess = [{'template_xml':'gsdownload_template.xml',
                                    'xml_config':{
                                        'template_layer_name':lyr,
                                        'template_outputformat':'image/tiff',
                                        'template_mimetype':'application/zip'},
                                    'dl_bool':True
                                    }]

        output_dir: str or Pathlib object, optional,
            user specified output directory

        verify: str, optional:
            add custom path to any organisation certificate stores that the
            environment needs
            Default Value:
                * True
            Possible Value:
                * 'dir/dir/cert.file'    

    Returns:
    -----------
        list_download_paths: list,
            list of pathlib objects for downloaded output for further reuse

    """

    # set output path if not specified
    if 'output_dir' not in kwargs:
        kwargs['output_dir']=Path.cwd()

    if 'verify' not in kwargs:
        kwargs['verify'] = True

    # set the request config dictionary 
    request_config = {
        'wps_server':conn['domain'] + '/geoserver/ows',
        'access_token':conn['access_token'],
        'headers':{'Content-type': 'application/xml','User-Agent': 'python'},
        'verify':kwargs['verify']
    }

    # submit wps jobs
    try:
        execution_dict = submit_wps_queue(request_config, config_wpsprocess)
    except Exception as error:
        print(error.args)
        print('The WPS submission has failed')
    else:

        # INITIALISE VARIABLES and drop the wps log file if it exists
        path_output = make_output_dir(kwargs['output_dir'])        


        # keep calling the wps job status until 'continue_process' = False 
        while True:

            execution_dict = poll_api_status(execution_dict, request_config, path_output)

            if execution_dict['continue_process']:
                time.sleep(15)
            else:
                break

        # after download is complete, process downloaded files (eg renames and extracting zips)
        if execution_dict['job_status'] == 'DOWNLOAD-SUCCESSFUL':
            execution_dict = process_wps_downloaded_files(execution_dict)
        
        # set log file and job duration in dict
        execution_dict['log_file_path'] = path_output / 'wps-log.csv'
        execution_dict['total_job_duration'] = (execution_dict['timestamp_job_end'] - execution_dict['timestamp_job_start']).total_seconds() / 60

        return execution_dict

def submit_wps_queue(request_config, config_wpsprocess):
   
    print('\n\t\t### ' + datetime.utcnow().isoformat() + ' :: WPS SUBMISSION :: lyr=' + config_wpsprocess['xml_config']['template_layer_name'])

    try:
        # overwrite xml string with job specific parameters
        payload = mod_the_xml(config_wpsprocess)

        response = requests.post(
            request_config['wps_server'],
            params={'access_token':request_config['access_token'],'SERVICE':'WPS','VERSION':'1.0.0','REQUEST':'EXECUTE'},
            data=payload,
            headers=request_config['headers'],
            verify=request_config['verify'])

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            # response status was not 200
            raise ValueError('non-200 response, additional info' , str(e))

        else:
            if not response.text.find('ExceptionReport') > 0:

                execution_dict = {
                        'job_id':response.text.split('executionId=')[1].split('&')[0],
                        'layer_name':config_wpsprocess['xml_config']['template_layer_name'],
                        'timestamp_job_start':datetime.utcnow(),
                        'continue_process':True,
                        }

                print('\t\t### ' + datetime.utcnow().isoformat() + ' :: job : ' + execution_dict['job_id'] + ' :: WPS JOB SUBMITTED')
                print('\t\t### ' + datetime.utcnow().isoformat() + ' :: job : ' + execution_dict['job_id'] + ' :: WPS STATUS CHECK URL : ' + request_config['wps_server'] + '?SERVICE=WPS&VERSION=1.0.0&REQUEST=GETEXECUTIONSTATUS&EXECUTIONID=' + execution_dict['job_id'] + '&access_token=' + request_config['access_token'])
                
                return execution_dict
            else:
                raise ValueError('wps server returned an exception', str(response.text))
                    
    except ValueError as error:

        # handle two different valueErrors for connection fail and incorrect WPS submission parmeters

        execution_dict = error

        print(datetime.utcnow().isoformat() + ' :: WPS submission failed :: check log for errors = ' + str(error.args))

        return execution_dict

def poll_api_status(execution_dict, request_config, path_output):
    """
    for each execution job, return the status of the job
    """

    try:

        if execution_dict['continue_process']:

            print('\t\t### ' + datetime.utcnow().isoformat() + ' :: job : ' + execution_dict['job_id'] + ' :: CHECKING STATUS')

            params = {
                'access_token':request_config['access_token'],
                'SERVICE':'WPS',
                'VERSION':'1.0.0',
                'REQUEST':'GetExecutionstatus',
                'EXECUTIONID':execution_dict['job_id'],
                }

            response = requests.get(
                request_config['wps_server'],
                params=params,
                headers=request_config['headers'],
                verify=request_config['verify']
                )

            # parse xml to python dictionary 
            d = xmltodict.parse(response.content)

            if 'wps:ExecuteResponse' in d:

                if 'wps:ProcessSucceeded' in d['wps:ExecuteResponse']['wps:Status']:

                    execution_dict.update({
                        'job_status':'READY-TO-DOWNLOAD',
                        'dl_url':d['wps:ExecuteResponse']['wps:ProcessOutputs']['wps:Output']['wps:Reference']['@href'] + '&access_token=' + request_config['access_token'],
                        'mime_type':d['wps:ExecuteResponse']['wps:ProcessOutputs']['wps:Output']['wps:Reference']['@mimeType'],
                        'timestamp_ready_to_dl':datetime.utcnow(),
                        })

                    print('\t\t### ' + datetime.utcnow().isoformat() + ' :: job : ' + execution_dict['job_id'] + ' :: READY FOR DOWNLOAD')

                    # if successful, return status = DOWNLOADED
                    execution_dict = download_wps_result_single(request_config, execution_dict, path_output)

                elif 'wps:ProcessFailed' in d['wps:ExecuteResponse']['wps:Status']:

                    print('\t\t### ' + datetime.utcnow().isoformat() + ' :: job : ' + execution_dict['job_id'] + ' :: JOB-FAILED ... check LOG for further details')

                    # TODO: add exception message. Needs testing
                    execution_dict.update({
                        'job_status':'WPS-FAILURE',
                        'continue_process':False,
                        'message':'GEOSERVER FAILURE REPORT',
                        'timestamp_job_end':datetime.utcnow(),
                        })
                else:
                    execution_dict.update({'job_status':'OUTSTANDING'})
                    print('\t\t### ' + datetime.utcnow().isoformat() + ' :: job : ' + execution_dict['job_id'] + ' :: STILL IN PROGRESS')

            elif 'ows:ExceptionReport' in d:

                exception_text = d['ows:ExceptionReport']['ows:Exception']['ows:ExceptionText']

                execution_dict.update({
                    'job_status':'WPS-GENERAL-ERROR',
                    'continue_process':False,
                    'message':'THIS IS A GENERAL ERROR WITH A WPS JOB. ERROR MESSAGE = ' + str(exception_text),
                    'timestamp_job_end':datetime.utcnow(),
                    })

                print('\t\t### ' + datetime.utcnow().isoformat() + ' :: job : ' + execution_dict['job_id'] + ' :: JOB-FAILED ... check LOG for further details')
              
        return execution_dict

    except Exception as error:

        print('\t\t### ' + datetime.utcnow().isoformat() + ' :: job : ' + execution_dict['job_id'] + ' :: UNKNOWN EXCEPTION')

        execution_dict.update({
            'job_status':'UNKNOWN-GENERAL-ERROR',
            'continue_process':False,
            'message':'UNKNOWNN GENERAL ERROR ENCOUNTERED WHEN CHECKING STATUS OF WPS JOB. ERROR MESSAGE:' + str(error),
            'timestamp_job_end':datetime.utcnow(),
            })
        return execution_dict

def download_wps_result_single(request_config, execution_dict, path_output):
    """
    function to get a wps result if the response is SUCCEEDED AND download is set to True in the config
    """
    
    file_extension = '.' + execution_dict['mime_type'].split('/')[1]
    filename_stub = execution_dict['layer_name'].split(':')[-1]
    dl_path = path_output / filename_stub
    dl_path.mkdir(parents=True, exist_ok=True)        
    local_file_name = Path(dl_path / str( filename_stub + file_extension))

    # make three download attempts
    for i in [1,2,3]:

        print('\t\t### ' + datetime.utcnow().isoformat() + ' :: job : ' + execution_dict['job_id'] + ' :: DOWNLOAD START : TRY ' + str(i) + ' of 3')
        
        try:

            with requests.get(
                execution_dict['dl_url'],
                headers=request_config['headers'],
                verify=request_config['verify'],
                stream=True) as response:
                 
                with open(local_file_name, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192*1024):
                        f.write(chunk)

            print('\t\t### ' + datetime.utcnow().isoformat() + ' :: job : ' + execution_dict['job_id'] + ' :: DOWNLOAD COMPLETE ON TRY ' + str(i))

            execution_dict.update({
                'job_status':'DOWNLOAD-SUCCESSFUL',
                'continue_process':False,
                'dl_file':local_file_name,
                'file_extension':file_extension,
                'filename_stub':filename_stub,
                'timestamp_dl_end':datetime.utcnow(),
                'timestamp_job_end':datetime.utcnow(),
                'download_try':i
                })

            return execution_dict

        except Exception as error:

            if i == 3:

                execution_dict.update({
                    'job_status':'DOWNLOAD-FAILED',
                    'continue_process':False,
                    'message':str(error),
                    'timestamp_job_end':datetime.utcnow(),
                    'download_try': i,
                    })

                print('\t\t### ' + datetime.utcnow().isoformat() + ' :: job : ' + execution_dict['job_id'] + ' :: STATUS : ' + execution_dict['job_status'] + ' :: MESSAGE :' + execution_dict['message'])

                return execution_dict

def process_wps_downloaded_files(execution_dict):
    """
    function to rename downloaded file if TIFF or extract from zip 
    """
    
    try:

        source_file_to_extract = execution_dict['dl_file']

        print('\t\t### ' + datetime.utcnow().isoformat() + ' :: job : ' + execution_dict['job_id'] + ' :: PROCESS DOWNLOAD START')

        # handle rename of zip contents
        if source_file_to_extract.suffix.lower() == '.zip':

            zip_ref = ZipFile(source_file_to_extract, 'r')
            zip_ref.extractall(source_file_to_extract.parent.parent)
            zip_list = zip_ref.filelist
            zip_ref.close()
            source_file_to_extract.unlink()            

            # loop through zip contents
            for f in zip_list:
            
                f_path = Path(source_file_to_extract.parent.parent / f.filename)
            
                if f_path.suffix.lower() == ".sld":
                    f_path.unlink()
                else:
                    Path(f_path).replace(source_file_to_extract.parent.parent / str(execution_dict['filename_stub'] + f_path.suffix.lower()))

        else:
            Path(source_file_to_extract).rename(source_file_to_extract.parent.parent / str(execution_dict['filename_stub'] + source_file_to_extract.suffix.lower()))

        # del download directory
        if source_file_to_extract.parent.is_dir():
            source_file_to_extract.parent.rmdir()

        execution_dict.update({
            'job_status':'LOCAL-POST-PROCESSING-SUCCESSFUL',
            'timestamp_extraction_end':datetime.utcnow(),
            'timestamp_job_end':datetime.utcnow(),
        })
        
        print('\t\t### ' + datetime.utcnow().isoformat() + ' :: job : ' + execution_dict['job_id'] + ' :: PROCESS DOWNLOAD END')

        return execution_dict

    except Exception as error:

        execution_dict.update({
            'message':'ERROR in extraction of files :: MESSAGE = ' + str(error)
        })

        return execution_dict

def output_log(list_of_results):

    df = pd.DataFrame(list_of_results)
    log_file_name = list_of_results[0]['log_file_path']
    df.to_csv(log_file_name, index_label='num')
    print('\t\t### ' + datetime.utcnow().isoformat() + ' :: JOB FINISHED. LOG FILE LOCATION : ' + str(log_file_name))

def make_output_dir(path_output):
    """
    generate output directory if it does not exist
    """

    if isinstance(path_output, Path) == False:
        path_output = Path(path_output)

    path_output.mkdir(parents=True, exist_ok=True)

    return path_output

def find_minimum_cloud_list(df):
    """
    eods query "special" keyword function
    + cross checks the dataframe with a static csv file of 'safe' granule list
    + then groups by the unique granule-reference
    + sorts by cloud cover and takes the lowest cloud value per granule
    + returns a new dataframe
    """
    
    df['title_stub'] = df['title'].str.split('_T', expand=True).loc[:,0] + '_' + df['granule-ref'].str[:6]

    no_split_df = df[~df['title'].str.contains("SPLIT")]

    # import safe granule-orb list
    if Path(Path.cwd() / 'static' / 'safe-granule-orbit-list.txt').exists():
        df_safe_list = pd.read_csv(Path.cwd() / 'static' / 'safe-granule-orbit-list.txt')
    else:
        raise ValueError('ERROR :: safe-granule-orbit-list.txt cannot be found')

    # create new col that matches the granule-orbit syntax
    no_split_df['gran-orb'] = no_split_df['granule-ref'].str[:6] + '_' + no_split_df['orbit-ref']
    no_split_df['granule-stub'] = no_split_df['granule-ref'].str[:6]

    df['gran-orb'] = df['granule-ref'].str[:6] + '_' + df['orbit-ref']
    df['granule-stub'] = df['granule-ref'].str[:6]

    safe_df = pd.merge(no_split_df, df_safe_list)

    if len(safe_df) > 0:
        return_df = safe_df.sort_values("split_cloud_cover").groupby(["granule-stub"], as_index=False).nth(0).sort_values("granule-stub")

        matching_split_series = return_df[return_df['split_granule.name'].notna()]['split_granule.name']
        matching_split_df = df[df['alternate'].isin(matching_split_series)]

        return_df = pd.concat([return_df, matching_split_df], ignore_index=True)

    else:
        raise ValueError('ERROR : You have selected find_lowest_cloud=True BUT your search criteria is too narrow, spatially or temporally and did not match any granule references in "./static/safe-granule-orbit-list.txt". Suggest widening your search')

    return return_df  


def query_catalog(conn, **kwargs):
    """Transform vectors from source to target coordinate reference system.
    Transform vectors of x, y and optionally z from source
    coordinate reference system into target.

    Parameters
    ------------
    conn: dict
        connection dictionary 
        Example: conn = {
            'domain': 'https://earthobs.defra.gov.uk',
            'username': '<insert-username>',
            'access_token': '<insert-access-token>',
            }}
    start_date: str, optional
        filter start date in format YYYY-MM-DD
    end_date: str, optional
        filter start end in format YYYY-MM-DD
    sat_id: int, optional
        filter for either sentinel-1 and sentinel-2
        Possible values:
            * 1
            * 2        
    type: str, optional,
        filter on data type, user can override default
        Default value:
            * 'layer'        
        Possible values:
            * 'raster'
            * 'vector'
    title: str, optional,
        filter on match or partial match of the layer name/title
    cloud_min: int, optional,
        filter on minima cloud value for s2 data, must be used
        with cloud_max keyword
        Possible values:
            * 0 to 100
    cloud_max: int, optional,
        filter on minima cloud value for s2 data, must be used
        with cloud_min keyword
        Possible values:
            * 0 to 100
    geom: str, optional,
        filter layers that intersect with Well Know Text WGS84 geometry
        Example:
            geom = 'Polygon((-2.4 51.9, -2.4 51.6, -1.9 51.6, -1.9 51.9, -2.4 51.9))'
    limit: int, optional:
        limit the number of records returned by query. As of 2020-09-11 there
        are ~ 10,500 records on EODS
        Default Value:
            * 20000
        Possible values:
            * 0 to n 
    find_least_cloud: bool, optional:
        custom filter to generate a list of least cloud per granule
    verify: str, optional:
        add custom path to any organisation certificate stores that the
        environment needs
        Default Value:
            * True
        Possible Value:
            * 'dir/dir/cert.file'
    output_dir: str or pathlib object, optional:
        user can specify custom output directory for data
        Default Value:
            * Path.cwd() the current working path.
        Possible Value:
            * 'some/dir/'
            * Path('some/dir')

    Returns
    ---------
    list_of_layers: list,
        List of layer names that match the EODS input query
    
    df: Pandas DataFrame,
        DataFrame table of results containing all metadata of query results
    """

    params = {
        'username':conn['username'],
        'api_key':conn['access_token'],
        'offset':0,
    }

    # set output path if not specified
    if 'output_dir' not in kwargs:
        kwargs['output_dir']=Path.cwd()

    # set limit to a sufficiently big number if not specified
    if 'limit' not in kwargs:
        params.update({'limit':20000})
    else:
        params.update({'limit':kwargs['limit']})

    # set type to layer if "raster" or "vector" not specified
    if 'type' not in kwargs:
        kwargs['type']='layer'
    else:
        params.update({'type__in':kwargs['type']})

    if 'verify' not in kwargs:
        kwargs['verify'] = True

    if 'sat_id' in kwargs and 'find_least_cloud' in kwargs:
        if kwargs['sat_id'] == 1:            
            # throw an error if user specifies an s2 custom function with s1
            raise ValueError("QUERY failed, you have specified 'sat_id'=1 and 'find_least_cloud'=True. Use 'sat_id'=2 and 'find_least_cloud'=True")
    
    if 'sat_id' in kwargs:
        params.update({'keywords__slug__in': 'sentinel-' + str(kwargs['sat_id'])})        
    
    # date filter condition check and set
    if 'start_date' in kwargs and 'end_date' not in kwargs:
        raise ValueError("QUERY failed, if querying by date, please specify *BOTH* 'start_date' and 'end_date'")
    elif 'start_date' not in kwargs and 'end_date' in kwargs:
        raise ValueError("QUERY failed, if querying by date, please specify *BOTH* 'start_date' and 'end_date'")
    elif 'start_date' in kwargs and 'end_date' in kwargs:
        params.update({'date__range': kwargs['start_date'] + ' 00:00,' + kwargs['end_date'] + ' 00:00'})
    
    if 'title' in kwargs:
        params.update({'q': kwargs['title']})

    if 'geom' in kwargs:
        params.update({'geometry': kwargs['geom']})

    # cloud filter condition check and set    
    if 'cloud_min' in kwargs and 'cloud_max' not in kwargs:
        raise ValueError("QUERY failed, if querying by cloud cover, please specify *BOTH* 'cloud_min' and 'cloud_max'")
    elif 'cloud_min' not in kwargs and 'cloud_max' in kwargs:
        raise ValueError("QUERY failed, if querying by cloud cover, please specify *BOTH* 'cloud_min' and 'cloud_max'")
    elif 'cloud_min' in kwargs and 'cloud_max' in kwargs:
        if kwargs['sat_id'] == 1:
            raise ValueError("QUERY failed, if querying by cloud cover, please specify 'sat_id'=2")
        elif kwargs['sat_id'] == 2:
            params.update({'cc_min': kwargs['cloud_min']})
            params.update({'cc_max': kwargs['cloud_max']})            

    print('\n' + datetime.utcnow().isoformat() + ' :: PARAMENTERS USED = ' + str(params))

    try:
        response = requests.get(
            conn['domain'] + '/api/base/search',
            params=params,
            verify=kwargs['verify'],
            headers={'User-Agent': 'python'}
        )

        if response.status_code == 200:

            print(datetime.utcnow().isoformat() + ' :: RESPONSE STATUS = 200 (SUCCESS)')
            print(datetime.utcnow().isoformat() + ' :: QUERY URL USED = ' + response.url)
            
            # create a json object of the api payload content
            json_response = json.loads(response.content)

            if json_response['meta']['total_count'] > 0:

                df = json_normalize(json_response, 'objects')

                # add extra cols to df for s2 info
                if 'sat_id' in kwargs:
                    if kwargs['sat_id'] == 2:
                        df['granule-ref'] = df['title'].str.split('_',n=4).str[3]
                        df['orbit-ref'] = df['title'].str.split('_',n=5).str[-2]
                        df['ARCSI_CLOUD_COVER'] =df['supplemental_information'].str.split(n=6).str[5]

                if 'find_least_cloud' in kwargs and kwargs['sat_id'] == 2:
                    if kwargs['find_least_cloud']:
                        temp_df = df[df['split_granule.name'].notna()][['alternate', 'ARCSI_CLOUD_COVER']].copy()
                        temp_df.rename(columns={"alternate": "split_granule.name", "ARCSI_CLOUD_COVER": "split_ARCSI_CLOUD_COVER"}, inplace=True)
                        merged_df = df[df['split_granule.name'].notna()].reset_index().merge(temp_df, how='outer', on='split_granule.name').set_index('index')
                        df['split_ARCSI_CLOUD_COVER'] = np.nan
                        df.loc[df['split_granule.name'].notna(), 'split_ARCSI_CLOUD_COVER'] = merged_df['split_ARCSI_CLOUD_COVER']

                        split_cloud_cover = np.where(df['split_granule.name'].notna(), ((df['ARCSI_CLOUD_COVER'].astype(
                            float) + df['split_ARCSI_CLOUD_COVER'].astype(
                            float).astype(float))/2).astype(str), df['ARCSI_CLOUD_COVER'])

                        df['split_cloud_cover'] = split_cloud_cover

                        filtered_df = find_minimum_cloud_list(df)
                else:
                    filtered_df = df.copy()

                # make output paths
                path_output = make_output_dir(kwargs['output_dir'])
                log_file_name = path_output / 'eods-query-all-results.csv'
                filtered_df.to_csv(log_file_name)                    
            
                output_list = filtered_df['alternate'].tolist()

                print('\nMatching Layers:\n')
                for item in output_list:
                    print('\t' + item)
                print('\n' + datetime.utcnow().isoformat() + ' :: NUMBER OF LAYERS RETURNED = ' + str(len(output_list)) + '\n')

            else:
                output_list = []
                filtered_df = None
                print('\n' + datetime.utcnow().isoformat() + ' :: QUERY WAS ACCEPTED BUT PARAMETERS USED RETURNED ZERO MATCHING RECORDS, TRY A DIFFERENT SET OF PARAMETERS')

            return output_list, filtered_df

        else:
            raise ValueError(datetime.utcnow().isoformat() + ' :: RESPONSE STATUS = ' + str(response.status_code) + ' (NOT SUCCESSFUL)' + str(response.status_code) + ' :: QUERY URL = ' + response.url)

    except requests.exceptions.RequestException as e:
        print('\n' + datetime.utcnow().isoformat() + ' :: ERROR, an Exception was raised, no list returned')
        print(e)
        return None
    
def mod_the_xml(item):
    """
    function read xml payload template and modify the payload with the config
    """
    path_xml_file = Path(os.path.dirname(os.path.realpath(__file__))) / 'xml' / item['template_xml']

    try:
        assert path_xml_file.exists and path_xml_file.is_file()
    except AssertionError as err:        
        print('\n\t ### ERROR :: pyeods cannot find the specified xml file\n')
        raise err
    
    with open(path_xml_file,'r') as template_xml:
        file_data = template_xml.read()
        
        if item['xml_config'] is not None:
            # update other "fixed" keys in the xml config
            for key, value in item['xml_config'].items():

                if key == 'template_mimetype':
                    value = '"' + value + '"'
                file_data = file_data.replace(key, value)
    
    return file_data
    
def get_bbox_corners_from_wkt(csw_wkt_geometry,epsg):
    """
    function to return a bbox coordinate pair representing lower_left and upper_right of an EODS layer's bounds
    """

    wkt_obj = shapely.wkt.loads(csw_wkt_geometry)

    ll_wgs84_pt = shapely.geometry.Point(wkt_obj.bounds[0], wkt_obj.bounds[1])
    ur_wgs84_pt = shapely.geometry.Point(wkt_obj.bounds[2], wkt_obj.bounds[3])

    wgs84 = pyproj.CRS('EPSG:4326')
    to_crs_epsg = pyproj.CRS('EPSG:' + str(epsg))

    project = pyproj.Transformer.from_crs(wgs84, to_crs_epsg, always_xy=True).transform
    ll_proj_pt = transform(project, ll_wgs84_pt)
    ur_proj_pt = transform(project, ur_wgs84_pt)

    return ll_proj_pt, ur_proj_pt

def post_to_layer_group_api(conn, url, the_json):
    """
    post content layergroup endpoint

    Parameters
    ----------
    conn : dict,
        Connection parameters
        Example: conn = {'domain': 'https://earthobs.defra.gov.uk',
                            'username': '<insert-username>',
                            'access_token': '<insert-access-token>'}

    url : str
        url of which layer group to access

    the_json : dict
        the dictionary that gets parsed to the 'json' parameter of the POST request

    Returns
    -------
    json response from layergroup api
    
    """    

    headers={'Content-type': 'application/json','User-Agent': 'eods scripting'}

    params = {'username':conn['username'],'api_key':conn['access_token']}

    try:
        # post the the EODS layer group api endpoint
        response = requests.post(
            url,
            params=params,
            headers=headers,
            json=the_json,
            )

        # raise an error if the response status is not successful
        print(response.raise_for_status()) 

        # if response is successful, print the response text
        print(f'\n## Response posting to {response.url} was successful ...')
        print(f'\n## Response text : \n{response.text}')
        
        return json.loads(response.content)
        
    except Exception as error:
        print('Error caught as exception')
        print(error)

def create_layer_group(conn, list_of_layers, name, abstract=None):
    """
    create a layer group 

    Parameters
    ----------
    conn : dict,
        Connection parameters
        Example: conn = {'domain': 'https://earthobs.defra.gov.uk',
                            'username': '<insert-username>',
                            'access_token': '<insert-access-token>'}

    list_of_layers : list
        list of layers
        Example: list of layers = [
            "geonode:S2B_20200404_lat50lon503_T30UUA_ORB037_utm30n_osgb_vmsk_sharp_rad_srefdem_stdsref",
            "geonode:S2B_20200404_lat50lon363_T30UVA_ORB037_utm30n_osgb_vmsk_sharp_rad_srefdem_stdsref"
        ]

    name : str
        specify the name of the layer group

    abstract : str, optional
        specify the abstract of the layer group

    Returns
    -------
    json response from layergroup api
    
    """

    # validation checks
    if not isinstance(list_of_layers, list):
        raise TypeError('ERROR. list_of_layers is not a list, aborting ...')

    if not isinstance(name, str):
        raise TypeError('ERROR. layer group name is not a string, aborting ...')

    if len(list_of_layers) == 0 :
        raise ValueError('ERROR. list_of_layers is empty, aborting ...')

    if len(name) == 0 :
        raise ValueError('ERROR. layer group name string is empty, aborting ...')

    url = f'{conn["domain"]}/api/layer_groups/'
   
    the_json = {'name': name, 'abstract': abstract, 'layers': list_of_layers}
    
    response_json = post_to_layer_group_api(conn, url, the_json)
    
    return response_json
    
def modify_layer_group(conn, list_of_layers, layergroup_id, abstract=None):
    """
    modify a layer group, referencing the layergroup ID and list of layers

    Parameters
    ----------
    conn : dict,
        Connection parameters
        Example: conn = {'domain': 'https://earthobs.defra.gov.uk',
                            'username': '<insert-username>',
                            'access_token': '<insert-access-token>'}

    list_of_layers : list
        list of layers
        Example: list of layers = [
            "geonode:S2B_20200404_lat50lon503_T30UUA_ORB037_utm30n_osgb_vmsk_sharp_rad_srefdem_stdsref",
            "geonode:S2B_20200404_lat50lon363_T30UVA_ORB037_utm30n_osgb_vmsk_sharp_rad_srefdem_stdsref"
        ]

    layergroup_id : integer
        EODS ID of the layer group, eg from: https://earthobs.defra.gov.uk/layer_groups/<layergroup_id>

    abstract : str, optional
        specify the modified abstract of the layer group, to overwrite if required

    Returns
    -------
    json response from layergroup api
    
    """

    # validation checks
    if not isinstance(list_of_layers, list):
        raise TypeError('ERROR. list_of_layers is not a list, aborting ...')

    if not isinstance(layergroup_id, int):
        raise TypeError('ERROR. layer group ID is not an integer, aborting ...')

    if len(list_of_layers) == 0 :
        raise ValueError('ERROR. list_of_layers is empty, aborting ...')
    
    url = f'{conn["domain"]}/api/layer_groups/{layergroup_id}/'
    
    the_json = {'abstract':abstract, 'layers':list_of_layers}
    
    response_json = post_to_layer_group_api(conn, url, the_json)
    
    return response_json
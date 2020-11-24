"""
module of library functions for programatic interaction with Defra's Earth 
Observation Data Service (EODS)

    AUTHOR          : Sam Franklin, CGI
    LAST UPDATED    : 2020-11-16
"""

# module-wide TODOs:
# TODO: update doc strings on all functions
# TODO: rename functions and reorder
# TODO: review imports and initial settings
# TODO: add new eods query function to use the safe granule list to filter out edge granules

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
import config
import shutil
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
                    Path(f_path).rename(source_file_to_extract.parent.parent / str(execution_dict['filename_stub'] + f_path.suffix.lower()))

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
    + runs ignore split granules function to filter out split granules
    + cross checks the dataframe with a static csv file of 'safe' granule list
    + then groups by the unique granule-reference
    + sorts by cloud cover and takes the lowest cloud value per granule
    + returns a new dataframe
    """

    # handle the case where kwargs are {'find_least_cloud':True,'ignore_split_granules':False,}
    # for this kwarg case, this can return split granules where SPLIT isn't in the title
    # which could generate a mosaic that has split granules and therefore 'holes' in the coverage
    # so run ignore_split_granules function in call cases where 'find_least_cloud=True'
    df = ignore_split_granules(df)

    # import safe granule-orb list
    if Path(Path.cwd() / 'static' / 'safe-granule-orbit-list.txt').exists():
        df_safe_list = pd.read_csv(Path.cwd() / 'static' / 'safe-granule-orbit-list.txt')
    else:
        raise ValueError('ERROR :: safe-granule-orbit-list.txt cannot be found')

    # create new col that matches the granule-orbit syntax
    df['gran-orb'] = df['granule-ref'] + '_' + df['orbit-ref']

    filtered_df = pd.merge(df,df_safe_list)

    if len(filtered_df) > 0:
        return_df = filtered_df.sort_values("ARCSI_CLOUD_COVER").groupby(["granule-ref"], as_index=False).first()        
    else:
        raise ValueError('ERROR : You have selected find_lowest_cloud=True BUT your search criteria is too narrow, spatially or temporally and did not match any granule references in "./static/safe-granule-orbit-list.txt". Suggest widening your search')

    return return_df
    

def ignore_split_granules(df):
    """
    eods query "special" keyword function
    + takes the input dataframe and removes all SPLIT granules.
    + one issue that has to be overcome, if a granule is split by ESA into two:
        then one of the components has the word 'SPLIT' added to the title,
        the other component granule of the split 'pair' does not.
        therefore, this function finds the SPLIT title, then finds the second component
        and removes BOTH component granules from the dataframe
    + returns a new dataframe
    """    

    if len(df[df['title'].str.contains('SPLIT')]) > 0:

        print(datetime.utcnow().isoformat() + ' :: INFO. Split Granules found in query results ... removing')

        # create a title substring which can be matched against a split list
        df['title_stub'] = df['title'].str.split('_T', expand=True).loc[:,0] + '_' + df['granule-ref'].str[:6]

        # create a Series object for that only contains the title 'sub string' 
        s = df[df["title"].str.contains("SPLIT")].title.str.split('SPLIT1',expand=True).loc[:,0]

        # select rows from main dataframe where records DO NOT MATCH (~ = syntax) the 'split' series of title stub strings
        filtered_out_split = df[~df.title_stub.isin(s)]

        return filtered_out_split

    else:
        print(datetime.utcnow().isoformat() + ' :: INFO. No split granules found in query results')
        
        return df

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
    ignore_split_granules: bool, optional:
        custom filter to generate a list of layers that does not contain
        the str "SPLIT" in the title
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

                if 'ignore_split_granules' in kwargs:
                    if kwargs['ignore_split_granules']:
                        df = ignore_split_granules(df)

                if 'find_least_cloud' in kwargs and kwargs['sat_id'] == 2 and kwargs['find_least_cloud']:
                    df = find_minimum_cloud_list(df)

                # make output paths
                path_output = make_output_dir(kwargs['output_dir'])
                log_file_name = path_output / 'eods-query-all-results.csv'
                df.to_csv(log_file_name)                    
            
                output_list = df['alternate'].tolist()

                print('\nMatching Layers:\n')
                for item in output_list:
                    print('\t' + item)
                print('\n' + datetime.utcnow().isoformat() + ' :: NUMBER OF LAYERS RETURNED = ' + str(len(output_list)) + '\n')

            else:
                output_list = []
                df = None
                print('\n' + datetime.utcnow().isoformat() + ' :: QUERY WAS ACCEPTED BUT PARAMETERS USED RETURNED ZERO MATCHING RECORDS, TRY A DIFFERENT SET OF PARAMETERS')

            return output_list, df

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

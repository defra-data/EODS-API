"""
module of library functions for programatic interaction with Defra's Earth 
Observation Data Service (EODS)

    AUTHOR          : Sam Franklin, CGI
    LAST UPDATED    : 2020-09-11
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
os.environ['PROJ_NETWORK'] = 'OFF'

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
    + takes an input dataframe, groups by the granule-reference, 
    + sorts by cloud cover and takes the lowest cloud per granule
    + returns a new dataframe
    """

    df_min_cloud_per_granule = df.sort_values("ARCSI_CLOUD_COVER").groupby(["granule-ref"], as_index=False).first()

    return df_min_cloud_per_granule

def ignore_split_granules(df):

    new_df = df[df["alternate"].str.contains("SPLIT") == False]

    return new_df


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
                    df = ignore_split_granules(df)

                if 'find_least_cloud' in kwargs and kwargs['sat_id'] == 2:
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
            print(datetime.utcnow().isoformat() + ' :: RESPONSE STATUS = ' + str(response.status_code) + ' (NOT SUCCESSFUL)' + str(response.status_code) + ' :: QUERY URL = ' + response.url)

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

def submit_wps_request(request_config, payload):
    """
    function submit the wps POST request
    """

    try:
        response = requests.post(
            request_config['wps_server'],
            params={'access_token':request_config['access_token'],'SERVICE':'WPS','VERSION':'1.0.0','REQUEST':'EXECUTE'},
            data=payload,
            headers=request_config['headers'],
            verify=request_config['verify'])    

        if response.status_code == 200:
            if not response.text.find('ExceptionReport') > 0:
                execution_id = response.text.split('executionId=')[1].split('&')[0]        
                print('\n' + datetime.utcnow().isoformat() + ' :: WPS job ' + execution_id + ' was successfully submitted to ' + response.url)
                print('\n' + datetime.utcnow().isoformat() + ' :: ' + str(response.content))
                return execution_id
            else:
                raise ValueError(datetime.utcnow().isoformat() + ' :: ERROR :: WPS server responded with the following exception :: Response=' + str(response.content))
                return
        else:
            raise ValueError(datetime.utcnow().isoformat() + ' :: ERORR :: WPS submission request response is not 200 (Success) :: Status=' + str(response.status_code))
            return

    except requests.exceptions.RequestException as err:
        raise ValueError(datetime.utcnow().isoformat() + ' :: ERORR :: WPS submission raised a connection ERROR :: Response = ' + str(err))
        return

def query_single_xml_page(page_counter, request_config):
    """
    function to query single page of xml using page_counter argument
    this increments by 10 as there are a maximum of 10 wps records per xml page
    """
    try:
        response = requests.get(
            request_config['wps_server'],
            params={'access_token':request_config['access_token'],'SERVICE':'WPS','VERSION':'1.0.0','REQUEST':'GetExecutions','startIndex':str(page_counter)},
            # params={'access_token':request_config['access_token'],'SERVICE':'WPS','VERSION':'1.0.0','REQUEST':'GetExecutiASDFASDFASDFons','startIndex':str(page_counter)},
            headers=request_config['headers'],
            verify=request_config['verify'],
            timeout=180
            )
    except requests.exceptions.RequestException as err:
        print('\n\t' + datetime.utcnow().isoformat() + ' :: ERORR :: Could not POLL the WPS server. It is possibly offline, try your submission again :: Response = ' + str(err))
        return -1

    # parse the xml to an ordered dict using 3rd party imported module xmltodict
    d = xmltodict.parse(response.content)

    # create a list of wps jobs and
    # deal with a single entry per XML document which yields a different structure to the ordered dict
    if isinstance(d['wps:GetExecutionsResponse']['wps:ExecuteResponse'], dict):
        jobs_list = []
        jobs_list.append(d['wps:GetExecutionsResponse']['wps:ExecuteResponse'])
    elif isinstance(d['wps:GetExecutionsResponse']['wps:ExecuteResponse'],list):
        jobs_list = [value for value in d['wps:GetExecutionsResponse']['wps:ExecuteResponse']]
        
    top_level_keys = list(d['wps:GetExecutionsResponse'].keys())
    
    job_count_for_user = d['wps:GetExecutionsResponse']['@count']    

    return jobs_list, page_counter, top_level_keys, job_count_for_user

def query_all_xml_pages(check_time, request_config):
    """
    function to handle querying multiple pages of xml status jobs
    """
    
    start_index = 0
    matching_of_dicts = []
    for page_counter in range(0,1000,10):
                
        jobs_list, page_counter, top_level_keys, job_count_for_user = query_single_xml_page(page_counter, request_config)

        # add this to a master list
        matching_of_dicts.append(jobs_list)

        # if there is no 'next' attribute, then you're on the last xml page so break out the loop
        if not any('@next' == key for key in top_level_keys):
            break

    num_of_xml_pages = int(page_counter / 10) + 1
    duration_to_parse_xml_sec = round((datetime.utcnow() - check_time).total_seconds(),0)
            
    try:
        response_list = [item['wps:Status'] for sublist in matching_of_dicts for item in sublist]
        return response_list, num_of_xml_pages, duration_to_parse_xml_sec, job_count_for_user
    
    except AppError as error:
        print(error)

def parse_wps_responses_to_pandas_dfs(response_list, check_time, frames, log_file_name, request_config, execution_dict):
    """
    """
    
    # parse dict to pandas dataframe and set the index
    df_temp = pd.DataFrame.from_dict(response_list)
    df = df_temp.set_index('wps:JobID')

    # filter df on current job_ids
    # filter_df  = df[df.index.isin(execution_dict.keys())]
    filter_df = df.loc[df.loc[:].index.isin(execution_dict.keys())]

    # add "check time" as column
    filter_df.loc[:,'check_time'] = check_time.isoformat()

    # append on some extra info to the dataframe using the index
    for index, row in filter_df.iterrows():
        filter_df.loc[index,'layer_name'] = execution_dict[index]['layer_name']
        filter_df.loc[index,'dl_url'] = request_config['wps_server'] + '?access_token=' + request_config['access_token'] + '&SERVICE=WPS&VERSION=1.0.0&REQUEST=GetExecutionResult&EXECUTIONID='  + index + '&outputId=result.' + execution_dict[index]['source_dict']['xml_config']['template_mimetype'].split('/')[-1] + '&mimetype=' + execution_dict[index]['source_dict']['xml_config']['template_mimetype']
        filter_df.loc[index,'dl_boolan'] = execution_dict[index]['source_dict']['dl_bool']
        filter_df.loc[index,'mimetype'] = execution_dict[index]['source_dict']['xml_config']['template_mimetype']
        
    # sort out the indices and concatenate dataframes together and output to csv
    filter_df.reset_index(inplace=True)
    filter_df.set_index(['wps:JobID','check_time'],inplace=True)
    frames.append(filter_df)
    rolling_merged_df = pd.concat(frames)
    rolling_merged_df.to_csv(log_file_name)
    
    return filter_df, rolling_merged_df, frames

def process_wps_downloaded_files(dl_path, filename_stub, file_extension):
    """
    function to rename downloaded file if TIFF or extract from zip 
    """
    
    input_path = Path(dl_path / str(filename_stub + file_extension))
        
    # if tif, skip extraction and rename the tif
    if file_extension == '.tif' or file_extension == '.tiff':    
        dl_file_renamed = Path(input_path).rename(dl_path / str(filename_stub + '.tif'))
        print(datetime.utcnow().isoformat() + ' :: FILE=' + str(dl_file_renamed) + ' :: RENAME COMPLETE')
        return dl_file_renamed

    elif file_extension == '.zip':
        zip_ref = ZipFile(input_path, 'r')
        zip_ref.extractall(dl_path)
        zip_list = zip_ref.filelist
        zip_ref.close()
        
        # delete the source zip
        if input_path.exists():
            input_path.unlink()
        
        # loop through zip contents
        for f in zip_list:
        
            f_path = Path(dl_path / f.filename)
        
            # if zip, call function again to extract
            if f_path.suffix == '.zip':            
                print(datetime.utcnow().isoformat() + ' :: FILE=' + str(f_path) + ' :: RUN EXTRACT ARCHIVE AGAIN')
                dl_file_renamed = process_wps_downloaded_files(dl_path, f_path.stem, f_path.suffix)

            # if tif rename tiff from uuid to actual filename
            elif f_path.suffix == ".tif" or f_path.suffix == ".tiff":
                dl_file_renamed = Path(f_path).rename(dl_path / str(filename_stub + '.tif'))
                print(datetime.utcnow().isoformat() + ' :: FILE=' + str(dl_file_renamed) + ' :: RENAME TIFF COMPLETE')
                
            # if something else, like a shape file, this should have the original name
            else:
                print(datetime.utcnow().isoformat() + ' :: FILE=' + str(f_path) + ' :: NO PROCESSING ON THIS FILETYPE REQUIRED')
                dl_file_renamed = f_path
            
        return dl_file_renamed
        
    else:
        raise ValueError("ERROR - some unknown file has been given:", f)
        
def download_wps_result(index, row, path_output, request_config):
    """
    function to get a wps result if the response is SUCCEEDED AND download is set to True in the config
    """
    
    print('\n' + datetime.utcnow().isoformat() + ' :: DOWNLOAD START :: URL = ' + row['dl_url'])

    try:
        # download the wps result
        response = requests.get(
            row['dl_url'],
            headers=request_config['headers'],
            verify=request_config['verify'])

        print(datetime.utcnow().isoformat() + ' :: DOWNLOAD COMPLETE')

        # write the download to a file
        file_extension = '.' + row['mimetype'].split('/')[1]
        dl_path = path_output / str(row['wps:Identifier'].split(':')[0] + row['wps:Identifier'].split(':')[1] + '-' + index[0])
        dl_path.mkdir(parents=True, exist_ok=True)
        filename_stub = row['layer_name'].split(':')[-1]
        local_file_name = Path(dl_path / str(filename_stub + file_extension))
        print(datetime.utcnow().isoformat() + ' :: FILE WRITE START :: FILE = ' + str(local_file_name))
        with open(local_file_name, 'wb') as f:
            f.write(response.content);
            f.close();        
        print(datetime.utcnow().isoformat() + ' :: FILE WRITE COMPLETE')
        print(datetime.utcnow().isoformat() + ' :: EXTRACT ARCHIVE FILE STARTED')

        # rename download and extract if it is a zip
        download_path = process_wps_downloaded_files(dl_path, filename_stub, file_extension)

        return download_path

    except requests.exceptions.RequestException as err:
        print('\n' + datetime.utcnow().isoformat() + ' :: ERROR, an Exception was raised, no list returned :: reponse is :' + str(err))
        return
        
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

    # validate that incoming wps config 
    try:
        if isinstance(config_wpsprocess, dict) and config_wpsprocess:
            new_list = []
            new_list.append(config_wpsprocess)
            config_wpsprocess = new_list
        elif len(config_wpsprocess) == 0:
            raise ValueError('\n\t ERROR :: the config_wpsprocess arg is empty, check your config_wpsprocess arg')    
    except ValueError as err:
        print(err)
        return -1

    # set output path if not specified
    if 'output_dir' not in kwargs:
        kwargs['output_dir']=Path.cwd()

    if 'verify' not in kwargs:
        kwargs['verify'] = True

    # INITIALISE VARIABLES
    path_output = make_output_dir(kwargs['output_dir'])

    request_config = {
        'wps_server':conn['domain'] + '/geoserver/ows',
        'access_token':conn['access_token'],
        'headers':{'Content-type': 'application/xml','User-Agent': 'python'},
        'verify':kwargs['verify']
    }

    poll_frequency_sec = 60
    execution_dict={}
    frames = []

    # STEP 1. SUBMIT the WPS request(s) by loop through the wps request INPUT dictionary   
    for item in config_wpsprocess:

        lyr = item['xml_config']['template_layer_name']
        print('\n' + datetime.utcnow().isoformat() + ' :: WPS SUNMISSION :: lyr=' + lyr + ', download=' + str(item['dl_bool']))

        try:
            modded_xml = mod_the_xml(item)
            execution_id = submit_wps_request(request_config, modded_xml)
        except ValueError as error:
            print(error)
            return

        execution_dict.update({execution_id :{'layer_name':lyr,'source_dict':item}})                

    print('\n' + datetime.utcnow().isoformat() + ' :: WPS SUBNMISSION :: A TOTAL OF ' + str(len(execution_dict)) +  ' REQUESTS WERE SUBMITTED')
    print(datetime.utcnow().isoformat() + ' :: WPS SUBNMISSION :: THE EXECUTION IDS =' + str(execution_dict.keys()))
    print(datetime.utcnow().isoformat() + ' :: WPS SUBNMISSION :: WPS GETEXCUTIONS URL IS ' + request_config['wps_server'] + '?SERVICE=WPS&VERSION=1.0.0&REQUEST=GetExecutions&access_token=' + request_config['access_token'])

    # create logging file directory    
    log_file_name = path_output / 'wps-running-log.csv'

    # STEP 2. POLL THE WPS GETEXECUTIONS PAGE for 1 hour until all jobs status is NOT 'STATUS=RUNNING' and break the loop
    for i in range(1,3600):

        check_time = datetime.utcnow()
        if i == 1:
            print('\n\n' + datetime.utcnow().isoformat() + ' :: POLLING WPS JOB STATUS LOGGING TO THIS FILE = ' + str(log_file_name))
        print('\n' + datetime.utcnow().isoformat() + ' :: POLLING WPS JOB STATUS TRY #' + format(i, '03'))

        # call functions to parse xml to a clean response list ready for pandas
        response_list, num_of_xml_pages, duration_to_parse_xml_sec, job_count_for_user = query_all_xml_pages(check_time, request_config)
        
        print(datetime.utcnow().isoformat() + ' :: POLLING WPS JOB STATUS TRY #' + format(i, '03') + ' :: RESPONSE RECEIVED in ' + str(duration_to_parse_xml_sec) + 's, wps jobs=' + str(job_count_for_user) + ', xml pages=' + str(num_of_xml_pages))
    
        # parse response list to pandas dfs
        filter_df, rolling_merged_df, frames = parse_wps_responses_to_pandas_dfs(
            response_list,
            check_time,
            frames,
            log_file_name,
            request_config,
            execution_dict)

        # if NO running processes, then break, otherwise loop again
        if not any(filter_df['wps:Status'] == 'RUNNING'):
            break
        print(datetime.utcnow().isoformat() + ' :: POLLING WPS JOB STATUS TRY #' + format(i, '03') + ' :: Jobs are still "STATUS=RUNNING"...script will poll again in ' + str(poll_frequency_sec) + 'sec time ...')

        time.sleep(poll_frequency_sec)

    # export summary wps jobs to CSV
    sumdf = rolling_merged_df.reset_index()
    grouped = sumdf.groupby(['wps:JobID','wps:Status']).first()
    grouped.to_csv(path_output / 'wps-summary-log.csv')
    
    print('\n' + datetime.utcnow().isoformat() + ' :: POLLING WPS JOB STATUS TRY #' + format(i, '03') + ' :: ALL WPS Jobs are either SUCCEEDED or FAILED, moving on to downloading the results')
    
    # STEP 3. loop through final dataframe and call the download function
    if any(filter_df['dl_boolan'].tolist()):
        list_download_paths = []
    
        for index, row in filter_df.iterrows():
            if row['wps:Status'] == 'SUCCEEDED' and row['dl_boolan']:
                download_path = download_wps_result(index, row, path_output, request_config)
                list_download_paths.append(download_path)
            elif row['wps:Status'] == 'FAILED':
                print(datetime.utcnow().isoformat() + ' skipping downloading for JobID=' + index[0] + ' :: JobStatus=FAILED' + ' :: WPS ERROR MESSAGE ' + str(row['wps:ProcessFailed']))
                print(datetime.utcnow().isoformat() + ' check the local log file for more info')
            else:
                print(datetime.utcnow().isoformat() + ' some other issue encountered, check the local log file for more info')
        return list_download_paths
    else:
        print(datetime.utcnow().isoformat() + ' :: NO WPS Jobs set to download, skipping downloading')
        return
        
    print('\n\n' + datetime.utcnow().isoformat() + ' :: #### WPS PROCESSING COMPLETE')

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


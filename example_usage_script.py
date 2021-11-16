#!/usr/bin/env python

import eodslib
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
import os

if __name__ == "__main__":

    start_time = datetime.utcnow()

    # USER MUST EDIT THE ENVIRONMENT FILE REFERENCED BELOW, OR CREATE THEIR OWN FILE AND REFERENCE IT
    load_dotenv('sample.env')

    # set configuration based on contents of the ENVIRONMENT FILE.
    conn = {
        'domain': os.getenv("HOST"),
        'username': os.getenv("API_USER"),
        'access_token': os.getenv("API_TOKEN"),
    }

    # use default path to local "output" directory
    output_dir = eodslib.make_output_dir(Path.cwd() / 'output')

    # specify a particular ARD to download using 'title' keyword
    eods_params = {
        'output_dir':output_dir,
        'find_least_cloud': True,
        'sat_id': 2
        }

    list_of_layers, df = eodslib.query_catalog(conn, **eods_params)

    # list_of_results = []

    """for lyr in list_of_layers:

        config_wpsprocess = {'template_xml':'gsdownload_template.xml',
            'xml_config':{
                'template_layer_name':lyr,
                'template_outputformat':'image/tiff',
                'template_mimetype':'application/zip'
                    },
            'dl_bool':True
        }

        execution_dict = eodslib.run_wps(conn, config_wpsprocess, output_dir=output_dir)"""

        #list_of_results.append(execution_dict)

    #eodslib.output_log(list_of_results)     

    time_diff_mins = round((datetime.utcnow() - start_time).total_seconds() / 60,2)
    print('\n\t### Total processing time (mins) = ' + str(time_diff_mins))
    print('\t### Script finished')
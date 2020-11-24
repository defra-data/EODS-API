#!/usr/bin/env python

import eodslib
from datetime import datetime
from pathlib import Path
import config

if __name__ == "__main__":

    start_time = datetime.utcnow()

    # set these variables in your config.py
    output_dir = config.output_dir
    conn = config.conn

    # specify a particular ARD to download using 'title' keyword
    eods_params = {
        'output_dir':output_dir,
        'title':'S2A_20200506_lat50lon223_T30UWA_ORB137_utm30n_osgb_vmsk_sharp_rad_srefdem_stdsref',
        }

    list_of_layers, df = eodslib.query_catalog(conn, **eods_params)

    list_of_results = []

    for lyr in list_of_layers:

        config_wpsprocess = {'template_xml':'gsdownload_template.xml',
            'xml_config':{
                'template_layer_name':lyr,
                'template_outputformat':'image/tiff',
                'template_mimetype':'application/zip'
                    },
            'dl_bool':True
        }

        execution_dict = eodslib.run_wps(conn, config_wpsprocess, output_dir=output_dir)

        list_of_results.append(execution_dict)

    eodslib.output_log(list_of_results)     

    time_diff_mins = round((datetime.utcnow() - start_time).total_seconds() / 60,2)
    print('\n\t### Total processing time (mins) = ' + str(time_diff_mins))
    print('\t### Script finished')
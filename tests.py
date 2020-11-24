#!/usr/bin/env python

import eodslib
from datetime import datetime
from pathlib import Path
import config
import argparse

if __name__ == "__main__":

    app_parser = argparse.ArgumentParser(description='Arg Pass')
    app_parser.add_argument('--test', type=int, default=1)
    args = app_parser.parse_args()
    start_time = datetime.utcnow()

    # configure the "conn" dictionary in the config.py file
    output_dir = config.output_dir
    conn = config.conn

    if args.test == 1:

        print('\n\t INFO :: TEST1 :: SINGLE WPS DOWNLOAD OF SMALL UPLOAD FILES')

        eods_params = {
            'output_dir':output_dir,
            'title':'_1_1',
            'type':'raster',    
            'limit':2,
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

    if args.test == 2:

        print('\n\t INFO :: TEST2 :: WPS DOWNLOAD TEST OF SMALL "EDGE" S2 GRANULES')

        eods_params = {
            'output_dir':output_dir,
            'title':'T30UXG_ORB080',
            'type':'raster',    
            'limit':2,
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

    if args.test == 3:

        print('\n\t INFO :: TEST3 :: QUERY OF EODS TO RETURN A CLOUD FREE, SPLIT GRANULE FREE LIST OF S2 GRANULES AND SEQUENTIALLY DOWNLOAD THEM')

        eods_params = {
            'output_dir':output_dir,
            'type':'raster',
            'sat_id':2,
            'date_start':'2020-05-01',
            'date_end':'2020-06-30',
            'find_least_cloud':True,
            'ignore_split_granules':True,
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

    if args.test == 4:

        print('\n\t INFO :: TEST4 :: QUERY OF EODS TO RETURN A SINGLE VECTOR AND WPS DOWNLOAD IT')

        eods_params = {
            'output_dir':output_dir,
            'title':'_1_1',
            'type':'vector',    
            'limit':1,
            }

        list_of_layers, df = eodslib.query_catalog(conn, **eods_params)

        list_of_results = []
        
        for lyr in list_of_layers:

            config_wpsprocess = {'template_xml':'gsdownload_template.xml',
                'xml_config':{
                    'template_layer_name':lyr,
                    'template_outputformat':'application/zip',
                    'template_mimetype':'application/zip'
                        },
                'dl_bool':True
            }
        
            execution_dict = eodslib.run_wps(conn, config_wpsprocess, output_dir=output_dir)

            list_of_results.append(execution_dict)

        eodslib.output_log(list_of_results)             

    if args.test == 5:

        print('\n\t INFO :: TEST5 :: QUERY OF EODS USING GEOM TO RETURN SINGLE GRANULE, THEN WPS RASTER CROP THE DOWNLOADS')

        cutting_geom_osgbwkt = 'POLYGON((455556 114292, 455556 106403, 467913 106403, 467913 114292, 455556 114292))'
        cat_search_geom_wgs84wkt_query = 'POLYGON((-1.18 50.94, -1.18 50.86, -1.06 50.86, -1.06 50.94, -1.18 50.94))'

        eods_params_ras_crop_demo = {
            'output_dir':output_dir,
            'start_date':'2020-01-01',
            'end_date':'2020-09-01',
            'title':'ORB137',
            'geom':cat_search_geom_wgs84wkt_query,
            'cloud_min':0,
            'cloud_max':20,
            'sat_id':2,
            'limit':1,
            }

        list_of_results = []

        for lyr in list_of_layers:
            
            wkt = df.loc[(df['alternate'] == lyr)]['csw_wkt_geometry'].item()
            lower_left, upper_right = eodslib.get_bbox_corners_from_wkt(wkt,27700)

            config_wpsprocess = {'template_xml':'rascropcoverage_template.xml',
                    'xml_config':{
                        'template_layer_name':lyr,
                        'template_mimetype':'image/tiff',
                        'template_ll':str(lower_left.x) + ' ' + str(lower_left.y),
                        'template_ur':str(upper_right.x) + ' ' + str(upper_right.y),
                        'template_clip_geom':cutting_geom_osgbwkt
                        },
                    'dl_bool':True
                    }

            execution_dict = eodslib.run_wps(conn, config_wpsprocess, output_dir=output_dir)

            list_of_results.append(execution_dict)

        eodslib.output_log(list_of_results)      

        
    if args.test == 6:

        print('\n\t INFO :: TEST6 :: QUERY OF EODS FOR 25 S1s AND WPS DOWNLOAD THEM')

        eods_params = {
            'output_dir':output_dir,
            'type':'raster',
            'sat_id':1,
            'date_start':'2020-01-01',
            'date_end':'2020-06-30',
            'limit':25,
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
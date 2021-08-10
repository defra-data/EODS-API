import eodslib
import os
import pytest
import requests
import responses
import shapely
import logging
import pandas as pd
import numpy as np

class TestPostLayerGroupAPI():
    def test_single_layer_handles_json_response(self, mocker):
        self.mock_post = mocker.patch('eodslib.requests.post')
        self.mock_post.return_value.content = bytes(b'{"a":"b"}')

        conn = {
        'domain': os.getenv("HOST"),
        'username': os.getenv("API_USER"),
        'access_token': os.getenv("API_TOKEN"),
        }

        url = f'{conn["domain"]}api/layer_groups/'
        the_json = {'name': 'unittest_group', 'abstract': 'This is a unittested abstract', 'layers': ['keep_api_test_create_group']}

        response_json = eodslib.post_to_layer_group_api(conn, url, the_json, quiet=False)

        mocker.resetall()

        assert response_json == {"a":"b"}

    @responses.activate
    def test_incorrect_status_code_raise_for_status_triggers_exception(self):
        conn = {
        'domain': os.getenv("HOST"),
        'username': os.getenv("API_USER"),
        'access_token': os.getenv("API_TOKEN"),
        }

        url = f'{conn["domain"]}api/layer_groups/'
        the_json = {'name': 'unittest_group', 'abstract': 'This is a unittested abstract', 'layers': ['keep_api_test_create_group']}

        responses.add(responses.POST, url, status=400)

        with pytest.raises(requests.exceptions.HTTPError):
            response_json = eodslib.post_to_layer_group_api(conn, url, the_json, quiet=False)
    
    @pytest.mark.skip_real()
    def test_incorrect_access_token_triggers_exception(self):
        conn = {
        'domain': os.getenv("HOST"),
        'username': os.getenv("API_USER"),
        'access_token': "badtoken",
        }

        url = f'{conn["domain"]}api/layer_groups/'
        the_json = {'name': 'unittest_group', 'abstract': 'This is a unittested abstract', 'layers': ['keep_api_test_create_group']}

        with pytest.raises(requests.exceptions.HTTPError):
            response_json = eodslib.post_to_layer_group_api(conn, url, the_json, quiet=False)

    @pytest.mark.skip_real()
    def test_incorrect_username_triggers_exception(self):
        conn = {
        'domain': os.getenv("HOST"),
        'username': "baduser",
        'access_token': os.getenv("API_TOKEN"),
        }

        url = f'{conn["domain"]}api/layer_groups/'
        the_json = {'name': 'unittest_group', 'abstract': 'This is a unittested abstract', 'layers': ['keep_api_test_create_group']}

        with pytest.raises(requests.exceptions.HTTPError):
            response_json = eodslib.post_to_layer_group_api(conn, url, the_json, quiet=False)

    def test_single_layer_quiet_handles_json_response(self, mocker):
        self.mock_post = mocker.patch('eodslib.requests.post')
        self.mock_post.return_value.content = bytes(b'{"a":"b"}')

        conn = {
        'domain': os.getenv("HOST"),
        'username': os.getenv("API_USER"),
        'access_token': os.getenv("API_TOKEN"),
        }

        url = f'{conn["domain"]}api/layer_groups/'
        the_json = {'name': 'unittest_group', 'abstract': 'This is a unittested abstract', 'layers': ['keep_api_test_create_group']}

        response_json = eodslib.post_to_layer_group_api(conn, url, the_json, quiet=True)

        mocker.resetall()

        assert response_json == {"a":"b"}

    @responses.activate
    def test_incorrect_status_code_raise_for_status_quiet_error_stdout_call(self, capsys):
        conn = {
        'domain': os.getenv("HOST"),
        'username': os.getenv("API_USER"),
        'access_token': os.getenv("API_TOKEN"),
        }

        url = f'{conn["domain"]}api/layer_groups/'
        the_json = {'name': 'unittest_group', 'abstract': 'This is a unittested abstract', 'layers': ['keep_api_test_create_group']}

        responses.add(responses.POST, url, status=400)

        response_json = eodslib.post_to_layer_group_api(conn, url, the_json, quiet=True)

        captured = capsys.readouterr()
        captured_list = captured.out.split('\n')
        captured_error_message_list = captured_list[1].split(':')
        captured_error_message_list_stub = ':'.join([captured_error_message_list[0], captured_error_message_list[1]])
        captured_stub = ' '.join([captured_list[0], captured_error_message_list_stub])
        error_message = 'Error caught as exception 400 Client Error: Bad Request for url'
        assert captured_stub == error_message
    
    @pytest.mark.skip_real()
    def test_incorrect_access_token_quiet_error_stdout_call(self, capsys):
        conn = {
        'domain': os.getenv("HOST"),
        'username': os.getenv("API_USER"),
        'access_token': "badtoken",
        }

        url = f'{conn["domain"]}api/layer_groups/'
        the_json = {'name': 'unittest_group', 'abstract': 'This is a unittested abstract', 'layers': ['keep_api_test_create_group']}

        response_json = eodslib.post_to_layer_group_api(conn, url, the_json, quiet=True)

        captured = capsys.readouterr()
        captured_list = captured.out.split('\n')
        captured_error_message_list = captured_list[1].split(':')
        captured_error_message_list_stub = ':'.join([captured_error_message_list[0], captured_error_message_list[1]])
        captured_stub = ' '.join([captured_list[0], captured_error_message_list_stub])
        error_message = 'Error caught as exception 502 Server Error: Bad Gateway for url'
        assert captured_stub == error_message

    @pytest.mark.skip_real()
    def test_incorrect_username_quiet_error_stdout_call(self, capsys):
        conn = {
        'domain': os.getenv("HOST"),
        'username': "baduser",
        'access_token': os.getenv("API_TOKEN"),
        }

        url = f'{conn["domain"]}api/layer_groups/'
        the_json = {'name': 'unittest_group', 'abstract': 'This is a unittested abstract', 'layers': ['keep_api_test_create_group']}

        response_json = eodslib.post_to_layer_group_api(conn, url, the_json, quiet=True)

        captured = capsys.readouterr()
        captured_list = captured.out.split('\n')
        captured_error_message_list = captured_list[1].split(':')
        captured_error_message_list_stub = ':'.join([captured_error_message_list[0], captured_error_message_list[1]])
        captured_stub = ' '.join([captured_list[0], captured_error_message_list_stub])
        error_message = 'Error caught as exception 502 Server Error: Bad Gateway for url'
        assert captured_stub == error_message

class TestCreateLayerGroup():
    def test_correct_param_input_returns_response_json(self, mocker):
        self.mock_post = mocker.patch('eodslib.post_to_layer_group_api')
        self.mock_post.return_value = 'mock response json'

        conn = {
        'domain': os.getenv("HOST"),
        'username': os.getenv("API_USER"),
        'access_token': os.getenv("API_TOKEN"),
        }

        list_of_layers = ['keep_api_test_create_group']

        response_json = eodslib.create_layer_group(
                                              conn,
                                              list_of_layers,
                                              'eodslib-create-layer-test-',
                                              abstract='some description of the layer group '
                                              )

        mocker.resetall()

        assert response_json == 'mock response json'

    def test_list_of_layers_not_list_triggers_exception(self, mocker):
        self.mock_post = mocker.patch('eodslib.post_to_layer_group_api')
        self.mock_post.return_value = 'mock response json'

        conn = {
        'domain': os.getenv("HOST"),
        'username': os.getenv("API_USER"),
        'access_token': os.getenv("API_TOKEN"),
        }

        list_of_layers = 'badstr'

        with pytest.raises(TypeError) as error:
            response_json = eodslib.create_layer_group(
                                                conn,
                                                list_of_layers,
                                                'eodslib-create-layer-test-',
                                                abstract='some description of the layer group '
                                                )
        assert error.value.args[0] == 'ERROR. list_of_layers is not a list, aborting ...'

        mocker.resetall()

    def test_name_not_str_triggers_exception(self, mocker):
        self.mock_post = mocker.patch('eodslib.post_to_layer_group_api')
        self.mock_post.return_value = 'mock response json'

        conn = {
        'domain': os.getenv("HOST"),
        'username': os.getenv("API_USER"),
        'access_token': os.getenv("API_TOKEN"),
        }

        list_of_layers = ['keep_api_test_create_group']

        with pytest.raises(TypeError) as error:
            response_json = eodslib.create_layer_group(
                                                conn,
                                                list_of_layers,
                                                ['eodslib-create-layer-test-'],
                                                abstract='some description of the layer group '
                                                )
        assert error.value.args[0] == 'ERROR. layer group name is not a string, aborting ...'

        mocker.resetall()

    def test_list_of_layers_empty_triggers_exception(self, mocker):
        self.mock_post = mocker.patch('eodslib.post_to_layer_group_api')
        self.mock_post.return_value = 'mock response json'

        conn = {
        'domain': os.getenv("HOST"),
        'username': os.getenv("API_USER"),
        'access_token': os.getenv("API_TOKEN"),
        }

        list_of_layers = []

        with pytest.raises(ValueError) as error:
            response_json = eodslib.create_layer_group(
                                                conn,
                                                list_of_layers,
                                                'eodslib-create-layer-test-',
                                                abstract='some description of the layer group '
                                                )
        assert error.value.args[0] == 'ERROR. list_of_layers is empty, aborting ...'

        mocker.resetall()

    def test_name_empty_triggers_exception(self, mocker):
        self.mock_post = mocker.patch('eodslib.post_to_layer_group_api')
        self.mock_post.return_value = 'mock response json'

        conn = {
        'domain': os.getenv("HOST"),
        'username': os.getenv("API_USER"),
        'access_token': os.getenv("API_TOKEN"),
        }

        list_of_layers = ['keep_api_test_create_group']

        with pytest.raises(ValueError) as error:
            response_json = eodslib.create_layer_group(
                                                conn,
                                                list_of_layers,
                                                '',
                                                abstract='some description of the layer group '
                                                )
        assert error.value.args[0] == 'ERROR. layer group name string is empty, aborting ...'

        mocker.resetall()


class TestModifyLayerGroup():
    def test_correct_param_input_returns_response_json(self, mocker):
        self.mock_post = mocker.patch('eodslib.post_to_layer_group_api')
        self.mock_post.return_value = 'mock response json'

        conn = {
        'domain': os.getenv("HOST"),
        'username': os.getenv("API_USER"),
        'access_token': os.getenv("API_TOKEN"),
        }

        list_of_layers = ['keep_api_test_create_group']

        response_json = eodslib.modify_layer_group(
                                                  conn,
                                                  list_of_layers,
                                                  0,
                                                  abstract='update the abstract '
                                                  )
        mocker.resetall()

        assert response_json == 'mock response json'

    def test_list_of_layers_not_list_triggers_exception(self, mocker):
        self.mock_post = mocker.patch('eodslib.post_to_layer_group_api')
        self.mock_post.return_value = 'mock response json'

        conn = {
        'domain': os.getenv("HOST"),
        'username': os.getenv("API_USER"),
        'access_token': os.getenv("API_TOKEN"),
        }

        list_of_layers = 'badstr'

        with pytest.raises(TypeError) as error:
            response_json = eodslib.modify_layer_group(
                                                      conn,
                                                      list_of_layers,
                                                      0,
                                                      abstract='update the abstract '
                                                      )
        assert error.value.args[0] == 'ERROR. list_of_layers is not a list, aborting ...'

        mocker.resetall()

    def test_id_not_int_triggers_exception(self, mocker):
        self.mock_post = mocker.patch('eodslib.post_to_layer_group_api')
        self.mock_post.return_value = 'mock response json'

        conn = {
        'domain': os.getenv("HOST"),
        'username': os.getenv("API_USER"),
        'access_token': os.getenv("API_TOKEN"),
        }

        list_of_layers = ['keep_api_test_create_group']

        with pytest.raises(TypeError) as error:
            response_json = eodslib.modify_layer_group(
                                                      conn,
                                                      list_of_layers,
                                                      'badID',
                                                      abstract='update the abstract '
                                                      )
        assert error.value.args[0] == 'ERROR. layer group ID is not an integer, aborting ...'

        mocker.resetall()

    def test_list_of_layers_empty_triggers_exception(self, mocker):
        self.mock_post = mocker.patch('eodslib.post_to_layer_group_api')
        self.mock_post.return_value = 'mock response json'

        conn = {
        'domain': os.getenv("HOST"),
        'username': os.getenv("API_USER"),
        'access_token': os.getenv("API_TOKEN"),
        }

        list_of_layers = []

        with pytest.raises(ValueError) as error:
            response_json = eodslib.modify_layer_group(
                                                      conn,
                                                      list_of_layers,
                                                      0,
                                                      abstract='update the abstract '
                                                      )
        assert error.value.args[0] == 'ERROR. list_of_layers is empty, aborting ...'
        
        mocker.resetall()

class TestGetBboxCornersFromWkt():
    def test_epsg_4326_input_correct_27700_transformation(self):
        test_polygon = 'POLYGON((-2.4591467333 51.7495497809,-2.4591467333 51.8218717504,-2.34253580452 51.8218717504,-2.34253580452 51.7495497809,-2.4591467333 51.7495497809))'
        ll, ur = eodslib.get_bbox_corners_from_wkt(test_polygon, 27700)
        expected_ll, expected_ur = shapely.geometry.Point(368399.6025602297, 205750.31733226206), shapely.geometry.Point(376487.60710743662, 213749.82323291147)

        assert list(ll.coords) == list(expected_ll.coords) and list(ur.coords) == list(expected_ur.coords)

    def test_polygon_not_str_triggers_exception(self):
        test_polygon = 5

        with pytest.raises(TypeError) as error:
            ll, ur = eodslib.get_bbox_corners_from_wkt(test_polygon, 27700)
        
        assert error.value.args[0] == 'Only str is accepted.'
        

    # add more tests for different types of invalid test_polygons? 
    def test_polygon_invalid_str_triggers_exception(self):
        # Get captured error log call message even on pass and unsure why so turned off loggingfor this one test
        # ERROR    shapely.geos:geos.py:252 IllegalArgumentException: Points of LinearRing do not form a closed linestring
        # This appears to be the error message for why the invalid polygon fails
        # Still successfully passes and fails as required so left as is
        logging.disable(logging.CRITICAL)
        test_polygon = 'POLYGON((1 2, 3 4))'

        with pytest.raises(shapely.errors.WKTReadingError) as error:
            ll, ur = eodslib.get_bbox_corners_from_wkt(test_polygon, 27700)

        logging.disable(logging.NOTSET)

        assert error.value.args[0] == 'Could not create geometry because of errors while reading input.'

class TestFindMinimumCloudList():
    def test_components_and_average_less_than_full_granule_return_both_splits(self, mocker):
        # S2A_date_lat1lon2_T12ABC_ORB034_etc = 0.05, S2A_date_lat1lon2_T12ABCSPLIT1_ORB034_etc = 0.1, S2A_date_lat1lon2_T12ABC_ORB034_fullgran = 0.12
        self.mock_path_exists= mocker.patch('eodslib.Path.exists')
        self.mock_path_exists.return_value = True
        self.mock_read_csv= mocker.patch('eodslib.pd.read_csv')
        self.mock_read_csv.return_value = pd.DataFrame(data={'gran-orb': ['T12ABC_ORB034']})
        df = pd.DataFrame(np.array([['S2A_date_lat1lon2_T12ABC_ORB034_etc', 'geonode:S2A_date_lat1lon2_T12ABC_ORB034_etc', 'T12ABC', 'ORB034', '0.05', 'geonode:S2A_date_lat1lon2_T12ABCSPLIT1_ORB034_etc', '0.1', '0.075'],
                                    ['S2A_date_lat1lon2_T12ABCSPLIT1_ORB034_etc', 'geonode:S2A_date_lat1lon2_T12ABCSPLIT1_ORB034_etc', 'T12ABCSPLIT1', 'ORB034', '0.1', 'geonode:S2A_date_lat1lon2_T12ABC_ORB034_etc', '0.05', '0.075'],
                                    ['S2A_date_lat1lon2_T12ABC_ORB034_fullgran', 'geonode:S2A_date_lat1lon2_T12ABC_ORB034_fullgran', 'T12ABC', 'ORB034', '0.12', np.nan, np.nan, '0.12']
                                    ]),
                                    columns=['title', 'alternate', 'granule-ref', 'orbit-ref', 'ARCSI_CLOUD_COVER', 'split_granule.name', 'split_ARCSI_CLOUD_COVER', 'split_cloud_cover'])
        response = eodslib.find_minimum_cloud_list(df)
        expected_response = pd.DataFrame(np.array([['S2A_date_lat1lon2_T12ABC_ORB034_etc', 'geonode:S2A_date_lat1lon2_T12ABC_ORB034_etc', 'T12ABC', 'ORB034', '0.05', 'geonode:S2A_date_lat1lon2_T12ABCSPLIT1_ORB034_etc', '0.1', '0.075', 'S2A_date_lat1lon2_T12ABC', 'T12ABC_ORB034', 'T12ABC'],
                                                   ['S2A_date_lat1lon2_T12ABCSPLIT1_ORB034_etc', 'geonode:S2A_date_lat1lon2_T12ABCSPLIT1_ORB034_etc', 'T12ABCSPLIT1', 'ORB034', '0.1', 'geonode:S2A_date_lat1lon2_T12ABC_ORB034_etc', '0.05', '0.075', 'S2A_date_lat1lon2_T12ABC', 'T12ABC_ORB034', 'T12ABC'],
                                                   ]),
                                                   columns=['title', 'alternate', 'granule-ref', 'orbit-ref', 'ARCSI_CLOUD_COVER', 'split_granule.name', 'split_ARCSI_CLOUD_COVER', 'split_cloud_cover', 'title_stub', 'gran-orb', 'granule-stub'])
        
        response_difference = response.compare(expected_response)
        with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # more options can be specified also
            print('Difference between received and expected responses:')
            print(response_difference)

        assert response_difference.empty

    def test_component_greater_but_average_less_than_full_granule_return_both_splits(self, mocker):
        # S2A_date_lat1lon2_T12ABC_ORB034_etc = 0.2, S2A_date_lat1lon2_T12ABCSPLIT1_ORB034_etc = 0.05, S2A_date_lat1lon2_T12ABC_ORB034_fullgran = 0.19
        self.mock_path_exists= mocker.patch('eodslib.Path.exists')
        self.mock_path_exists.return_value = True
        self.mock_read_csv= mocker.patch('eodslib.pd.read_csv')
        self.mock_read_csv.return_value = pd.DataFrame(data={'gran-orb': ['T12ABC_ORB034']})
        df = pd.DataFrame(np.array([['S2A_date_lat1lon2_T12ABC_ORB034_etc', 'geonode:S2A_date_lat1lon2_T12ABC_ORB034_etc', 'T12ABC', 'ORB034', '0.2', 'geonode:S2A_date_lat1lon2_T12ABCSPLIT1_ORB034_etc', '0.05', '0.125'],
                                    ['S2A_date_lat1lon2_T12ABCSPLIT1_ORB034_etc', 'geonode:S2A_date_lat1lon2_T12ABCSPLIT1_ORB034_etc', 'T12ABCSPLIT1', 'ORB034', '0.05', 'geonode:S2A_date_lat1lon2_T12ABC_ORB034_etc', '0.2', '0.125'],
                                    ['S2A_date_lat1lon2_T12ABC_ORB034_fullgran', 'geonode:S2A_date_lat1lon2_T12ABC_ORB034_fullgran', 'T12ABC', 'ORB034', '0.19', np.nan, np.nan, '0.19']
                                    ]),
                                    columns=['title', 'alternate', 'granule-ref', 'orbit-ref', 'ARCSI_CLOUD_COVER', 'split_granule.name', 'split_ARCSI_CLOUD_COVER', 'split_cloud_cover'])
        response = eodslib.find_minimum_cloud_list(df)
        expected_response = pd.DataFrame(np.array([['S2A_date_lat1lon2_T12ABC_ORB034_etc', 'geonode:S2A_date_lat1lon2_T12ABC_ORB034_etc', 'T12ABC', 'ORB034', '0.2', 'geonode:S2A_date_lat1lon2_T12ABCSPLIT1_ORB034_etc', '0.05', '0.125', 'S2A_date_lat1lon2_T12ABC', 'T12ABC_ORB034', 'T12ABC'],
                                                   ['S2A_date_lat1lon2_T12ABCSPLIT1_ORB034_etc', 'geonode:S2A_date_lat1lon2_T12ABCSPLIT1_ORB034_etc', 'T12ABCSPLIT1', 'ORB034', '0.05', 'geonode:S2A_date_lat1lon2_T12ABC_ORB034_etc', '0.2', '0.125', 'S2A_date_lat1lon2_T12ABC', 'T12ABC_ORB034', 'T12ABC'],
                                                   ]),
                                                   columns=['title', 'alternate', 'granule-ref', 'orbit-ref', 'ARCSI_CLOUD_COVER', 'split_granule.name', 'split_ARCSI_CLOUD_COVER', 'split_cloud_cover', 'title_stub', 'gran-orb', 'granule-stub'])
        
        response_difference = response.compare(expected_response, align_axis=0)
        with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # more options can be specified also
            print('Difference between received and expected responses:')
            print(response_difference)

        assert response_difference.empty

    def test_components_and_average_greater_than_full_granule_return_full_gran(self, mocker):
        # S2A_date_lat1lon2_T12ABC_ORB034_etc = 0.15, S2A_date_lat1lon2_T12ABCSPLIT1_ORB034_etc = 0.2, S2A_date_lat1lon2_T12ABC_ORB034_fullgran = 0.1
        self.mock_path_exists= mocker.patch('eodslib.Path.exists')
        self.mock_path_exists.return_value = True
        self.mock_read_csv= mocker.patch('eodslib.pd.read_csv')
        self.mock_read_csv.return_value = pd.DataFrame(data={'gran-orb': ['T12ABC_ORB034']})
        df = pd.DataFrame(np.array([['S2A_date_lat1lon2_T12ABC_ORB034_etc', 'geonode:S2A_date_lat1lon2_T12ABC_ORB034_etc', 'T12ABC', 'ORB034', '0.15', 'geonode:S2A_date_lat1lon2_T12ABCSPLIT1_ORB034_etc', '0.2', '0.175'],
                                    ['S2A_date_lat1lon2_T12ABCSPLIT1_ORB034_etc', 'geonode:S2A_date_lat1lon2_T12ABCSPLIT1_ORB034_etc', 'T12ABCSPLIT1', 'ORB034', '0.2', 'geonode:S2A_date_lat1lon2_T12ABC_ORB034_etc', '0.15', '0.175'],
                                    ['S2A_date_lat1lon2_T12ABC_ORB034_fullgran', 'geonode:S2A_date_lat1lon2_T12ABC_ORB034_fullgran', 'T12ABC', 'ORB034', '0.1', np.nan, np.nan, '0.1']
                                    ]),
                                    columns=['title', 'alternate', 'granule-ref', 'orbit-ref', 'ARCSI_CLOUD_COVER', 'split_granule.name', 'split_ARCSI_CLOUD_COVER', 'split_cloud_cover'])
        response = eodslib.find_minimum_cloud_list(df)
        expected_response = pd.DataFrame(np.array([['S2A_date_lat1lon2_T12ABC_ORB034_fullgran', 'geonode:S2A_date_lat1lon2_T12ABC_ORB034_fullgran', 'T12ABC', 'ORB034', '0.1', np.nan, np.nan, '0.1', 'S2A_date_lat1lon2_T12ABC', 'T12ABC_ORB034', 'T12ABC'],
                                                   ]),
                                                   columns=['title', 'alternate', 'granule-ref', 'orbit-ref', 'ARCSI_CLOUD_COVER', 'split_granule.name', 'split_ARCSI_CLOUD_COVER', 'split_cloud_cover', 'title_stub', 'gran-orb', 'granule-stub'])
        
        response_difference = response.compare(expected_response, align_axis=0)
        with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # more options can be specified also
            print('Difference between received and expected responses:')
            print(response_difference)

        assert response_difference.empty

    def test_no_layers_match_safe_list_trigger_exception(self, mocker):
        self.mock_path_exists= mocker.patch('eodslib.Path.exists')
        self.mock_path_exists.return_value = True
        self.mock_read_csv= mocker.patch('eodslib.pd.read_csv')
        self.mock_read_csv.return_value = pd.DataFrame(data={'gran-orb': ['badref']})
        df = pd.DataFrame(np.array([['S2A_date_lat1lon2_T12ABC_ORB034_etc', 'geonode:S2A_date_lat1lon2_T12ABC_ORB034_etc', 'T12ABC', 'ORB034', '0.15', 'geonode:S2A_date_lat1lon2_T12ABCSPLIT1_ORB034_etc', '0.2', '0.175'],
                                    ['S2A_date_lat1lon2_T12ABCSPLIT1_ORB034_etc', 'geonode:S2A_date_lat1lon2_T12ABCSPLIT1_ORB034_etc', 'T12ABCSPLIT1', 'ORB034', '0.2', 'geonode:S2A_date_lat1lon2_T12ABC_ORB034_etc', '0.15', '0.175'],
                                    ['S2A_date_lat1lon2_T12ABC_ORB034_fullgran', 'geonode:S2A_date_lat1lon2_T12ABC_ORB034_fullgran', 'T12ABC', 'ORB034', '0.1', np.nan, np.nan, '0.1']
                                    ]),
                                    columns=['title', 'alternate', 'granule-ref', 'orbit-ref', 'ARCSI_CLOUD_COVER', 'split_granule.name', 'split_ARCSI_CLOUD_COVER', 'split_cloud_cover'])
        
        with pytest.raises(ValueError) as error:
            response = eodslib.find_minimum_cloud_list(df)
        
        assert error.value.args[0] == 'ERROR : You have selected find_least_cloud=True BUT your search criteria is too narrow, spatially or temporally and did not match any granule references in "./static/safe-granule-orbit-list.txt". Suggest widening your search'

    def test_no_safe_list_found_trigger_exception(self, mocker):
        self.mock_path_exists= mocker.patch('eodslib.Path.exists')
        self.mock_path_exists.return_value = False
        df = pd.DataFrame(np.array([['S2A_date_lat1lon2_T12ABC_ORB034_etc', 'geonode:S2A_date_lat1lon2_T12ABC_ORB034_etc', 'T12ABC', 'ORB034', '0.15', 'geonode:S2A_date_lat1lon2_T12ABCSPLIT1_ORB034_etc', '0.2', '0.175'],
                                    ['S2A_date_lat1lon2_T12ABCSPLIT1_ORB034_etc', 'geonode:S2A_date_lat1lon2_T12ABCSPLIT1_ORB034_etc', 'T12ABCSPLIT1', 'ORB034', '0.2', 'geonode:S2A_date_lat1lon2_T12ABC_ORB034_etc', '0.15', '0.175'],
                                    ['S2A_date_lat1lon2_T12ABC_ORB034_fullgran', 'geonode:S2A_date_lat1lon2_T12ABC_ORB034_fullgran', 'T12ABC', 'ORB034', '0.1', np.nan, np.nan, '0.1']
                                    ]),
                                    columns=['title', 'alternate', 'granule-ref', 'orbit-ref', 'ARCSI_CLOUD_COVER', 'split_granule.name', 'split_ARCSI_CLOUD_COVER', 'split_cloud_cover'])
        
        with pytest.raises(ValueError) as error:
            response = eodslib.find_minimum_cloud_list(df)
        
        assert error.value.args[0] == 'ERROR :: safe-granule-orbit-list.txt cannot be found'


class TestQueryCatalog():
    def test_successful_query_return_correct_list_and_correctly_modified_df_from_json_response(self, setup, mocker):
        self.mock_get = mocker.patch('eodslib.requests.get')
        self.mock_get.return_value.status_code = 200
        self.mock_get.return_value.content = bytes(b'{"title":"S2A_date_lat1lon2_T12ABC_ORB034_etc", "alternate": "geonode:S2A_date_lat1lon2_T12ABC_ORB034_etc",\
                                                      "granule-ref": "T12ABC", "orbit-ref": "ORB034", "ARCSI_CLOUD_COVER": "0.15",\
                                                      "split_granule.name": "geonode:S2A_date_lat1lon2_T12ABCSPLIT1_ORB034_etc", "split_ARCSI_CLOUD_COVER": "0.2", "split_cloud_cover": "0.175"}')

        # todo: account for json_response['meta']['total_count'] > 0: eodslib.py 587

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
            
        output_list, filtered_df = eodslib.query_catalog(conn, **eods_params)

        print(output_list)
        print(filtered_df)
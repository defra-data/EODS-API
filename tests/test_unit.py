from pathlib import Path
import eodslib
import os
import pytest
import requests
import responses
import shapely
import logging
import pandas as pd
import numpy as np
from datetime import datetime


class TestPostLayerGroupAPI():
    def test_single_layer_handles_json_response(self, mocker):
        self.mock_post = mocker.patch('eodslib.requests.post')
        self.mock_post.return_value.content = bytes(b'{"a":"b"}')

        conn = {
            'domain': 'domainname',
            'username': 'username',
            'access_token': 'token',
        }

        url = f'{conn["domain"]}api/layer_groups/'
        the_json = {'name': 'unittest_group', 'abstract': 'This is a unittested abstract', 'layers': [
            'keep_api_test_create_group']}

        response_json = eodslib.post_to_layer_group_api(
            conn, url, the_json, quiet=False)

        mocker.resetall()

        assert response_json == {"a": "b"}

    @responses.activate
    def test_incorrect_status_code_raise_for_status_triggers_exception(self):
        conn = {
            'domain': 'https://domain',
            'username': 'username',
            'access_token': 'token',
        }

        url = f'{conn["domain"]}api/layer_groups/'
        the_json = {'name': 'unittest_group', 'abstract': 'This is a unittested abstract', 'layers': [
            'keep_api_test_create_group']}

        responses.add(responses.POST, url, status=400)

        with pytest.raises(requests.exceptions.HTTPError):
            eodslib.post_to_layer_group_api(conn, url, the_json, quiet=False)

    @pytest.mark.skip_real()
    def test_incorrect_access_token_triggers_exception(self):
        conn = {
            'domain': os.getenv("HOST"),
            'username': os.getenv("API_USER"),
            'access_token': "badtoken",
        }

        url = f'{conn["domain"]}api/layer_groups/'
        the_json = {'name': 'unittest_group', 'abstract': 'This is a unittested abstract', 'layers': [
            'keep_api_test_create_group']}

        with pytest.raises(requests.exceptions.HTTPError):
            eodslib.post_to_layer_group_api(conn, url, the_json, quiet=False)

    @pytest.mark.skip_real()
    def test_incorrect_username_triggers_exception(self):
        conn = {
            'domain': os.getenv("HOST"),
            'username': "baduser",
            'access_token': os.getenv("API_TOKEN"),
        }

        url = f'{conn["domain"]}api/layer_groups/'
        the_json = {'name': 'unittest_group', 'abstract': 'This is a unittested abstract', 'layers': [
            'keep_api_test_create_group']}

        with pytest.raises(requests.exceptions.HTTPError):
            eodslib.post_to_layer_group_api(conn, url, the_json, quiet=False)

    def test_single_layer_quiet_handles_json_response(self, mocker):
        self.mock_post = mocker.patch('eodslib.requests.post')
        self.mock_post.return_value.content = bytes(b'{"a":"b"}')

        conn = {
            'domain': 'domainname',
            'username': 'username',
            'access_token': 'token',
        }

        url = f'{conn["domain"]}api/layer_groups/'
        the_json = {'name': 'unittest_group', 'abstract': 'This is a unittested abstract', 'layers': [
            'keep_api_test_create_group']}

        response_json = eodslib.post_to_layer_group_api(
            conn, url, the_json, quiet=True)

        mocker.resetall()

        assert response_json == {"a": "b"}

    @responses.activate
    def test_incorrect_status_code_raise_for_status_quiet_error_stdout_call(self, capsys):
        conn = {
            'domain': 'https://domain',
            'username': 'username',
            'access_token': 'token',
        }

        url = f'{conn["domain"]}api/layer_groups/'
        the_json = {'name': 'unittest_group', 'abstract': 'This is a unittested abstract', 'layers': [
            'keep_api_test_create_group']}

        responses.add(responses.POST, url, status=400)

        eodslib.post_to_layer_group_api(conn, url, the_json, quiet=True)

        captured = capsys.readouterr()
        captured_list = captured.out.split('\n')
        captured_error_message_list = captured_list[1].split(':')
        captured_error_message_list_stub = ':'.join(
            [captured_error_message_list[0], captured_error_message_list[1]])
        captured_stub = ' '.join(
            [captured_list[0], captured_error_message_list_stub])
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
        the_json = {'name': 'unittest_group', 'abstract': 'This is a unittested abstract', 'layers': [
            'keep_api_test_create_group']}

        eodslib.post_to_layer_group_api(conn, url, the_json, quiet=True)

        captured = capsys.readouterr()
        captured_list = captured.out.split('\n')
        captured_error_message_list = captured_list[1].split(':')
        captured_error_message_list_stub = ':'.join(
            [captured_error_message_list[0], captured_error_message_list[1]])
        captured_stub = ' '.join(
            [captured_list[0], captured_error_message_list_stub])
        error_message = 'Error caught as exception 401 Client Error: Unauthorized for url'
        assert captured_stub == error_message

    @pytest.mark.skip_real()
    def test_incorrect_username_quiet_error_stdout_call(self, capsys):
        conn = {
            'domain': os.getenv("HOST"),
            'username': "baduser",
            'access_token': os.getenv("API_TOKEN"),
        }

        url = f'{conn["domain"]}api/layer_groups/'
        the_json = {'name': 'unittest_group', 'abstract': 'This is a unittested abstract', 'layers': [
            'keep_api_test_create_group']}

        eodslib.post_to_layer_group_api(conn, url, the_json, quiet=True)

        captured = capsys.readouterr()
        captured_list = captured.out.split('\n')
        captured_error_message_list = captured_list[1].split(':')
        captured_error_message_list_stub = ':'.join(
            [captured_error_message_list[0], captured_error_message_list[1]])
        captured_stub = ' '.join(
            [captured_list[0], captured_error_message_list_stub])
        error_message = 'Error caught as exception 401 Client Error: Unauthorized for url'
        assert captured_stub == error_message


class TestCreateLayerGroup():
    def test_correct_param_input_returns_response_json(self, mocker):
        self.mock_post = mocker.patch('eodslib.post_to_layer_group_api')
        self.mock_post.return_value = 'mock response json'

        conn = {
            'domain': 'domainname',
            'username': 'username',
            'access_token': 'token',
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
            'domain': 'domainname',
            'username': 'username',
            'access_token': 'token',
        }

        list_of_layers = 'badstr'

        with pytest.raises(TypeError) as error:
            eodslib.create_layer_group(
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
            'domain': 'domainname',
            'username': 'username',
            'access_token': 'token',
        }

        list_of_layers = ['keep_api_test_create_group']

        with pytest.raises(TypeError) as error:
            eodslib.create_layer_group(
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
            'domain': 'domainname',
            'username': 'username',
            'access_token': 'token',
        }

        list_of_layers = []

        with pytest.raises(ValueError) as error:
            eodslib.create_layer_group(
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
            'domain': 'domainname',
            'username': 'username',
            'access_token': 'token',
        }

        list_of_layers = ['keep_api_test_create_group']

        with pytest.raises(ValueError) as error:
            eodslib.create_layer_group(
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
            'domain': 'domainname',
            'username': 'username',
            'access_token': 'token',
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
            'domain': 'domainname',
            'username': 'username',
            'access_token': 'token',
        }

        list_of_layers = 'badstr'

        with pytest.raises(TypeError) as error:
            eodslib.modify_layer_group(
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
            'domain': 'domainname',
            'username': 'username',
            'access_token': 'token',
        }

        list_of_layers = ['keep_api_test_create_group']

        with pytest.raises(TypeError) as error:
            eodslib.modify_layer_group(
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
            'domain': 'domainname',
            'username': 'username',
            'access_token': 'token',
        }

        list_of_layers = []

        with pytest.raises(ValueError) as error:
            eodslib.modify_layer_group(
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
        expected_ll, expected_ur = shapely.geometry.Point(
            368399.6025602297, 205750.31733226206), shapely.geometry.Point(376487.60710743662, 213749.82323291147)

        assert list(ll.coords) == list(expected_ll.coords) and list(
            ur.coords) == list(expected_ur.coords)

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
        self.mock_path_exists = mocker.patch('eodslib.Path.exists')
        self.mock_path_exists.return_value = True
        self.mock_read_csv = mocker.patch('eodslib.pd.read_csv')
        self.mock_read_csv.return_value = pd.DataFrame(
            data={'gran-orb': ['T12ABC_ORB034']})
        df = pd.DataFrame(np.array([['S2A_date_lat1lon2_T12ABC_ORB034_etc', 'geonode:S2A_date_lat1lon2_T12ABC_ORB034_etc', 'T12ABC', 'ORB034', '0.05', 'geonode:S2A_date_lat1lon2_T12ABCSPLIT1_ORB034_etc', '0.1', '0.075'],
                                    ['S2A_date_lat1lon2_T12ABCSPLIT1_ORB034_etc', 'geonode:S2A_date_lat1lon2_T12ABCSPLIT1_ORB034_etc',
                                        'T12ABCSPLIT1', 'ORB034', '0.1', 'geonode:S2A_date_lat1lon2_T12ABC_ORB034_etc', '0.05', '0.075'],
                                    ['S2A_date_lat1lon2_T12ABC_ORB034_fullgran', 'geonode:S2A_date_lat1lon2_T12ABC_ORB034_fullgran',
                                        'T12ABC', 'ORB034', '0.12', np.nan, np.nan, '0.12']
                                    ]),
                          columns=['title', 'alternate', 'granule-ref', 'orbit-ref', 'ARCSI_CLOUD_COVER', 'split_granule.name', 'split_ARCSI_CLOUD_COVER', 'split_cloud_cover'])
        response = eodslib.find_minimum_cloud_list(df)
        expected_response = pd.DataFrame(np.array([['S2A_date_lat1lon2_T12ABC_ORB034_etc', 'geonode:S2A_date_lat1lon2_T12ABC_ORB034_etc', 'T12ABC', 'ORB034', '0.05', 'geonode:S2A_date_lat1lon2_T12ABCSPLIT1_ORB034_etc', '0.1', '0.075', 'S2A_date_lat1lon2_T12ABC', 'T12ABC_ORB034', 'T12ABC'],
                                                   ['S2A_date_lat1lon2_T12ABCSPLIT1_ORB034_etc', 'geonode:S2A_date_lat1lon2_T12ABCSPLIT1_ORB034_etc', 'T12ABCSPLIT1', 'ORB034',
                                                       '0.1', 'geonode:S2A_date_lat1lon2_T12ABC_ORB034_etc', '0.05', '0.075', 'S2A_date_lat1lon2_T12ABC', 'T12ABC_ORB034', 'T12ABC'],
                                                   ]),
                                         columns=['title', 'alternate', 'granule-ref', 'orbit-ref', 'ARCSI_CLOUD_COVER', 'split_granule.name', 'split_ARCSI_CLOUD_COVER', 'split_cloud_cover', 'title_stub', 'gran-orb', 'granule-stub'])

        response_difference = response.compare(expected_response)
        # more options can be specified also
        with pd.option_context('display.max_rows', None, 'display.max_columns', None):
            print('Difference between received and expected responses:')
            print(response_difference)

        mocker.resetall()

        assert response_difference.empty

    def test_component_greater_but_average_less_than_full_granule_return_both_splits(self, mocker):
        # S2A_date_lat1lon2_T12ABC_ORB034_etc = 0.2, S2A_date_lat1lon2_T12ABCSPLIT1_ORB034_etc = 0.05, S2A_date_lat1lon2_T12ABC_ORB034_fullgran = 0.19
        self.mock_path_exists = mocker.patch('eodslib.Path.exists')
        self.mock_path_exists.return_value = True
        self.mock_read_csv = mocker.patch('eodslib.pd.read_csv')
        self.mock_read_csv.return_value = pd.DataFrame(
            data={'gran-orb': ['T12ABC_ORB034']})
        df = pd.DataFrame(np.array([['S2A_date_lat1lon2_T12ABC_ORB034_etc', 'geonode:S2A_date_lat1lon2_T12ABC_ORB034_etc', 'T12ABC', 'ORB034', '0.2', 'geonode:S2A_date_lat1lon2_T12ABCSPLIT1_ORB034_etc', '0.05', '0.125'],
                                    ['S2A_date_lat1lon2_T12ABCSPLIT1_ORB034_etc', 'geonode:S2A_date_lat1lon2_T12ABCSPLIT1_ORB034_etc',
                                        'T12ABCSPLIT1', 'ORB034', '0.05', 'geonode:S2A_date_lat1lon2_T12ABC_ORB034_etc', '0.2', '0.125'],
                                    ['S2A_date_lat1lon2_T12ABC_ORB034_fullgran', 'geonode:S2A_date_lat1lon2_T12ABC_ORB034_fullgran',
                                        'T12ABC', 'ORB034', '0.19', np.nan, np.nan, '0.19']
                                    ]),
                          columns=['title', 'alternate', 'granule-ref', 'orbit-ref', 'ARCSI_CLOUD_COVER', 'split_granule.name', 'split_ARCSI_CLOUD_COVER', 'split_cloud_cover'])
        response = eodslib.find_minimum_cloud_list(df)
        expected_response = pd.DataFrame(np.array([['S2A_date_lat1lon2_T12ABC_ORB034_etc', 'geonode:S2A_date_lat1lon2_T12ABC_ORB034_etc', 'T12ABC', 'ORB034', '0.2', 'geonode:S2A_date_lat1lon2_T12ABCSPLIT1_ORB034_etc', '0.05', '0.125', 'S2A_date_lat1lon2_T12ABC', 'T12ABC_ORB034', 'T12ABC'],
                                                   ['S2A_date_lat1lon2_T12ABCSPLIT1_ORB034_etc', 'geonode:S2A_date_lat1lon2_T12ABCSPLIT1_ORB034_etc', 'T12ABCSPLIT1', 'ORB034',
                                                       '0.05', 'geonode:S2A_date_lat1lon2_T12ABC_ORB034_etc', '0.2', '0.125', 'S2A_date_lat1lon2_T12ABC', 'T12ABC_ORB034', 'T12ABC'],
                                                   ]),
                                         columns=['title', 'alternate', 'granule-ref', 'orbit-ref', 'ARCSI_CLOUD_COVER', 'split_granule.name', 'split_ARCSI_CLOUD_COVER', 'split_cloud_cover', 'title_stub', 'gran-orb', 'granule-stub'])

        response_difference = response.compare(expected_response, align_axis=0)
        # more options can be specified also
        with pd.option_context('display.max_rows', None, 'display.max_columns', None):
            print('Difference between received and expected responses:')
            print(response_difference)

        mocker.resetall()

        assert response_difference.empty

    def test_components_and_average_greater_than_full_granule_return_full_gran(self, mocker):
        # S2A_date_lat1lon2_T12ABC_ORB034_etc = 0.15, S2A_date_lat1lon2_T12ABCSPLIT1_ORB034_etc = 0.2, S2A_date_lat1lon2_T12ABC_ORB034_fullgran = 0.1
        self.mock_path_exists = mocker.patch('eodslib.Path.exists')
        self.mock_path_exists.return_value = True
        self.mock_read_csv = mocker.patch('eodslib.pd.read_csv')
        self.mock_read_csv.return_value = pd.DataFrame(
            data={'gran-orb': ['T12ABC_ORB034']})
        df = pd.DataFrame(np.array([['S2A_date_lat1lon2_T12ABC_ORB034_etc', 'geonode:S2A_date_lat1lon2_T12ABC_ORB034_etc', 'T12ABC', 'ORB034', '0.15', 'geonode:S2A_date_lat1lon2_T12ABCSPLIT1_ORB034_etc', '0.2', '0.175'],
                                    ['S2A_date_lat1lon2_T12ABCSPLIT1_ORB034_etc', 'geonode:S2A_date_lat1lon2_T12ABCSPLIT1_ORB034_etc',
                                        'T12ABCSPLIT1', 'ORB034', '0.2', 'geonode:S2A_date_lat1lon2_T12ABC_ORB034_etc', '0.15', '0.175'],
                                    ['S2A_date_lat1lon2_T12ABC_ORB034_fullgran', 'geonode:S2A_date_lat1lon2_T12ABC_ORB034_fullgran',
                                        'T12ABC', 'ORB034', '0.1', np.nan, np.nan, '0.1']
                                    ]),
                          columns=['title', 'alternate', 'granule-ref', 'orbit-ref', 'ARCSI_CLOUD_COVER', 'split_granule.name', 'split_ARCSI_CLOUD_COVER', 'split_cloud_cover'])
        response = eodslib.find_minimum_cloud_list(df)
        expected_response = pd.DataFrame(np.array([['S2A_date_lat1lon2_T12ABC_ORB034_fullgran', 'geonode:S2A_date_lat1lon2_T12ABC_ORB034_fullgran', 'T12ABC', 'ORB034', '0.1', np.nan, np.nan, '0.1', 'S2A_date_lat1lon2_T12ABC', 'T12ABC_ORB034', 'T12ABC'],
                                                   ]),
                                         columns=['title', 'alternate', 'granule-ref', 'orbit-ref', 'ARCSI_CLOUD_COVER', 'split_granule.name', 'split_ARCSI_CLOUD_COVER', 'split_cloud_cover', 'title_stub', 'gran-orb', 'granule-stub'])

        response_difference = response.compare(expected_response, align_axis=0)
        # more options can be specified also
        with pd.option_context('display.max_rows', None, 'display.max_columns', None):
            print('Difference between received and expected responses:')
            print(response_difference)

        mocker.resetall()

        assert response_difference.empty

    def test_no_layers_match_safe_list_trigger_exception(self, mocker):
        self.mock_path_exists = mocker.patch('eodslib.Path.exists')
        self.mock_path_exists.return_value = True
        self.mock_read_csv = mocker.patch('eodslib.pd.read_csv')
        self.mock_read_csv.return_value = pd.DataFrame(
            data={'gran-orb': ['badref']})
        df = pd.DataFrame(np.array([['S2A_date_lat1lon2_T12ABC_ORB034_etc', 'geonode:S2A_date_lat1lon2_T12ABC_ORB034_etc', 'T12ABC', 'ORB034', '0.15', 'geonode:S2A_date_lat1lon2_T12ABCSPLIT1_ORB034_etc', '0.2', '0.175'],
                                    ['S2A_date_lat1lon2_T12ABCSPLIT1_ORB034_etc', 'geonode:S2A_date_lat1lon2_T12ABCSPLIT1_ORB034_etc',
                                        'T12ABCSPLIT1', 'ORB034', '0.2', 'geonode:S2A_date_lat1lon2_T12ABC_ORB034_etc', '0.15', '0.175'],
                                    ['S2A_date_lat1lon2_T12ABC_ORB034_fullgran', 'geonode:S2A_date_lat1lon2_T12ABC_ORB034_fullgran',
                                        'T12ABC', 'ORB034', '0.1', np.nan, np.nan, '0.1']
                                    ]),
                          columns=['title', 'alternate', 'granule-ref', 'orbit-ref', 'ARCSI_CLOUD_COVER', 'split_granule.name', 'split_ARCSI_CLOUD_COVER', 'split_cloud_cover'])

        with pytest.raises(ValueError) as error:
            eodslib.find_minimum_cloud_list(df)

        assert error.value.args[0] == 'ERROR : You have selected find_least_cloud=True BUT your search criteria is too narrow, spatially or temporally and did not match any granule references in "./static/safe-granule-orbit-list.txt". Suggest widening your search'

    def test_no_safe_list_found_trigger_exception(self, mocker):
        self.mock_path_exists = mocker.patch('eodslib.Path.exists')
        self.mock_path_exists.return_value = False
        df = pd.DataFrame(np.array([['S2A_date_lat1lon2_T12ABC_ORB034_etc', 'geonode:S2A_date_lat1lon2_T12ABC_ORB034_etc', 'T12ABC', 'ORB034', '0.15', 'geonode:S2A_date_lat1lon2_T12ABCSPLIT1_ORB034_etc', '0.2', '0.175'],
                                    ['S2A_date_lat1lon2_T12ABCSPLIT1_ORB034_etc', 'geonode:S2A_date_lat1lon2_T12ABCSPLIT1_ORB034_etc',
                                        'T12ABCSPLIT1', 'ORB034', '0.2', 'geonode:S2A_date_lat1lon2_T12ABC_ORB034_etc', '0.15', '0.175'],
                                    ['S2A_date_lat1lon2_T12ABC_ORB034_fullgran', 'geonode:S2A_date_lat1lon2_T12ABC_ORB034_fullgran',
                                        'T12ABC', 'ORB034', '0.1', np.nan, np.nan, '0.1']
                                    ]),
                          columns=['title', 'alternate', 'granule-ref', 'orbit-ref', 'ARCSI_CLOUD_COVER', 'split_granule.name', 'split_ARCSI_CLOUD_COVER', 'split_cloud_cover'])

        with pytest.raises(ValueError) as error:
            eodslib.find_minimum_cloud_list(df)

        mocker.resetall()

        assert error.value.args[0] == 'ERROR :: safe-granule-orbit-list.txt cannot be found'


class TestQueryCatalog():
    def test_successful_query_return_correct_list_and_df(self, mocker):
        self.mock_get = mocker.patch('eodslib.requests.get')
        self.mock_get.return_value.status_code = 200
        self.mock_get.return_value.content = bytes(
            b'{"meta": {"total_count": 1}, "objects": [{"alternate":"geonode:layername"}]}')

        self.mock_make_output_dir = mocker.patch('eodslib.make_output_dir')
        self.mock_make_output_dir.return_value = Path.cwd()

        self.mock_df_to_csv = mocker.patch('eodslib.pd.DataFrame.to_csv')
        self.mock_df_to_csv = None

        conn = {
            'domain': 'domainname',
            'username': 'username',
            'access_token': 'token',
        }

        eods_params = {
            'title': 'keep_api_test_create_group',
        }

        output_list, filtered_df = eodslib.query_catalog(conn, **eods_params)

        expected_output_list = [
            'geonode:layername']
        expected_filtered_df = pd.DataFrame(np.array(
            [["geonode:layername"]]),
            columns=["alternate"])

        df_difference = filtered_df.compare(expected_filtered_df, align_axis=0)
        with pd.option_context('display.max_rows', None, 'display.max_columns', None):
            print('Difference between received and expected filtered_dfs:')
            print(df_difference)

        output_list_bool = output_list == expected_output_list
        filtered_df_bool = df_difference.empty

        mocker.resetall()

        assert output_list_bool and filtered_df_bool

    def test_find_least_cloud_sat_id_1_trigger_exception(self):
        conn = {
            'domain': 'domainname',
            'username': 'username',
            'access_token': 'token',
        }

        eods_params = {
            'title': 'keep_api_test_create_group',
            'sat_id': 1,
            'find_least_cloud': True
        }
        with pytest.raises(ValueError) as error:
            eodslib.query_catalog(conn, **eods_params)

        assert error.value.args[0] == "QUERY failed, you have specified 'sat_id'=1 and 'find_least_cloud'=True. Use 'sat_id'=2 and 'find_least_cloud'=True"

    def test_start_date_no_end_date_trigger_exception(self):
        conn = {
            'domain': 'domainname',
            'username': 'username',
            'access_token': 'token',
        }

        eods_params = {
            'title': 'keep_api_test_create_group',
            'start_date': '2020-01-01'
        }
        with pytest.raises(ValueError) as error:
            eodslib.query_catalog(conn, **eods_params)

        assert error.value.args[0] == "QUERY failed, if querying by date, please specify *BOTH* 'start_date' and 'end_date'"

    def test_end_date_no_start_date_trigger_exception(self):
        conn = {
            'domain': 'domainname',
            'username': 'username',
            'access_token': 'token',
        }

        eods_params = {
            'title': 'keep_api_test_create_group',
            'end_date': '2020-01-01'
        }
        with pytest.raises(ValueError) as error:
            eodslib.query_catalog(conn, **eods_params)

        assert error.value.args[0] == "QUERY failed, if querying by date, please specify *BOTH* 'start_date' and 'end_date'"

    def test_cloud_min_no_cloud_max_trigger_exception(self):
        conn = {
            'domain': 'domainname',
            'username': 'username',
            'access_token': 'token',
        }

        eods_params = {
            'title': 'keep_api_test_create_group',
            'cloud_min': 1
        }
        with pytest.raises(ValueError) as error:
            eodslib.query_catalog(conn, **eods_params)

        assert error.value.args[0] == "QUERY failed, if querying by cloud cover, please specify *BOTH* 'cloud_min' and 'cloud_max'"

    def test_cloud_max_no_cloud_min_trigger_exception(self):
        conn = {
            'domain': 'domainname',
            'username': 'username',
            'access_token': 'token',
        }

        eods_params = {
            'title': 'keep_api_test_create_group',
            'cloud_max': 1
        }
        with pytest.raises(ValueError) as error:
            eodslib.query_catalog(conn, **eods_params)

        assert error.value.args[0] == "QUERY failed, if querying by cloud cover, please specify *BOTH* 'cloud_min' and 'cloud_max'"

    def test_cloud_min_cloud_max_and_sat_id_1_trigger_exception(self):
        conn = {
            'domain': 'domainname',
            'username': 'username',
            'access_token': 'token',
        }

        eods_params = {
            'title': 'keep_api_test_create_group',
            'cloud_min': 1,
            'cloud_max': 5,
            'sat_id': 1
        }
        with pytest.raises(ValueError) as error:
            eodslib.query_catalog(conn, **eods_params)

        assert error.value.args[0] == "QUERY failed, if querying by cloud cover, please specify 'sat_id'=2"

    def test_json_response_meta_total_count_0_return_empty_list_none_df(self, mocker):
        self.mock_get = mocker.patch('eodslib.requests.get')
        self.mock_get.return_value.status_code = 200
        self.mock_get.return_value.content = bytes(
            b'{"meta": {"total_count": 0}}')

        conn = {
            'domain': 'domainname',
            'username': 'username',
            'access_token': 'token',
        }

        eods_params = {
            'title': 'keep_api_test_create_group',
        }

        output_list, filtered_df = eodslib.query_catalog(conn, **eods_params)

        expected_output_list = []
        expected_filtered_df = None

        output_list_bool = output_list == expected_output_list
        filtered_df_bool = filtered_df == expected_filtered_df

        mocker.resetall()

        assert output_list_bool and filtered_df_bool

    def test_non_200_status_trigger_exception(self, mocker):
        self.mock_get = mocker.patch('eodslib.requests.get')
        self.mock_get.return_value.status_code = 400
        self.mock_get.return_value.content = bytes(b'{"a":"b"}')
        self.mock_get.return_value.url = 'testurl'
        self.mock_datetime = mocker.patch('eodslib.datetime')
        self.mock_datetime.utcnow.return_value.isoformat.return_value = 'timestamp'

        conn = {
            'domain': 'domainname',
            'username': 'username',
            'access_token': 'token',
        }

        eods_params = {
            'title': 'keep_api_test_create_group',
        }
        with pytest.raises(ValueError) as error:
            eodslib.query_catalog(conn, **eods_params)

        expected_error = 'timestamp' + ' :: RESPONSE STATUS = ' + \
            str(self.mock_get.return_value.status_code) + ' (NOT SUCCESSFUL)' + \
            str(self.mock_get.return_value.status_code) + \
            ' :: QUERY URL (CONTAINS SENSITIVE AUTHENTICATION DETAILS, DO NOT SHARE) = ' + 'testurl'

        mocker.resetall()

        assert error.value.args[0] == expected_error

    def test_request_exception_triggered_return_none_prints_error(self, mocker, capsys):
        self.mock_get = mocker.patch('eodslib.requests.get')
        self.mock_get.side_effect = requests.exceptions.RequestException(
            'test error message')
        self.mock_datetime = mocker.patch('eodslib.datetime')
        self.mock_datetime.utcnow.return_value.isoformat.return_value = 'timestamp'

        conn = {
            'domain': 'domainname',
            'username': 'username',
            'access_token': 'token',
        }

        eods_params = {
            'title': 'keep_api_test_create_group',
        }

        eodslib.query_catalog(conn, **eods_params)

        captured_out = capsys.readouterr().out
        trim_out = captured_out.strip('\n')
        error_message = trim_out

        expected_error = "timestamp :: ERROR, an Exception was raised, no list returned\ntest error message"

        mocker.resetall()

        assert error_message == expected_error

    def test_sat_id_2_return_correct_list_and_df_with_new_cols(self, mocker):
        self.mock_get = mocker.patch('eodslib.requests.get')
        self.mock_get.return_value.status_code = 200
        self.mock_get.return_value.content = bytes(
            b'{"meta": {"total_count": 1}}')

        self.mock_json_normalize = mocker.patch('eodslib.json_normalize')
        df = pd.DataFrame({'title': "S2A_date_lat1lon2_T12ABC_ORB034_etc", "alternate": "geonode:S2A_date_lat1lon2_T12ABC_ORB034_etc",
                           "supplemental_information": "Data Collection Time: time\nARCSI_CLOUD_COVER: 0.12345\netc"}, index=[0])
        self.mock_json_normalize.return_value = df

        self.mock_make_output_dir = mocker.patch('eodslib.make_output_dir')
        self.mock_make_output_dir.return_value = Path.cwd()

        self.mock_df_to_csv = mocker.patch('eodslib.pd.DataFrame.to_csv')
        self.mock_df_to_csv = None

        self.mock_minimum_cloud = mocker.patch(
            'eodslib.find_minimum_cloud_list')

        def side_effect_fn(*args, **kwargs):
            return args[0]
        self.mock_minimum_cloud.side_effect = side_effect_fn

        conn = {
            'domain': 'domainname',
            'username': 'username',
            'access_token': 'token',
        }

        eods_params = {
            'title': 'keep_api_test_create_group',
            'sat_id': 2
        }

        output_list, filtered_df = eodslib.query_catalog(conn, **eods_params)

        expected_filtered_df = pd.DataFrame({'title': "S2A_date_lat1lon2_T12ABC_ORB034_etc", "alternate": "geonode:S2A_date_lat1lon2_T12ABC_ORB034_etc",
                                             "supplemental_information": "Data Collection Time: time\nARCSI_CLOUD_COVER: 0.12345\netc",
                                             "granule-ref": "T12ABC", "orbit-ref": "ORB034", "ARCSI_CLOUD_COVER": "0.12345"}, index=[0])

        expected_output_list = ["geonode:S2A_date_lat1lon2_T12ABC_ORB034_etc"]

        df_difference = filtered_df.compare(expected_filtered_df, align_axis=0)
        with pd.option_context('display.max_rows', None, 'display.max_columns', None):
            print('Difference between received and expected filtered_dfs:')
            print(df_difference)

        output_list_bool = output_list == expected_output_list
        filtered_df_bool = df_difference.empty

        mocker.resetall()

        assert output_list_bool and filtered_df_bool

    def test_sat_id_not_2_return_correct_list_and_df(self, mocker):
        self.mock_get = mocker.patch('eodslib.requests.get')
        self.mock_get.return_value.status_code = 200
        self.mock_get.return_value.content = bytes(
            b'{"meta": {"total_count": 1}}')

        self.mock_json_normalize = mocker.patch('eodslib.json_normalize')
        df = pd.DataFrame({"alternate": "geonode:layername",
                           }, index=[0])
        self.mock_json_normalize.return_value = df

        self.mock_make_output_dir = mocker.patch('eodslib.make_output_dir')
        self.mock_make_output_dir.return_value = Path.cwd()

        self.mock_df_to_csv = mocker.patch('eodslib.pd.DataFrame.to_csv')
        self.mock_df_to_csv = None

        conn = {
            'domain': 'domainname',
            'username': 'username',
            'access_token': 'token',
        }

        eods_params = {
            'title': 'keep_api_test_create_group',
            'sat_id': 1
        }

        output_list, filtered_df = eodslib.query_catalog(conn, **eods_params)

        expected_filtered_df = pd.DataFrame({"alternate": "geonode:layername",
                                             }, index=[0])

        expected_output_list = ["geonode:layername"]

        df_difference = filtered_df.compare(expected_filtered_df, align_axis=0)
        with pd.option_context('display.max_rows', None, 'display.max_columns', None):
            print('Difference between received and expected filtered_dfs:')
            print(df_difference)

        output_list_bool = output_list == expected_output_list
        filtered_df_bool = df_difference.empty

        mocker.resetall()

        assert output_list_bool and filtered_df_bool

    def test_find_least_cloud_sat_id_not_1_or_2_return_correct_list_and_df(self, mocker):
        self.mock_get = mocker.patch('eodslib.requests.get')
        self.mock_get.return_value.status_code = 200
        self.mock_get.return_value.content = bytes(
            b'{"meta": {"total_count": 1}}')

        self.mock_json_normalize = mocker.patch('eodslib.json_normalize')
        df = pd.DataFrame({"alternate": "geonode:layername",
                           }, index=[0])
        self.mock_json_normalize.return_value = df

        self.mock_make_output_dir = mocker.patch('eodslib.make_output_dir')
        self.mock_make_output_dir.return_value = Path.cwd()

        self.mock_df_to_csv = mocker.patch('eodslib.pd.DataFrame.to_csv')
        self.mock_df_to_csv = None

        conn = {
            'domain': 'domainname',
            'username': 'username',
            'access_token': 'token',
        }

        eods_params = {
            'title': 'keep_api_test_create_group',
            'sat_id': 3,
            'find_least_cloud': True
        }

        output_list, filtered_df = eodslib.query_catalog(conn, **eods_params)

        expected_filtered_df = pd.DataFrame({"alternate": "geonode:layername",
                                             }, index=[0])

        expected_output_list = ["geonode:layername"]

        df_difference = filtered_df.compare(expected_filtered_df, align_axis=0)
        with pd.option_context('display.max_rows', None, 'display.max_columns', None):
            print('Difference between received and expected filtered_dfs:')
            print(df_difference)

        output_list_bool = output_list == expected_output_list
        filtered_df_bool = df_difference.empty

        mocker.resetall()

        assert output_list_bool and filtered_df_bool

    def test_find_least_cloud_false_sat_id_2_return_correct_list_and_df_with_new_cols(self, mocker):
        self.mock_get = mocker.patch('eodslib.requests.get')
        self.mock_get.return_value.status_code = 200
        self.mock_get.return_value.content = bytes(
            b'{"meta": {"total_count": 1}}')

        self.mock_json_normalize = mocker.patch('eodslib.json_normalize')
        df = pd.DataFrame({'title': "S2A_date_lat1lon2_T12ABC_ORB034_etc", "alternate": "geonode:S2A_date_lat1lon2_T12ABC_ORB034_etc",
                           "supplemental_information": "Data Collection Time: time\nARCSI_CLOUD_COVER: 0.12345\netc"}, index=[0])
        self.mock_json_normalize.return_value = df

        self.mock_make_output_dir = mocker.patch('eodslib.make_output_dir')
        self.mock_make_output_dir.return_value = Path.cwd()

        self.mock_df_to_csv = mocker.patch('eodslib.pd.DataFrame.to_csv')
        self.mock_df_to_csv = None

        conn = {
            'domain': 'domainname',
            'username': 'username',
            'access_token': 'token',
        }

        eods_params = {
            'title': 'keep_api_test_create_group',
            'sat_id': 2,
            'find_least_cloud': False
        }

        output_list, filtered_df = eodslib.query_catalog(conn, **eods_params)

        expected_filtered_df = pd.DataFrame({'title': "S2A_date_lat1lon2_T12ABC_ORB034_etc", "alternate": "geonode:S2A_date_lat1lon2_T12ABC_ORB034_etc",
                                             "supplemental_information": "Data Collection Time: time\nARCSI_CLOUD_COVER: 0.12345\netc",
                                             "granule-ref": "T12ABC", "orbit-ref": "ORB034", "ARCSI_CLOUD_COVER": "0.12345"}, index=[0])

        expected_output_list = ["geonode:S2A_date_lat1lon2_T12ABC_ORB034_etc"]

        df_difference = filtered_df.compare(expected_filtered_df, align_axis=0)
        with pd.option_context('display.max_rows', None, 'display.max_columns', None):
            print('Difference between received and expected filtered_dfs:')
            print(df_difference)

        output_list_bool = output_list == expected_output_list
        filtered_df_bool = df_difference.empty

        mocker.resetall()

        assert output_list_bool and filtered_df_bool

    def test_find_least_cloud_sat_id_2_fullgran_return_correct_list_and_df_with_new_cols(self, mocker):
        self.mock_get = mocker.patch('eodslib.requests.get')
        self.mock_get.return_value.status_code = 200
        self.mock_get.return_value.content = bytes(
            b'{"meta": {"total_count": 1}}')

        self.mock_json_normalize = mocker.patch('eodslib.json_normalize')
        df = pd.DataFrame({'title': "S2A_date_lat1lon2_T12ABC_ORB034_etc", "alternate": "geonode:S2A_date_lat1lon2_T12ABC_ORB034_etc",
                           "supplemental_information": "Data Collection Time: time\nARCSI_CLOUD_COVER: 0.12345\netc"}, index=[0])
        self.mock_json_normalize.return_value = df

        self.mock_make_output_dir = mocker.patch('eodslib.make_output_dir')
        self.mock_make_output_dir.return_value = Path.cwd()

        self.mock_df_to_csv = mocker.patch('eodslib.pd.DataFrame.to_csv')
        self.mock_df_to_csv = None

        self.mock_minimum_cloud = mocker.patch(
            'eodslib.find_minimum_cloud_list')

        def side_effect_fn(*args, **kwargs):
            return args[0]
        self.mock_minimum_cloud.side_effect = side_effect_fn

        conn = {
            'domain': 'domainname',
            'username': 'username',
            'access_token': 'token',
        }

        eods_params = {
            'title': 'keep_api_test_create_group',
            'find_least_cloud': True,
            'sat_id': 2
        }

        output_list, filtered_df = eodslib.query_catalog(conn, **eods_params)

        expected_filtered_df = pd.DataFrame({'title': "S2A_date_lat1lon2_T12ABC_ORB034_etc", "alternate": "geonode:S2A_date_lat1lon2_T12ABC_ORB034_etc",
                                             "supplemental_information": "Data Collection Time: time\nARCSI_CLOUD_COVER: 0.12345\netc",
                                             "granule-ref": "T12ABC", "orbit-ref": "ORB034", "ARCSI_CLOUD_COVER": "0.12345", "split_cloud_cover": "0.12345"}, index=[0])

        expected_output_list = ["geonode:S2A_date_lat1lon2_T12ABC_ORB034_etc"]

        df_difference = filtered_df.compare(expected_filtered_df, align_axis=0)
        with pd.option_context('display.max_rows', None, 'display.max_columns', None):
            print('Difference between received and expected filtered_dfs:')
            print(df_difference)

        output_list_bool = output_list == expected_output_list
        filtered_df_bool = df_difference.empty

        mocker.resetall()

        assert output_list_bool and filtered_df_bool

    def test_find_least_cloud_sat_id_2_both_split_gran_components_return_correct_list_and_df_with_new_cols(self, mocker):
        self.mock_get = mocker.patch('eodslib.requests.get')
        self.mock_get.return_value.status_code = 200
        self.mock_get.return_value.content = bytes(
            b'{"meta": {"total_count": 1}}')

        self.mock_json_normalize = mocker.patch('eodslib.json_normalize')
        df = pd.DataFrame({'title': ["S2A_date_lat1lon2_T12ABC_ORB034_etc", "S2A_date_lat1lon2_T12ABCSPLIT1_ORB034_etc"],
                           "alternate": ["geonode:S2A_date_lat1lon2_T12ABC_ORB034_etc", "geonode:S2A_date_lat1lon2_T12ABCSPLIT1_ORB034_etc"],
                           "supplemental_information": ["Data Collection Time: time\nARCSI_CLOUD_COVER: 0.1\netc",
                                                        "Data Collection Time: time\nARCSI_CLOUD_COVER: 0.3\netc"],
                           "split_granule.name": ["geonode:S2A_date_lat1lon2_T12ABCSPLIT1_ORB034_etc", "geonode:S2A_date_lat1lon2_T12ABC_ORB034_etc"],
                           })
        self.mock_json_normalize.return_value = df

        self.mock_make_output_dir = mocker.patch('eodslib.make_output_dir')
        self.mock_make_output_dir.return_value = Path.cwd()

        self.mock_df_to_csv = mocker.patch('eodslib.pd.DataFrame.to_csv')
        self.mock_df_to_csv = None

        self.mock_minimum_cloud = mocker.patch(
            'eodslib.find_minimum_cloud_list')

        def side_effect_fn(*args, **kwargs):
            return args[0]
        self.mock_minimum_cloud.side_effect = side_effect_fn

        conn = {
            'domain': 'domainname',
            'username': 'username',
            'access_token': 'token',
        }

        eods_params = {
            'title': 'keep_api_test_create_group',
            'find_least_cloud': True,
            'sat_id': 2
        }

        output_list, filtered_df = eodslib.query_catalog(conn, **eods_params)

        expected_filtered_df = pd.DataFrame({'title': ["S2A_date_lat1lon2_T12ABC_ORB034_etc", "S2A_date_lat1lon2_T12ABCSPLIT1_ORB034_etc"],
                                             "alternate": ["geonode:S2A_date_lat1lon2_T12ABC_ORB034_etc", "geonode:S2A_date_lat1lon2_T12ABCSPLIT1_ORB034_etc"],
                                             "supplemental_information": ["Data Collection Time: time\nARCSI_CLOUD_COVER: 0.1\netc",
                                                                          "Data Collection Time: time\nARCSI_CLOUD_COVER: 0.3\netc"],
                                             "split_granule.name": ["geonode:S2A_date_lat1lon2_T12ABCSPLIT1_ORB034_etc", "geonode:S2A_date_lat1lon2_T12ABC_ORB034_etc"],
                                             "granule-ref": ["T12ABC", "T12ABCSPLIT1"],
                                             "orbit-ref": ["ORB034", "ORB034"],
                                             "ARCSI_CLOUD_COVER": ["0.1", "0.3"],
                                             "split_ARCSI_CLOUD_COVER": ["0.3", "0.1"],
                                             "split_cloud_cover": ["0.2", "0.2"]
                                             })

        expected_output_list = ["geonode:S2A_date_lat1lon2_T12ABC_ORB034_etc",
                                "geonode:S2A_date_lat1lon2_T12ABCSPLIT1_ORB034_etc"]

        df_difference = filtered_df.compare(expected_filtered_df, align_axis=0)
        with pd.option_context('display.max_rows', None, 'display.max_columns', None):
            print('Difference between received and expected filtered_dfs:')
            print(df_difference)

        output_list_bool = output_list == expected_output_list
        filtered_df_bool = df_difference.empty

        mocker.resetall()

        assert output_list_bool and filtered_df_bool


class TestRunWps():
    def test_all_correct_responses_return_correct_execution_dict(self, mocker):
        self.mock_submit_queue = mocker.patch('eodslib.submit_wps_queue')
        self.mock_submit_queue.return_value = {'job_id': '123', 'layer_name': 'geonode:layername',
                                               'timestamp_job_start': datetime(2021, 8, 17), 'timestamp_job_end': datetime(2021, 8, 18),
                                               'continue_process': True}

        self.mock_make_output_dir = mocker.patch('eodslib.make_output_dir')
        self.mock_make_output_dir.return_value = Path.cwd()

        self.mock_poll = mocker.patch(
            'eodslib.poll_api_status')

        def poll_side_effect_fn(*args, **kwargs):
            execution_dict = args[0]
            execution_dict['continue_process'] = False
            execution_dict['job_status'] = 'DOWNLOAD-SUCCESSFUL'
            return execution_dict

        self.mock_poll.side_effect = poll_side_effect_fn

        self.mock_process = mocker.patch(
            'eodslib.process_wps_downloaded_files')

        def process_side_effect_fn(*args, **kwargs):
            execution_dict = args[0]
            return execution_dict

        self.mock_process.side_effect = process_side_effect_fn

        conn = {
            'domain': 'domainname',
            'access_token': 'token',
        }

        config_wpsprocess = {
        }

        execution_dict = eodslib.run_wps(conn, config_wpsprocess)

        assert execution_dict == {'job_id': '123', 'layer_name': 'geonode:layername', 'job_status': 'DOWNLOAD-SUCCESSFUL',
                                  'timestamp_job_start': datetime(2021, 8, 17), 'timestamp_job_end': datetime(2021, 8, 18),
                                  'continue_process': False, 'log_file_path': Path.cwd() / 'wps-log.csv',
                                  'total_job_duration': 1440.0}


class TestSubmitWpsQueue():
    @responses.activate
    def test_successful_post_return_correct_execution_dict(self, mocker):
        self.mock_mod_xml = mocker.patch('eodslib.mod_the_xml')
        self.mock_mod_xml.return_value = None

        self.mock_datetime = mocker.patch('eodslib.datetime')
        self.mock_datetime.utcnow.return_value = datetime(2021, 8, 17)

        url = 'https://domain'

        config_wpsprocess = {
            'xml_config': {
                'template_layer_name': 'layername',
            },
        }

        request_config = {
            'wps_server': 'https://domain',
            'access_token': 'token',
            'headers': {'header': 'a_header'},
            'verify': 'verify'
        }

        responses.add(responses.POST, url, status=200,
                      body='Body executionId=123')

        response = eodslib.submit_wps_queue(request_config, config_wpsprocess)

        assert response == {'job_id': '123', 'layer_name': 'layername', 'timestamp_job_start': datetime(
            2021, 8, 17, 0, 0), 'continue_process': True}


class TestPollApiStatus():
    @responses.activate
    def test_successful_post_return_correct_execution_dict(self, mocker):
        self.mock_datetime = mocker.patch('eodslib.datetime')
        self.mock_datetime.utcnow.return_value = datetime(2021, 8, 17)

        self.mock_get = mocker.patch('eodslib.requests.get')
        self.mock_get.return_value.content = bytes(
            b'<?xml version="1.0" encoding="UTF-8"?><wps:ExecuteResponse xmlns:wps="http://www.opengis.net/wps/1.0.0"><wps:Status><wps:ProcessSucceeded></wps:ProcessSucceeded></wps:Status><wps:ProcessOutputs><wps:Output><wps:Reference href="href" mimeType="mime"/></wps:Output></wps:ProcessOutputs></wps:ExecuteResponse>'
        )

        self.mock_download_single = mocker.patch('eodslib.download_wps_result_single')
        def side_effect_fn(*args, **kwargs):
            execution_dict = args[1]
            return execution_dict

        self.mock_download_single.side_effect = side_effect_fn

        

        request_config = {
            'wps_server': 'https://domain',
            'access_token': 'token',
            'headers': {'header': 'a_header'},
            'verify': 'verify'
        }

        path_output = Path.cwd()

        execution_dict = {'job_id': '123', 'layer_name': 'layername', 'timestamp_job_start': datetime(
            2021, 8, 17, 0, 0), 'continue_process': True}

        execution_dict = eodslib.poll_api_status(
            execution_dict, request_config, path_output)

        expected_execution_dict = {'job_id': '123', 'layer_name': 'layername',
                                   'timestamp_job_start': datetime(2021, 8, 17, 0, 0),
                                   'continue_process': True,
                                   'dl_url': 'href&access_token=token',
                                   'job_status': 'READY-TO-DOWNLOAD',
                                   'mime_type': 'mime',
                                   'timestamp_ready_to_dl': datetime(2021, 8, 17, 0, 0)}

        assert execution_dict == expected_execution_dict

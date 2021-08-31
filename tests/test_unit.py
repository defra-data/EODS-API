from pathlib import Path, PosixPath
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
from zipfile import ZipInfo

def return_first_arg_side_effect_fn(*args, **kwargs):
    return args[0]

def return_second_arg_side_effect_fn(*args, **kwargs):
    return args[1]

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
    @pytest.fixture(autouse=True, scope='function')
    def setup(self, mocker):
        self.mock_post = mocker.patch('eodslib.post_to_layer_group_api')
        self.mock_post.return_value = 'mock response json'
        self.conn = {
            'domain': 'domainname',
            'username': 'username',
            'access_token': 'token',
        }

    def test_correct_param_input_returns_response_json(self, mocker):
        list_of_layers = ['keep_api_test_create_group']

        response_json = eodslib.create_layer_group(
            self.conn,
            list_of_layers,
            'eodslib-create-layer-test-',
            abstract='some description of the layer group '
        )

        mocker.resetall()

        assert response_json == 'mock response json'

    def test_list_of_layers_not_list_triggers_exception(self, mocker):
        list_of_layers = 'badstr'

        with pytest.raises(TypeError) as error:
            eodslib.create_layer_group(
                self.conn,
                list_of_layers,
                'eodslib-create-layer-test-',
                abstract='some description of the layer group '
            )
        assert error.value.args[0] == 'ERROR. list_of_layers is not a list, aborting ...'

        mocker.resetall()

    def test_name_not_str_triggers_exception(self, mocker):
        list_of_layers = ['keep_api_test_create_group']

        with pytest.raises(TypeError) as error:
            eodslib.create_layer_group(
                self.conn,
                list_of_layers,
                ['eodslib-create-layer-test-'],
                abstract='some description of the layer group '
            )
        assert error.value.args[0] == 'ERROR. layer group name is not a string, aborting ...'

        mocker.resetall()

    def test_list_of_layers_empty_triggers_exception(self, mocker):
        list_of_layers = []

        with pytest.raises(ValueError) as error:
            eodslib.create_layer_group(
                self.conn,
                list_of_layers,
                'eodslib-create-layer-test-',
                abstract='some description of the layer group '
            )
        assert error.value.args[0] == 'ERROR. list_of_layers is empty, aborting ...'

        mocker.resetall()

    def test_name_empty_triggers_exception(self, mocker):
        list_of_layers = ['keep_api_test_create_group']

        with pytest.raises(ValueError) as error:
            eodslib.create_layer_group(
                self.conn,
                list_of_layers,
                '',
                abstract='some description of the layer group '
            )
        assert error.value.args[0] == 'ERROR. layer group name string is empty, aborting ...'

        mocker.resetall()


class TestModifyLayerGroup():
    @pytest.fixture(autouse=True, scope='function')
    def setup(self, mocker):
        self.mock_post = mocker.patch('eodslib.post_to_layer_group_api')
        self.mock_post.return_value = 'mock response json'
        self.conn = {
            'domain': 'domainname',
            'username': 'username',
            'access_token': 'token',
        }

    def test_correct_param_input_returns_response_json(self, mocker):
        list_of_layers = ['keep_api_test_create_group']

        response_json = eodslib.modify_layer_group(
            self.conn,
            list_of_layers,
            0,
            abstract='update the abstract '
        )
        mocker.resetall()

        assert response_json == 'mock response json'

    def test_list_of_layers_not_list_triggers_exception(self, mocker):
        list_of_layers = 'badstr'

        with pytest.raises(TypeError) as error:
            eodslib.modify_layer_group(
                self.conn,
                list_of_layers,
                0,
                abstract='update the abstract '
            )
        assert error.value.args[0] == 'ERROR. list_of_layers is not a list, aborting ...'

        mocker.resetall()

    def test_id_not_int_triggers_exception(self, mocker):
        list_of_layers = ['keep_api_test_create_group']

        with pytest.raises(TypeError) as error:
            eodslib.modify_layer_group(
                self.conn,
                list_of_layers,
                'badID',
                abstract='update the abstract '
            )
        assert error.value.args[0] == 'ERROR. layer group ID is not an integer, aborting ...'

        mocker.resetall()

    def test_list_of_layers_empty_triggers_exception(self, mocker):
        list_of_layers = []

        with pytest.raises(ValueError) as error:
            eodslib.modify_layer_group(
                self.conn,
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
    @pytest.fixture(autouse=True, scope='function')
    def setup(self, mocker):
        self.mock_path_exists = mocker.patch('eodslib.Path.exists')
        self.mock_path_exists.return_value = True
        self.mock_read_csv = mocker.patch('eodslib.pd.read_csv')
        self.mock_read_csv.return_value = pd.DataFrame(
            data={'gran-orb': ['T12ABC_ORB034']})

    def test_components_and_average_less_than_full_granule_return_both_splits(self, mocker):
        # S2A_date_lat1lon2_T12ABC_ORB034_etc = 0.05, S2A_date_lat1lon2_T12ABCSPLIT1_ORB034_etc = 0.1, S2A_date_lat1lon2_T12ABC_ORB034_fullgran = 0.12
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
    @pytest.fixture(autouse=True, scope='function')
    def setup(self, mocker):
        self.mock_get = mocker.patch('eodslib.requests.get')
        self.mock_get.return_value.status_code = 200
        self.mock_get.return_value.content = bytes(
            b'{"meta": {"total_count": 1}}')
        self.mock_get.return_value.url = 'testurl'

        self.mock_make_output_dir = mocker.patch('eodslib.make_output_dir')
        self.mock_make_output_dir.return_value = Path.cwd()

        self.mock_df_to_csv = mocker.patch('eodslib.pd.DataFrame.to_csv')
        self.mock_df_to_csv.return_value = None

        self.mock_datetime = mocker.patch('eodslib.datetime')
        self.mock_datetime.utcnow.return_value.isoformat.return_value = 'timestamp'

        self.conn = {
            'domain': 'domainname',
            'username': 'username',
            'access_token': 'token',
        }

    def test_successful_query_return_correct_list_and_df(self, mocker):
        self.mock_get.return_value.content = bytes(
            b'{"meta": {"total_count": 1}, "objects": [{"alternate":"geonode:layername"}]}')

        eods_params = {
            'title': 'keep_api_test_create_group',
        }

        output_list, filtered_df = eodslib.query_catalog(self.conn, **eods_params)

        expected_output_list = ['geonode:layername']
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
        eods_params = {
            'title': 'keep_api_test_create_group',
            'sat_id': 1,
            'find_least_cloud': True
        }
        with pytest.raises(ValueError) as error:
            eodslib.query_catalog(self.conn, **eods_params)

        assert error.value.args[0] == "QUERY failed, you have specified 'sat_id'=1 and 'find_least_cloud'=True. Use 'sat_id'=2 and 'find_least_cloud'=True"

    def test_start_date_no_end_date_trigger_exception(self):
        eods_params = {
            'title': 'keep_api_test_create_group',
            'start_date': '2020-01-01'
        }
        with pytest.raises(ValueError) as error:
            eodslib.query_catalog(self.conn, **eods_params)

        assert error.value.args[0] == "QUERY failed, if querying by date, please specify *BOTH* 'start_date' and 'end_date'"

    def test_end_date_no_start_date_trigger_exception(self):
        eods_params = {
            'title': 'keep_api_test_create_group',
            'end_date': '2020-01-01'
        }
        with pytest.raises(ValueError) as error:
            eodslib.query_catalog(self.conn, **eods_params)

        assert error.value.args[0] == "QUERY failed, if querying by date, please specify *BOTH* 'start_date' and 'end_date'"

    def test_cloud_min_no_cloud_max_trigger_exception(self):
        eods_params = {
            'title': 'keep_api_test_create_group',
            'cloud_min': 1
        }
        with pytest.raises(ValueError) as error:
            eodslib.query_catalog(self.conn, **eods_params)

        assert error.value.args[0] == "QUERY failed, if querying by cloud cover, please specify *BOTH* 'cloud_min' and 'cloud_max'"

    def test_cloud_max_no_cloud_min_trigger_exception(self):
        eods_params = {
            'title': 'keep_api_test_create_group',
            'cloud_max': 1
        }
        with pytest.raises(ValueError) as error:
            eodslib.query_catalog(self.conn, **eods_params)

        assert error.value.args[0] == "QUERY failed, if querying by cloud cover, please specify *BOTH* 'cloud_min' and 'cloud_max'"

    def test_cloud_min_cloud_max_and_sat_id_1_trigger_exception(self):
        eods_params = {
            'title': 'keep_api_test_create_group',
            'cloud_min': 1,
            'cloud_max': 5,
            'sat_id': 1
        }
        with pytest.raises(ValueError) as error:
            eodslib.query_catalog(self.conn, **eods_params)

        assert error.value.args[0] == "QUERY failed, if querying by cloud cover, please specify 'sat_id'=2"

    def test_json_response_meta_total_count_0_return_empty_list_none_df(self, mocker):
        self.mock_get.return_value.content = bytes(
            b'{"meta": {"total_count": 0}}')

        eods_params = {
            'title': 'keep_api_test_create_group',
        }

        output_list, filtered_df = eodslib.query_catalog(self.conn, **eods_params)

        expected_output_list = []
        expected_filtered_df = None

        output_list_bool = output_list == expected_output_list
        filtered_df_bool = filtered_df == expected_filtered_df

        mocker.resetall()

        assert output_list_bool and filtered_df_bool

    def test_non_200_status_trigger_exception(self, mocker):
        self.mock_get.return_value.status_code = 400

        eods_params = {
            'title': 'keep_api_test_create_group',
        }
        with pytest.raises(ValueError) as error:
            eodslib.query_catalog(self.conn, **eods_params)

        expected_error = 'timestamp' + ' :: RESPONSE STATUS = ' + \
            str(self.mock_get.return_value.status_code) + ' (NOT SUCCESSFUL)' + \
            str(self.mock_get.return_value.status_code) + \
            ' :: QUERY URL (CONTAINS SENSITIVE AUTHENTICATION DETAILS, DO NOT SHARE) = ' + 'testurl'

        mocker.resetall()

        assert error.value.args[0] == expected_error

    def test_request_exception_triggered_return_none_prints_error(self, mocker, capsys):
        self.mock_get.side_effect = requests.exceptions.RequestException(
            'test error message')

        eods_params = {
            'title': 'keep_api_test_create_group',
        }

        eodslib.query_catalog(self.conn, **eods_params)

        captured_out = capsys.readouterr().out
        trim_out = captured_out.strip('\n')
        error_message = trim_out

        expected_error = "timestamp :: ERROR, an Exception was raised, no list returned\ntest error message"

        mocker.resetall()

        assert error_message == expected_error

    def test_sat_id_2_return_correct_list_and_df_with_new_cols(self, mocker):
        self.mock_json_normalize = mocker.patch('eodslib.json_normalize')
        df = pd.DataFrame({'title': "S2A_date_lat1lon2_T12ABC_ORB034_etc", "alternate": "geonode:S2A_date_lat1lon2_T12ABC_ORB034_etc",
                           "supplemental_information": "Data Collection Time: time\nARCSI_CLOUD_COVER: 0.12345\netc"}, index=[0])
        self.mock_json_normalize.return_value = df

        self.mock_minimum_cloud = mocker.patch(
            'eodslib.find_minimum_cloud_list')

        self.mock_minimum_cloud.side_effect = return_first_arg_side_effect_fn

        eods_params = {
            'title': 'keep_api_test_create_group',
            'sat_id': 2
        }

        output_list, filtered_df = eodslib.query_catalog(self.conn, **eods_params)

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
        self.mock_json_normalize = mocker.patch('eodslib.json_normalize')
        df = pd.DataFrame({"alternate": "geonode:layername",
                           }, index=[0])
        self.mock_json_normalize.return_value = df

        eods_params = {
            'title': 'keep_api_test_create_group',
            'sat_id': 1
        }

        output_list, filtered_df = eodslib.query_catalog(self.conn, **eods_params)

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
        self.mock_json_normalize = mocker.patch('eodslib.json_normalize')
        df = pd.DataFrame({"alternate": "geonode:layername",
                           }, index=[0])
        self.mock_json_normalize.return_value = df

        eods_params = {
            'title': 'keep_api_test_create_group',
            'sat_id': 3,
            'find_least_cloud': True
        }

        output_list, filtered_df = eodslib.query_catalog(self.conn, **eods_params)

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
        self.mock_json_normalize = mocker.patch('eodslib.json_normalize')
        df = pd.DataFrame({'title': "S2A_date_lat1lon2_T12ABC_ORB034_etc", "alternate": "geonode:S2A_date_lat1lon2_T12ABC_ORB034_etc",
                           "supplemental_information": "Data Collection Time: time\nARCSI_CLOUD_COVER: 0.12345\netc"}, index=[0])
        self.mock_json_normalize.return_value = df

        eods_params = {
            'title': 'keep_api_test_create_group',
            'sat_id': 2,
            'find_least_cloud': False
        }

        output_list, filtered_df = eodslib.query_catalog(self.conn, **eods_params)

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
        self.mock_json_normalize = mocker.patch('eodslib.json_normalize')
        df = pd.DataFrame({'title': "S2A_date_lat1lon2_T12ABC_ORB034_etc", "alternate": "geonode:S2A_date_lat1lon2_T12ABC_ORB034_etc",
                           "supplemental_information": "Data Collection Time: time\nARCSI_CLOUD_COVER: 0.12345\netc"}, index=[0])
        self.mock_json_normalize.return_value = df

        self.mock_minimum_cloud = mocker.patch(
            'eodslib.find_minimum_cloud_list')
        self.mock_minimum_cloud.side_effect = return_first_arg_side_effect_fn

        eods_params = {
            'title': 'keep_api_test_create_group',
            'find_least_cloud': True,
            'sat_id': 2
        }

        output_list, filtered_df = eodslib.query_catalog(self.conn, **eods_params)

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
        self.mock_json_normalize = mocker.patch('eodslib.json_normalize')
        df = pd.DataFrame({'title': ["S2A_date_lat1lon2_T12ABC_ORB034_etc", "S2A_date_lat1lon2_T12ABCSPLIT1_ORB034_etc"],
                           "alternate": ["geonode:S2A_date_lat1lon2_T12ABC_ORB034_etc", "geonode:S2A_date_lat1lon2_T12ABCSPLIT1_ORB034_etc"],
                           "supplemental_information": ["Data Collection Time: time\nARCSI_CLOUD_COVER: 0.1\netc",
                                                        "Data Collection Time: time\nARCSI_CLOUD_COVER: 0.3\netc"],
                           "split_granule.name": ["geonode:S2A_date_lat1lon2_T12ABCSPLIT1_ORB034_etc", "geonode:S2A_date_lat1lon2_T12ABC_ORB034_etc"],
                           })
        self.mock_json_normalize.return_value = df

        self.mock_minimum_cloud = mocker.patch(
            'eodslib.find_minimum_cloud_list')

        self.mock_minimum_cloud.side_effect = return_first_arg_side_effect_fn

        eods_params = {
            'title': 'keep_api_test_create_group',
            'find_least_cloud': True,
            'sat_id': 2
        }

        output_list, filtered_df = eodslib.query_catalog(self.conn, **eods_params)

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
    @pytest.fixture(autouse=True, scope='function')
    def setup(self, mocker):
        self.mock_submit_queue = mocker.patch('eodslib.submit_wps_queue')
        self.mock_submit_queue.return_value = {'job_id': '123',
                                               'timestamp_job_start': datetime(2021, 8, 17), 'timestamp_job_end': datetime(2021, 8, 18),
                                               'job_status': None,
                                               'continue_process': False}

        self.mock_make_output_dir = mocker.patch('eodslib.make_output_dir')

        self.mock_poll = mocker.patch(
            'eodslib.poll_api_status')

        self.mock_process = mocker.patch(
            'eodslib.process_wps_downloaded_files')
        self.mock_process.side_effect = return_first_arg_side_effect_fn

        self.mock_sleep = mocker.patch(
            'eodslib.time.sleep')
        self.mock_sleep.return_value = None

        self.conn = {
            'domain': 'domainname',
            'access_token': 'token',
        }

        self.config_wpsprocess = {}

    def test_all_correct_responses_return_correct_execution_dict(self, mocker):
        self.mock_submit_queue.return_value = {'job_id': '123',
                                               'timestamp_job_start': datetime(2021, 8, 17), 'timestamp_job_end': datetime(2021, 8, 18),
                                               }

        self.mock_make_output_dir.return_value = Path.cwd()

        def poll_side_effect_fn(*args, **kwargs):
            execution_dict = args[0]
            execution_dict['continue_process'] = False
            execution_dict['job_status'] = 'DOWNLOAD-SUCCESSFUL'
            return execution_dict
        
        self.mock_poll.side_effect = poll_side_effect_fn

        execution_dict = eodslib.run_wps(self.conn, self.config_wpsprocess)

        assert execution_dict == {'job_id': '123', 'job_status': 'DOWNLOAD-SUCCESSFUL',
                                  'timestamp_job_start': datetime(2021, 8, 17), 'timestamp_job_end': datetime(2021, 8, 18),
                                  'continue_process': False, 'log_file_path': Path.cwd() / 'wps-log.csv',
                                  'total_job_duration': 1440.0}

    def test_output_dir_not_provided_kwarg_set_as_cwd(self, mocker):
        self.mock_make_output_dir.side_effect = return_first_arg_side_effect_fn

        self.mock_poll.side_effect = return_first_arg_side_effect_fn

        _ = eodslib.run_wps(self.conn, self.config_wpsprocess)

        self.mock_make_output_dir.assert_called_once_with(Path.cwd())

    def test_verify_not_provided_kwarg_set_as_true(self, mocker):
        self.mock_make_output_dir.side_effect = return_first_arg_side_effect_fn

        self.mock_poll.side_effect = return_first_arg_side_effect_fn

        _ = eodslib.run_wps(self.conn, self.config_wpsprocess)

        expected_request_config = {
            'wps_server': self.conn['domain'] + '/geoserver/ows',
            'access_token': self.conn['access_token'],
            'headers': {'Content-type': 'application/xml', 'User-Agent': 'python'},
            'verify': True
        }

        self.mock_submit_queue.assert_called_once_with(
            expected_request_config, self.config_wpsprocess)

    def test_error_in_submit_wps_queue_trigger_quiet_exception(self, mocker, capsys):
        self.mock_submit_queue.return_value = None
        self.mock_submit_queue.side_effect = Exception("testing message")

        _ = eodslib.run_wps(self.conn, self.config_wpsprocess)

        captured = capsys.readouterr()
        error_message = "('testing message',)\nThe WPS submission has failed\n"
        assert captured.out == error_message

    def test_continue_process_false_poll_api_called_once(self, mocker):
        self.mock_make_output_dir.side_effect = return_first_arg_side_effect_fn

        self.mock_poll.side_effect = return_first_arg_side_effect_fn

        _ = eodslib.run_wps(self.conn, self.config_wpsprocess)

        self.mock_poll.assert_called_once()

    def test_poll_api_status_passes_second_try_poll_api_called_twice(self, mocker):
        self.mock_submit_queue.return_value = {'job_id': '123',
                                               'timestamp_job_start': datetime(2021, 8, 17), 'timestamp_job_end': datetime(2021, 8, 18),
                                               'job_status': None,
                                               'continue_process': True}

        def poll_side_effect_fn(*args, **kwargs):
            execution_dict = args[0]
            if execution_dict['continue_process'] == True:
                execution_dict['continue_process'] = 'TryAgain'
            elif execution_dict['continue_process'] == 'TryAgain':
                execution_dict['continue_process'] = False
                execution_dict['job_status'] = 'DOWNLOAD-SUCCESSFUL'
            return execution_dict

        self.mock_make_output_dir.side_effect = return_first_arg_side_effect_fn

        self.mock_poll.side_effect = poll_side_effect_fn

        _ = eodslib.run_wps(self.conn, self.config_wpsprocess)

        assert self.mock_poll.call_count == 2

    def test_poll_api_status_passes_second_try_time_sleep_called(self, mocker):
        self.mock_submit_queue.return_value = {'job_id': '123',
                                               'timestamp_job_start': datetime(2021, 8, 17), 'timestamp_job_end': datetime(2021, 8, 18),
                                               'job_status': None,
                                               'continue_process': True}

        def poll_side_effect_fn(*args, **kwargs):
            execution_dict = args[0]
            if execution_dict['continue_process'] == True:
                execution_dict['continue_process'] = 'TryAgain'
            elif execution_dict['continue_process'] == 'TryAgain':
                execution_dict['continue_process'] = False
                execution_dict['job_status'] = 'DOWNLOAD-SUCCESSFUL'
            return execution_dict

        self.mock_make_output_dir.side_effect = return_first_arg_side_effect_fn

        self.mock_poll.side_effect = poll_side_effect_fn

        _ = eodslib.run_wps(self.conn, self.config_wpsprocess)

        self.mock_sleep.assert_called_once_with(15)

    def test_job_status_download_successful_process_wps_downloaded_files_called_once(self, mocker):
        self.mock_submit_queue = mocker.patch('eodslib.submit_wps_queue')
        self.mock_submit_queue.return_value = {'job_id': '123',
                                               'timestamp_job_start': datetime(2021, 8, 17), 'timestamp_job_end': datetime(2021, 8, 18),
                                               'job_status': 'DOWNLOAD-SUCCESSFUL',
                                               'continue_process': False}

        self.mock_make_output_dir.side_effect = return_first_arg_side_effect_fn

        self.mock_poll.side_effect = return_first_arg_side_effect_fn

        _ = eodslib.run_wps(self.conn, self.config_wpsprocess)

        self.mock_process.assert_called_once()

    def test_job_status_not_successful_process_wps_downloaded_files_not_called(self, mocker):
        self.mock_make_output_dir.side_effect = return_first_arg_side_effect_fn

        self.mock_poll.side_effect = return_first_arg_side_effect_fn

        _ = eodslib.run_wps(self.conn, self.config_wpsprocess)

        self.mock_process.assert_not_called()


class TestSubmitWpsQueue():
    @pytest.fixture(autouse=True, scope='function')
    def setup(self, mocker):
        self.mock_mod_xml = mocker.patch('eodslib.mod_the_xml')
        self.mock_mod_xml.return_value = None

        self.mock_datetime = mocker.patch('eodslib.datetime')
        self.mock_datetime.utcnow.return_value = datetime(2021, 8, 17)

        self.url = 'https://domain'

        self.config_wpsprocess = {
            'xml_config': {
                'template_layer_name': 'layername',
            },
        }

        self.request_config = {
            'wps_server': self.url,
            'access_token': 'token',
            'headers': {'header': 'a_header'},
            'verify': 'verify'
        }
    @responses.activate
    def test_successful_post_return_correct_execution_dict(self, mocker):
        responses.add(responses.POST, self.url, status=200,
                      body='Body executionId=123')

        response = eodslib.submit_wps_queue(self.request_config, self.config_wpsprocess)

        assert response == {'job_id': '123', 'layer_name': 'layername', 'timestamp_job_start': datetime(
            2021, 8, 17, 0, 0), 'continue_process': True}

    @responses.activate
    def test_raise_for_status_trigger_quiet_exception(self, mocker, capsys):
        responses.add(responses.POST, self.url, status=400)

        _ = eodslib.submit_wps_queue(self.request_config, self.config_wpsprocess)

        captured = capsys.readouterr()
        expected_error_message = ("\n\t\t### 2021-08-17T00:00:00 :: WPS SUBMISSION :: lyr=layername\n"
                                  "2021-08-17T00:00:00 :: WPS submission failed :: check log for errors (CONTAINS SENSITIVE AUTHENTICATION DETAILS, DO NOT SHARE) = "
                                  "('non-200 response, additional info (MAY CONTAIN SENSITIVE AUTHENTICATION DETAILS, DO NOT SHARE)', "
                                  "'400 Client Error: Bad Request for url: https://domain/?access_token=token&SERVICE=WPS&VERSION=1.0.0&REQUEST=EXECUTE')\n")

        assert captured.out == expected_error_message

    @responses.activate
    def test_raise_for_status_return_correct_exception(self, mocker, capsys):
        responses.add(responses.POST, self.url, status=400)

        error = eodslib.submit_wps_queue(self.request_config, self.config_wpsprocess)

        expected_error_message = ('non-200 response, additional info (MAY CONTAIN SENSITIVE AUTHENTICATION DETAILS, DO NOT SHARE)',
                                  '400 Client Error: Bad Request for url: https://domain/?access_token=token&SERVICE=WPS&VERSION=1.0.0&REQUEST=EXECUTE')

        assert type(error) is type(
            ValueError()) and error.args == expected_error_message

    @responses.activate
    def test_wps_submission_fail_trigger_quiet_exception(self, mocker, capsys):
        responses.add(responses.POST, self.url, status=200,
                      body='Body ExceptionReport executionId=123')

        _ = eodslib.submit_wps_queue(self.request_config, self.config_wpsprocess)

        captured = capsys.readouterr()
        expected_error_message = ("\n\t\t### 2021-08-17T00:00:00 :: WPS SUBMISSION :: lyr=layername\n"
                                  "2021-08-17T00:00:00 :: WPS submission failed :: check log for errors (CONTAINS SENSITIVE AUTHENTICATION DETAILS, DO NOT SHARE) = "
                                  "('wps server returned an exception', 'Body ExceptionReport executionId=123')\n")

        assert captured.out == expected_error_message

    @responses.activate
    def test_wps_submission_fail_return_correct_exception(self, mocker):
        responses.add(responses.POST, self.url, status=200,
                      body='Body ExceptionReport executionId=123')

        error = eodslib.submit_wps_queue(self.request_config, self.config_wpsprocess)

        expected_error_message = ('wps server returned an exception',
                                  'Body ExceptionReport executionId=123')

        assert type(error) is type(
            ValueError()) and error.args == expected_error_message

    @responses.activate
    def test_all_correct_responses_mod_the_xml_called_once(self, mocker):
        responses.add(responses.POST, self.url, status=200,
                      body='Body ExceptionReport executionId=123')

        _ = eodslib.submit_wps_queue(self.request_config, self.config_wpsprocess)

        self.mock_mod_xml.assert_called_once_with(self.config_wpsprocess)

    @responses.activate
    def test_all_correct_responses_raise_for_status_called_once(self, mocker):
        responses.add(responses.POST, self.url, status=200,
                      body='Body executionId=123')

        self.mock_raise_for_status = mocker.patch.object(
            requests.Response, 'raise_for_status')
        self.mock_raise_for_status.return_value = None

        _ = eodslib.submit_wps_queue(self.request_config, self.config_wpsprocess)

        self.mock_raise_for_status.assert_called_once_with()


class TestPollApiStatus():
    @pytest.fixture(autouse=True, scope='function')
    def setup(self, mocker):
        self.mock_datetime = mocker.patch('eodslib.datetime')
        self.mock_datetime.utcnow.return_value = datetime(2021, 8, 17)

        self.mock_get = mocker.patch('eodslib.requests.get')

        self.mock_download_single = mocker.patch(
            'eodslib.download_wps_result_single')

        self.mock_download_single.side_effect = return_second_arg_side_effect_fn

        self.request_config = {
            'wps_server': 'https://domain',
            'access_token': 'token',
            'headers': {'header': 'a_header'},
            'verify': 'verify'
        }
    
    def test_all_correct_responses_return_correct_execution_dict(self, mocker):
        self.mock_get.return_value.content = bytes(
            b'<?xml version="1.0" encoding="UTF-8"?><wps:ExecuteResponse xmlns:wps="http://www.opengis.net/wps/1.0.0"><wps:Status><wps:ProcessSucceeded></wps:ProcessSucceeded></wps:Status><wps:ProcessOutputs><wps:Output><wps:Reference href="href" mimeType="mime"/></wps:Output></wps:ProcessOutputs></wps:ExecuteResponse>'
        )

        execution_dict = {'job_id': '123', 'timestamp_job_start': datetime(
            2021, 8, 17, 0, 0), 'continue_process': True}

        execution_dict = eodslib.poll_api_status(
            execution_dict, self.request_config, None)

        expected_execution_dict = {'job_id': '123',
                                   'timestamp_job_start': datetime(2021, 8, 17, 0, 0),
                                   'continue_process': True,
                                   'dl_url': 'href&access_token=token',
                                   'job_status': 'READY-TO-DOWNLOAD',
                                   'mime_type': 'mime',
                                   'timestamp_ready_to_dl': datetime(2021, 8, 17, 0, 0)}

        assert execution_dict == expected_execution_dict

    def test_requests_get_exception_return_correct_execution_dict(self, mocker):
        self.mock_get.side_effect = Exception('Test Exception')

        execution_dict = {'job_id': '123', 'continue_process': True}

        execution_dict = eodslib.poll_api_status(
            execution_dict, self.request_config, None)

        expected_execution_dict = {'job_id': '123',
                                   'continue_process': False,
                                   'job_status': 'UNKNOWN-GENERAL-ERROR',
                                   'message': 'UNKNOWN GENERAL ERROR ENCOUNTERED WHEN CHECKING STATUS OF WPS JOB. ERROR MESSAGE:Test Exception',
                                   'timestamp_job_end': datetime(2021, 8, 17, 0, 0)}

        assert execution_dict == expected_execution_dict

    def test_continue_process_false_no_exception_report_return_input_execution_dict(self, mocker):
        # request_config = {} # Look at!

        execution_dict = {'continue_process': False}

        execution_dict = eodslib.poll_api_status(
            execution_dict, self.request_config, None)

        expected_execution_dict = {'continue_process': False}

        assert execution_dict == expected_execution_dict

    def test_no_execute_response_no_exception_report_return_input_execution_dict(self, mocker):
        self.mock_xml_dict_parse = mocker.patch('eodslib.xmltodict.parse')

        execution_dict = {'job_id': '123', 'continue_process': True}

        execution_dict = eodslib.poll_api_status(
            execution_dict, self.request_config, None)

        expected_execution_dict = {'job_id': '123',
                                   'continue_process': True,
                                   }

        assert execution_dict == expected_execution_dict

    def test_no_execute_response_with_exception_report_return_correct_execution_dict(self, mocker):
        self.mock_xml_dict_parse = mocker.patch('eodslib.xmltodict.parse')
        self.mock_xml_dict_parse.return_value = {'ows:ExceptionReport': {
            'ows:Exception': {'ows:ExceptionText': 'Error Test'}}}

        execution_dict = {'job_id': '123', 'continue_process': True}

        execution_dict = eodslib.poll_api_status(
            execution_dict, self.request_config, None)

        expected_execution_dict = {'job_id': '123',
                                   'continue_process': False,
                                   'job_status': 'WPS-GENERAL-ERROR',
                                   'message': 'THIS IS A GENERAL ERROR WITH A WPS JOB. ERROR MESSAGE = Error Test',
                                   'timestamp_job_end': datetime(2021, 8, 17, 0, 0)
                                   }

        assert execution_dict == expected_execution_dict

    def test_with_execute_response_no_process_succeeded_with_process_failed_return_correct_execution_dict(self, mocker):
        self.mock_xml_dict_parse = mocker.patch('eodslib.xmltodict.parse')
        self.mock_xml_dict_parse.return_value = {'wps:ExecuteResponse': {
            'wps:Status': {'wps:ProcessFailed': None}}}

        execution_dict = {'job_id': '123', 'continue_process': True}

        execution_dict = eodslib.poll_api_status(
            execution_dict, self.request_config, None)

        expected_execution_dict = {'job_id': '123',
                                   'continue_process': False,
                                   'job_status': 'WPS-FAILURE',
                                   'message': 'GEOSERVER FAILURE REPORT',
                                   'timestamp_job_end': datetime(2021, 8, 17, 0, 0)
                                   }

        assert execution_dict == expected_execution_dict

    def test_with_execute_response_no_process_succeeded_no_process_failed_return_correct_execution_dict(self, mocker):
        self.mock_xml_dict_parse = mocker.patch('eodslib.xmltodict.parse')
        self.mock_xml_dict_parse.return_value = {
            'wps:ExecuteResponse': {'wps:Status': {'key': 'value'}}}

        execution_dict = {'job_id': '123', 'continue_process': True}

        execution_dict = eodslib.poll_api_status(
            execution_dict, self.request_config, None)

        expected_execution_dict = {'job_id': '123',
                                   'continue_process': True,
                                   'job_status': 'OUTSTANDING',
                                   }

        assert execution_dict == expected_execution_dict

    def test_all_correct_responses_download_wps_result_single_correctly_called_once(self, mocker):
        self.mock_get.return_value.content = bytes(
            b'<?xml version="1.0" encoding="UTF-8"?><wps:ExecuteResponse xmlns:wps="http://www.opengis.net/wps/1.0.0"><wps:Status><wps:ProcessSucceeded></wps:ProcessSucceeded></wps:Status><wps:ProcessOutputs><wps:Output><wps:Reference href="href" mimeType="mime"/></wps:Output></wps:ProcessOutputs></wps:ExecuteResponse>'
        )

        execution_dict = {'job_id': '123', 'timestamp_job_start': datetime(
            2021, 8, 17, 0, 0), 'continue_process': True}

        execution_dict = eodslib.poll_api_status(
            execution_dict, self.request_config, None)

        expected_execution_dict = {'job_id': '123',
                                   'timestamp_job_start': datetime(2021, 8, 17, 0, 0),
                                   'continue_process': True,
                                   'dl_url': 'href&access_token=token',
                                   'job_status': 'READY-TO-DOWNLOAD',
                                   'mime_type': 'mime',
                                   'timestamp_ready_to_dl': datetime(2021, 8, 17, 0, 0)}

        self.mock_download_single.assert_called_once_with(
            self.request_config, expected_execution_dict, None)

    def test_with_continue_process_requests_get_correctly_called_once(self, mocker):
        self.mock_xml_dict_parse = mocker.patch('eodslib.xmltodict.parse')

        execution_dict = {'job_id': '123', 'continue_process': True}

        _ = eodslib.poll_api_status(
            execution_dict, self.request_config, None)

        self.mock_get.assert_called_once_with('https://domain', params={'access_token': 'token',
                                                                        'SERVICE': 'WPS',
                                                                        'VERSION': '1.0.0',
                                                                        'REQUEST': 'GetExecutionstatus',
                                                                        'EXECUTIONID': '123'},
                                              headers={'header': 'a_header'}, verify='verify')

    def test_with_continue_process_xml_to_dict_parse_correctly_called_once(self, mocker):
        self.mock_get.return_value.content = 'get content'

        self.mock_xml_dict_parse = mocker.patch('eodslib.xmltodict.parse')

        execution_dict = {'job_id': '123', 'continue_process': True}

        _ = eodslib.poll_api_status(
            execution_dict, self.request_config, None)

        self.mock_xml_dict_parse.assert_called_once_with('get content')


class TestDownloadWpsResultSingle():
    @pytest.fixture(autouse=True, scope='function')
    def setup(self, mocker):
        self.mock_datetime = mocker.patch('eodslib.datetime')
        self.mock_datetime.utcnow.return_value = datetime(2021, 8, 17)

        self.mock_mkdir = mocker.patch('eodslib.Path.mkdir')
        self.mock_mkdir.return_value = None

        self.mock_get = mocker.patch('eodslib.requests.get')
        self.mock_get.return_value.__enter__.return_value.iter_content.return_value = []

        self.mock_open = mocker.patch(
            'builtins.open', mocker.mock_open())

        self.execution_dict = {'job_id': '123', 'layer_name': 'geonode:layername',
                          'mime_type': '/mime',
                          'dl_url': ''}

        self.request_config = {
            'headers': 'header',
            'verify': 'verify'
        }

    def test_successful_get_return_correct_execution_dict(self, mocker):
        execution_dict = eodslib.download_wps_result_single(
            self.request_config, self.execution_dict, Path.cwd())

        expected_execution_dict = {'job_id': '123', 'layer_name': 'geonode:layername',
                                   'continue_process': False,
                                   'dl_url': '',
                                   'job_status': 'DOWNLOAD-SUCCESSFUL',
                                   'mime_type': '/mime',
                                   'dl_file': Path.cwd() / 'layername' / 'layername.mime',
                                   'file_extension': '.mime',
                                   'filename_stub': 'layername',
                                   'timestamp_dl_end': datetime(2021, 8, 17, 0, 0),
                                   'timestamp_job_end': datetime(2021, 8, 17, 0, 0),
                                   'download_try': 1}

        assert execution_dict == expected_execution_dict

    def test_successful_get_correct_with_open_as_f_calls(self, mocker):
        self.mock_get.return_value.__enter__.return_value.iter_content.return_value = [
            'te', 'st']

        eodslib.download_wps_result_single(
            self.request_config, self.execution_dict, Path.cwd()
        )

        calls = [
            mocker.call(Path.cwd() / 'layername' / 'layername.mime', 'wb'),
            mocker.call().__enter__(),
            mocker.call().write('te'),
            mocker.call().write('st'),
            mocker.call().__exit__(None, None, None)]

        assert self.mock_open.mock_calls == calls

    def test_failed_get_return_correct_execution_dict(self, mocker):
        self.mock_get.side_effect = Exception('Error message')

        execution_dict = eodslib.download_wps_result_single(
            self.request_config, self.execution_dict, Path.cwd()
        )

        expected_execution_dict = {'job_id': '123', 'layer_name': 'geonode:layername',
                                   'mime_type': '/mime',
                                   'dl_url': '',
                                   'job_status': 'DOWNLOAD-FAILED',
                                   'continue_process': False,
                                   'message': 'Error message',
                                   'timestamp_job_end': datetime(2021, 8, 17, 0, 0),
                                   'download_try': 3
                                   }

        assert execution_dict == expected_execution_dict

    def test_failed_get_get_called_three_times(self, mocker):
        self.mock_get.side_effect = Exception('Error message')

        eodslib.download_wps_result_single(
            self.request_config, self.execution_dict, Path.cwd()
        )

        assert self.mock_get.call_count == 3

    def test_failed_get_get_has_correct_calls(self, mocker):
        self.mock_get.side_effect = Exception('Error message')

        eodslib.download_wps_result_single(
            self.request_config, self.execution_dict, Path.cwd()
        )

        expected_calls = [
            mocker.call('', headers='header', verify='verify', stream=True),
            mocker.call('', headers='header', verify='verify', stream=True),
            mocker.call('', headers='header', verify='verify', stream=True)
            ]

        self.mock_get.assert_has_calls(expected_calls)

    def test_successful_get_get_called_once(self, mocker):
        eodslib.download_wps_result_single(
            self.request_config, self.execution_dict, Path.cwd()
        )

        assert self.mock_get.call_count == 1

    def test_successful_get_get_has_correct_call(self, mocker):
        eodslib.download_wps_result_single(
            self.request_config, self.execution_dict, Path.cwd()
        )

        self.mock_get.assert_called_once_with('', headers='header', verify='verify', stream=True)



class TestProcessWpsDownloadedFiles():
    # real zip path manip
    # /mnt/c/Users/henry.wild/repos/EODS/eodslib/EODS-API/tests/output/2021-08-18T151428Z/keep_api_test_create_group/keep_api_test_create_group.zip # source_file_to_extract
    # /mnt/c/Users/henry.wild/repos/EODS/eodslib/EODS-API/tests/output/2021-08-18T151428Z/keep_api_test_create_group # source_file_to_extract.parent
    # /mnt/c/Users/henry.wild/repos/EODS/eodslib/EODS-API/tests/output/2021-08-18T151428Z # source_file_to_extract.parent.parent
    # /mnt/c/Users/henry.wild/repos/EODS/eodslib/EODS-API/tests/output/2021-08-18T151428Z/ef5b64d1-df19-4365-b735-54ce35cf95e2.tiff # f_path
    # keep_api_test_create_group # filename_stub
    # /mnt/c/Users/henry.wild/repos/EODS/eodslib/EODS-API/tests/output/2021-08-18T151428Z/keep_api_test_create_group.tiff # final path

    # test zip path manip
    # source/parent/filename.zip # source_file_to_extract
    # source/parent/ # source_file_to_extract.parent
    # source/ # source_file_to_extract.parent.parent
    # source/alphanumstr.tiff # f_path
    # layername # filename_stub
    # source/layername.tiff # final path

    # test not zip path manip
    # source/parent/filename.txt # source_file_to_extract
    # source/parent/ # source_file_to_extract.parent
    # source/ # source_file_to_extract.parent.parent
    # layername # filename_stub
    # source/layername.txt # final path
    def test_successful_get_zip_file_return_correct_execution_dict(self, mocker):
        self.mock_datetime = mocker.patch('eodslib.datetime')
        self.mock_datetime.utcnow.return_value = datetime(2021, 8, 17)

        self.mock_zip = mocker.patch('eodslib.ZipFile')

        class zipmock():
            def __init__(self):
                info = ZipInfo(filename='alphanumstr.tiff')
                info.file_size = 1
                info.compress_size = 1
                self.filelist = [info]

            def extractall(self, _):
                return None

            def close(self):
                return None

        self.mock_zip.return_value = zipmock()

        self.mock_unlink = mocker.patch.object(Path, 'unlink')
        self.mock_replace = mocker.patch.object(Path, 'replace')

        execution_dict = {'job_id': '123',
                          'dl_file': Path('source/parent/filename.zip'),
                          'filename_stub': 'layername'}

        execution_dict = eodslib.process_wps_downloaded_files(execution_dict)

        expected_execution_dict = {'job_id': '123',
                                   'job_status': 'LOCAL-POST-PROCESSING-SUCCESSFUL',
                                   'dl_file': Path('source/parent/filename.zip'),
                                   'filename_stub': 'layername',
                                   'timestamp_extraction_end': datetime(2021, 8, 17, 0, 0),
                                   'timestamp_job_end': datetime(2021, 8, 17, 0, 0),
                                   }

        assert execution_dict == expected_execution_dict

    def test_successful_get_not_zip_file_return_correct_execution_dict(self, mocker):
        self.mock_datetime = mocker.patch('eodslib.datetime')
        self.mock_datetime.utcnow.return_value = datetime(2021, 8, 17)

        self.mock_rename = mocker.patch.object(Path, 'rename')

        execution_dict = {'job_id': '123',
                          'dl_file': Path('source/parent/filename.txt'),
                          'filename_stub': 'layername'}

        execution_dict = eodslib.process_wps_downloaded_files(execution_dict)

        expected_execution_dict = {'job_id': '123',
                                   'job_status': 'LOCAL-POST-PROCESSING-SUCCESSFUL',
                                   'dl_file': Path('source/parent/filename.txt'),
                                   'filename_stub': 'layername',
                                   'timestamp_extraction_end': datetime(2021, 8, 17, 0, 0),
                                   'timestamp_job_end': datetime(2021, 8, 17, 0, 0),
                                   }

        assert execution_dict == expected_execution_dict

    def test_successful_get_zip_file_sld_f_path_unlinked(self, mocker):
        self.mock_datetime = mocker.patch('eodslib.datetime')
        self.mock_datetime.utcnow.return_value = datetime(2021, 8, 17)

        self.mock_zip = mocker.patch('eodslib.ZipFile')

        class zipmock():
            def __init__(self):
                info = ZipInfo(filename='alphanumstr.sld')
                info.file_size = 1
                info.compress_size = 1
                self.filelist = [info]

            def extractall(self, _):
                return None

            def close(self):
                return None

        self.mock_zip.return_value = zipmock()

        self.mock_unlink = mocker.patch.object(Path, 'unlink')

        execution_dict = {'job_id': '123',
                          'dl_file': Path('source/parent/filename.zip'),
                          'filename_stub': 'layername'}

        eodslib.process_wps_downloaded_files(execution_dict)

        assert self.mock_unlink.call_count == 2

    def test_successful_get_zip_file_sld_f_path_not_replaced(self, mocker):
        self.mock_datetime = mocker.patch('eodslib.datetime')
        self.mock_datetime.utcnow.return_value = datetime(2021, 8, 17)

        self.mock_zip = mocker.patch('eodslib.ZipFile')

        class zipmock():
            def __init__(self):
                info = ZipInfo(filename='alphanumstr.sld')
                info.file_size = 1
                info.compress_size = 1
                self.filelist = [info]

            def extractall(self, _):
                return None

            def close(self):
                return None

        self.mock_zip.return_value = zipmock()

        self.mock_unlink = mocker.patch.object(Path, 'unlink')
        self.mock_replace = mocker.patch.object(Path, 'replace')

        execution_dict = {'job_id': '123',
                          'dl_file': Path('source/parent/filename.zip'),
                          'filename_stub': 'layername'}

        eodslib.process_wps_downloaded_files(execution_dict)

        self.mock_replace.assert_not_called()

    def test_successful_get_zip_file_sld_f_path_not_renamed(self, mocker):
        self.mock_datetime = mocker.patch('eodslib.datetime')
        self.mock_datetime.utcnow.return_value = datetime(2021, 8, 17)

        self.mock_zip = mocker.patch('eodslib.ZipFile')

        class zipmock():
            def __init__(self):
                info = ZipInfo(filename='alphanumstr.sld')
                info.file_size = 1
                info.compress_size = 1
                self.filelist = [info]

            def extractall(self, _):
                return None

            def close(self):
                return None

        self.mock_zip.return_value = zipmock()

        self.mock_unlink = mocker.patch.object(Path, 'unlink')
        self.mock_rename = mocker.patch.object(Path, 'rename')

        execution_dict = {'job_id': '123',
                          'dl_file': Path('source/parent/filename.zip'),
                          'filename_stub': 'layername'}

        eodslib.process_wps_downloaded_files(execution_dict)

        self.mock_rename.assert_not_called()

    def test_successful_get_zip_file_not_sld_f_path_correctly_replaced(self, mocker):
        self.mock_datetime = mocker.patch('eodslib.datetime')
        self.mock_datetime.utcnow.return_value = datetime(2021, 8, 17)

        self.mock_zip = mocker.patch('eodslib.ZipFile')

        class zipmock():
            def __init__(self):
                info = ZipInfo(filename='alphanumstr.txt')
                info.file_size = 1
                info.compress_size = 1
                self.filelist = [info]

            def extractall(self, _):
                return None

            def close(self):
                return None

        self.mock_zip.return_value = zipmock()

        self.mock_unlink = mocker.patch.object(Path, 'unlink')
        self.mock_replace = mocker.patch.object(Path, 'replace')

        execution_dict = {'job_id': '123',
                          'dl_file': Path('source/parent/filename.zip'),
                          'filename_stub': 'layername'}

        eodslib.process_wps_downloaded_files(execution_dict)

        self.mock_replace.assert_called_once_with(Path('source/layername.txt'))

    def test_successful_get_zip_file_not_sld_f_path_not_unlinked(self, mocker):
        self.mock_datetime = mocker.patch('eodslib.datetime')
        self.mock_datetime.utcnow.return_value = datetime(2021, 8, 17)

        self.mock_zip = mocker.patch('eodslib.ZipFile')

        class zipmock():
            def __init__(self):
                info = ZipInfo(filename='alphanumstr.txt')
                info.file_size = 1
                info.compress_size = 1
                self.filelist = [info]

            def extractall(self, _):
                return None

            def close(self):
                return None

        self.mock_zip.return_value = zipmock()

        self.mock_unlink = mocker.patch.object(Path, 'unlink')
        self.mock_replace = mocker.patch.object(Path, 'replace')

        execution_dict = {'job_id': '123',
                          'dl_file': Path('source/parent/filename.zip'),
                          'filename_stub': 'layername'}

        eodslib.process_wps_downloaded_files(execution_dict)

        assert self.mock_unlink.call_count == 1

    def test_successful_get_zip_file_not_sld_source_path_not_renamed(self, mocker):
        self.mock_datetime = mocker.patch('eodslib.datetime')
        self.mock_datetime.utcnow.return_value = datetime(2021, 8, 17)

        self.mock_zip = mocker.patch('eodslib.ZipFile')

        class zipmock():
            def __init__(self):
                info = ZipInfo(filename='alphanumstr.txt')
                info.file_size = 1
                info.compress_size = 1
                self.filelist = [info]

            def extractall(self, _):
                return None

            def close(self):
                return None

        self.mock_zip.return_value = zipmock()

        self.mock_unlink = mocker.patch.object(Path, 'unlink')
        self.mock_replace = mocker.patch.object(Path, 'replace')
        self.mock_rename = mocker.patch.object(Path, 'rename')

        execution_dict = {'job_id': '123',
                          'dl_file': Path('source/parent/filename.zip'),
                          'filename_stub': 'layername'}

        eodslib.process_wps_downloaded_files(execution_dict)

        self.mock_rename.assert_not_called()

    def test_successful_get_not_zip_file_source_path_correctly_renamed(self, mocker):
        self.mock_datetime = mocker.patch('eodslib.datetime')
        self.mock_datetime.utcnow.return_value = datetime(2021, 8, 17)

        self.mock_rename = mocker.patch.object(Path, 'rename')

        execution_dict = {'job_id': '123',
                          'dl_file': Path('source/parent/filename.txt'),
                          'filename_stub': 'layername'}

        eodslib.process_wps_downloaded_files(execution_dict)

        self.mock_rename.assert_called_once_with(Path('source/layername.txt'))

    def test_successful_get_not_zip_file_zipfile_not_called(self, mocker):
        self.mock_datetime = mocker.patch('eodslib.datetime')
        self.mock_datetime.utcnow.return_value = datetime(2021, 8, 17)

        self.mock_zip = mocker.patch('eodslib.ZipFile')

        self.mock_rename = mocker.patch.object(Path, 'rename')

        execution_dict = {'job_id': '123',
                          'dl_file': Path('source/parent/filename.txt'),
                          'filename_stub': 'layername'}

        eodslib.process_wps_downloaded_files(execution_dict)

        self.mock_zip.assert_not_called()

    def test_successful_get_source_file_parent_is_dir_parent_is_rm(self, mocker):
        self.mock_datetime = mocker.patch('eodslib.datetime')
        self.mock_datetime.utcnow.return_value = datetime(2021, 8, 17)

        self.mock_zip = mocker.patch('eodslib.ZipFile')

        self.mock_rename = mocker.patch.object(Path, 'rename')

        self.mock_is_dir = mocker.patch.object(Path, 'is_dir')
        self.mock_is_dir.return_value = True

        self.mock_rmdir = mocker.patch.object(Path, 'rmdir')

        execution_dict = {'job_id': '123',
                          'dl_file': Path('source/parent/filename.txt'),
                          'filename_stub': 'layername'}

        eodslib.process_wps_downloaded_files(execution_dict)

        self.mock_rmdir.assert_called_once()

    def test_successful_get_source_file_parent_is_not_dir_parent_is_not_rm(self, mocker):
        self.mock_datetime = mocker.patch('eodslib.datetime')
        self.mock_datetime.utcnow.return_value = datetime(2021, 8, 17)

        self.mock_zip = mocker.patch('eodslib.ZipFile')

        self.mock_rename = mocker.patch.object(Path, 'rename')

        self.mock_is_dir = mocker.patch.object(Path, 'is_dir')
        self.mock_is_dir.return_value = False

        self.mock_rmdir = mocker.patch.object(Path, 'rmdir')

        execution_dict = {'job_id': '123',
                          'dl_file': Path('source/parent/filename.txt'),
                          'filename_stub': 'layername'}

        eodslib.process_wps_downloaded_files(execution_dict)

        self.mock_rmdir.assert_not_called()
        

class TestOutputLog():
    def test_successful_get_return_correct_execution_dict(self, mocker):
        self.mock_datetime = mocker.patch('eodslib.datetime')
        self.mock_datetime.utcnow.return_value = datetime(2021, 8, 17)

        self.mock_to_csv = mocker.patch.object(pd.DataFrame, 'to_csv')

        list_of_result = [{'log_file_path': Path.cwd()}]

        eodslib.output_log(list_of_result)

        self.mock_to_csv.assert_called_once_with(Path.cwd(), index_label='num')

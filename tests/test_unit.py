import eodslib
import os
import pytest
import requests
import responses

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

        response_json = eodslib.post_to_layer_group_api(conn, url, the_json)

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
        
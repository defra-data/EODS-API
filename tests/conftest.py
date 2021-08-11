from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime

import pytest
import sys
import os
import random
import string
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
import eodslib


def pytest_addoption(parser):
    parser.addoption("--env", dest="env", required=True, action="store", choices=[
                     "DEV", "AGW", "SND", "PRE", "PRD"], help="The selected tenancy: DEV, AGW, SND, PRE, or PRD")
    parser.addoption("--mod-id", dest="mod-id", default=0, type=int, action="store", help="The integer id of the group updated by test_modify_group.py, required if test_create_group.py is not also run prior. 0 is a reserved value.")
    parser.addoption("--skip-real", dest="skip-real", action="store_true", help="")

@pytest.fixture(autouse=True, scope='session')
def env(request):
    return request.config.getoption("--env")

@pytest.fixture(autouse=True, scope='session')
def run_id():
    id = ''.join(random.choice(string.ascii_uppercase) for i in range(10))
    return id

@pytest.fixture(autouse=True, scope='session')
def time_started():
    time = datetime.now().strftime("%d/%m/%Y-%H:%M:%S")
    return time

@pytest.fixture(autouse=True, scope='session')
def unique_run_string(run_id, time_started):
    unique_string = '-'.join([run_id, time_started])
    return unique_string

@pytest.fixture(autouse=True, scope='session')
def print_unique_run_string(unique_run_string):
    print("This run's unique ID string is:", unique_run_string)

@pytest.fixture(scope='session', autouse=True)
def setup(env):
    # import settings and ENVIRONMENT VARIABLES
    # config_env = settings.creds[env]
    # print(env)
    base_eodslib_path = Path(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    env_file_name = env + '.env'
    load_dotenv(dotenv_path =
                base_eodslib_path / env_file_name
                )
    # load_dotenv(dotenv_path = '.env.' + env + '.secret')
    yield None

@pytest.fixture(scope='session')
def set_output_dir():
        output_dir = eodslib.make_output_dir(Path(os.path.dirname(os.path.realpath(__file__))) / 'output' / datetime.utcnow().strftime(
            "%Y-%m-%dT%H%M%SZ"))
        yield output_dir

@pytest.fixture(scope='session', autouse=True)
def modify_id_list():
    return []

@pytest.fixture(scope='session', autouse=True)
def set_modify_id(modify_id_list, request):
    parsed_id = request.config.getoption("--mod-id")
    if parsed_id != 0:
        modify_id_list.append(parsed_id)

@pytest.fixture(autouse=True)
def skip_by_real(request):
    if request.config.getoption("--skip-real"):
        if request.node.get_closest_marker('skip_real'):
            pytest.skip('Skipping tests that hit the real API server.')

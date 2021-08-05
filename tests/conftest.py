from dotenv import load_dotenv
from pathlib import Path

import pytest
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
import eodslib


def pytest_addoption(parser):
    parser.addoption("--env", dest="env", required=True, action="store", choices=[
                     "DEV", "AGW", "SND", "PRE", "PRD"], help="The selected tenancy: DEV, AGW, SND, PRE, or PRD")

@pytest.fixture(autouse=True, scope='session')
def env(request):
    return request.config.getoption("--env")

@pytest.fixture(scope='session', autouse=True)
def setup(env):
    # import settings and ENVIRONMENT VARIABLES
    # config_env = settings.creds[env]
    # print(env)
    load_dotenv(dotenv_path =
                '../' + 
                env + '.env'
                )
    # load_dotenv(dotenv_path = '.env.' + env + '.secret')

    output_dir = eodslib.make_output_dir(Path.cwd() / 'output')

    yield output_dir

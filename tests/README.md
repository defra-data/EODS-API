# EODS-API/tests
This repo provides a set of pytest tests for the eodslib module.

# Building your local environment

Build your local environment as described in the README of the root repo.

# Handling your EODS credentials

* You will need to authenticate using your EODS username and api token, note your token is different to your password.
* Credentials for each environment (AGW, SND, PRE, PRD) are entered into an Environment file (`<env-code>.env`), located in the root of the repo. A sample file is provided:
```bash
$ cat sample.env
HOST=environmentdomain
API_USER=someuser
API_TOKEN=sometoken
```
* Either override the defaults with your own credentials, or create a new `.env` file with your own credentials.

# Running the tests

## Running all tests

* In the root repo run this command in your terminal, replacing <env-code> with the three-letter code of the environment you are testing:
```bash
pytest tests/. --env <env-code> 2>&1 | tee ./tests/output/eodslib_test_output.txt
```
    * This will create a date and time-stamped subdirectory in the tests/output directory containing all eods query csvs, the downloaded tiff files, and wps logs, while a txt file containing the terminal output (eodslib_test_output.txt) will saved to the tests/output directory.

* If you wish to run this from the tests subdirectory, run this command instead:
```bash
pytest . --env <env-code> 2>&1 | tee ./output/eodslib_test_output.txt
```

* The whole test suite should take under 1 minute to complete.

### Arguments
* --env: This is a required value which dictates which environment is tested, and specifically determines which environment file is opened to access the host, username, and access token variables stored within. The available options are: DEV, AGW, SND, PRE, or PRD.
* --mod-id: This is a conditionally-required integer value when running `test_modify_group.py`. This value is the id of the layer group being modified by the test. This value is only not required if you are also running `test_create_group.py` during the same run, in which case the group created by that test is the default group modified. If you run both `test_create_group.py` and `test_modify_group.py` and also parse in --mod-id then the group specified by --mod-id will be the one modified.

### Flags

* --skip-real: This skips all tests which actually touch the API endpoints. This includes 4 unit tests for the post_to_layer_group_api function, as well as all 5 of the end-to-end tests.

## Running just the unit tests

Follow the same steps as for running all tests, but if running from the root replace `tests/.` with  `tests/test_unit.py`, and if running from the tests subdirectory replace `.` with `test_unit.py`.

# ToDo

* Add tests for find_minimum_cloud_list & query_catlog for when split with missing counterpart eg due to geometry search
* Add end-to-end tests for wps fns along with mod_the_xml, output_log, and make_output_dir?

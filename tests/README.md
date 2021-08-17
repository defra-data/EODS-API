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

In the root repo run this command in your terminal, replacing <env-code> with the three-letter code of the environment you are testing:
```bash
pytest tests/. --env <env-code> -vvv -rA 2>&1 | tee ./tests/output/eodslib_test_output.txt
```
This will create and date and time-stamped subdirectory in the tests/output directory containing all eods query csvs, the downloaded tiff files, and wps logs, while a txt file containing the terminal output (eodslib_test_output.txt) will saved to the tests/output directory.

If you wish to run this from the tests subdirectory, run this command instead:
```bash
pytest . --env <env-code> -vvv -rA 2>&1 | tee ./output/eodslib_test_output.txt
```

# ToDo

* Add tests for find_minimum_cloud_list & query_catlog for when split with missing counterpart eg due to geometry search
* Add tests for wps fns along with mod_the_xml, output_log, and make_output_dir
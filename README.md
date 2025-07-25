# Vista_Equity_REST_API
Fivetran Table Configuration Automation
This repository contains two Python scripts to automate the management of table and column configurations in Fivetran using the REST API.

## 📄 Overview
- Goal of this project was to have the flow:
  - Create a connection in the DEV environment and define the specific schema
  - Run a script to save the schema as source code internally
  - Create a connection in a QA / PROD environment
  - Run a script to deploy the saved source code to the QA / PROD connection 

To do this, I wrote two scripts:

**generate_schema_json.py**

- Extracts the current table and column configurations from a given Fivetran connector and saves them as individual JSON files per table.

**apply_table_configs.py**

- Reads the previously saved JSON files and applies the configurations back to the corresponding tables using the Fivetran API.

## 🧰 Requirements
- Python 3.7+
- requests library
Install dependencies (if needed):

``pip install -r requirements.txt``

✅ Note: Make sure your Fivetran API key and secret are available as environment variables or modify the script to include them directly.

## 🔧 Usage
1. Generate Table Configurations

`python3 generate_schema_json.py --connection_id <CONNECTION_ID> --folder_name <OUTPUT_FOLDER>`

Arguments:

- --connection_id: Fivetran connection ID (e.g., github_connector)

- --folder_name: Name of the local folder to save JSON table configurations (will be created if it doesn't exist)

Example:

`python3 generate_table_configs.py --connection_id burnt_crispt --folder_name github_config`

2. Apply Table Configurations

`python3 apply_table_configs.py --connection_id <CONNECTION_ID> --folder_name <INPUT_FOLDER>`

Arguments:

- --connection_id: Fivetran connection ID

- --folder_name: Name of the folder containing JSON files generated by the previous script

Example:

`python3 apply_table_configs.py --connection_id crazed_orange --folder_name github_config`

## Key Notes
- The apply_table_configs runs the generate_table_configs script after it alters the schema config of the requested connection
  - This is to ensure that the JSON files are up to date
  - After you change the files and before you run apply_table_configs, it may be good to push the changes to Git to ensure you save the schema config you were trying to deploy (because if it doesn't deploy correctly it will be written over)
- When a SaaS based connection is first created, you can't alter the column configuration until either (1) schema has been retrieved (2) initial sync has started
  - If you create a new connection and it hasn't had an initial sync yet, the script won't through an error but paste a list of the columns we attepted to edit but were unable to.
 




## 🔒 Authentication
Both scripts expect Fivetran API credentials to be set in your environment:

``export FIVETRAN_API_KEY="your_api_key"``

`` export FIVETRAN_API_SECRET="your_api_secret" ``

Or you can modify the requests headers in the script to hardcode them if preferred (not recommended for production).

## 👥 Contributors
Adam Rees

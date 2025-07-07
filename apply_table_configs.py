import os
import json
import requests
import subprocess

FIVETRAN_API_KEY = os.getenv("FIVETRAN_API_KEY")
FIVETRAN_API_SECRET = os.getenv("FIVETRAN_API_SECRET")
BASE_URL = "https://api.fivetran.com/v1"

HEADERS = {
    "Content-Type": "application/json"
}

# Empty lists to populate throughout the script and be printed at the end for error tracking
error_table_messages = []
error_column_messages = []

def reload_schema(connection_id):
    url = f"https://api.fivetran.com/v1/connections/{connection_id}/schemas/reload"
    reload_payload = {
        "exclude_mode": "PRESERVE"
    }
    response_payload= requests.post(url, auth=(FIVETRAN_API_KEY, FIVETRAN_API_SECRET), json=reload_payload)

# This function retrieves the schema name for a given Fivetran connection. 
# GET request fetches schema metadata and returns the name of the first schema
def get_schema_name(connection_id):

    reload_schema(connection_id)

    url = f"{BASE_URL}/connections/{connection_id}/schemas"

    response = requests.get(url, headers=HEADERS, auth=(FIVETRAN_API_KEY, FIVETRAN_API_SECRET))
    response.raise_for_status()
    data = response.json()
    return next(iter(data["data"]["schemas"].keys()))

# This function sends a PATCH request to the Fivetran API to update settings for a specific table, such as whether it's enabled and its sync mode. 
# It constructs the payload conditionally based on the presence of optional fields.
# Logs any failures to a global error_table_messages list while printing success messages to the console.
def update_table_config(connection_id, schema_name, table_name, table_data):
    payload = {
        "enabled": table_data["enabled"]
    }

    if table_data.get("sync_mode"):
        payload["sync_mode"] = table_data["sync_mode"]


    url = f"{BASE_URL}/connections/{connection_id}/schemas/{schema_name}/tables/{table_name}"
    response = requests.patch(url, headers=HEADERS, auth=(FIVETRAN_API_KEY, FIVETRAN_API_SECRET), json=payload)

    if response.status_code >= 400:
        error_table_messages.append({"table": table_name, "error": response.text})
    else:
        print(f"Updated table {table_name}")

# This function iterates through all columns in a given table and attempts to update each one’s configuration via a PATCH request.
# It skips columns if updates aren't allowed (as defined in enabled_patch_settings) and logs failures to error_column_messages. 
def update_column_config(connection_id, schema_name, table_name, table_data):
    columns = table_data.get("columns", {})

    for column_name, column_data in columns.items():
        patch_settings = column_data.get("supports_columns_config", {})
        if not patch_settings.get("allowed", True):
            print(f"Skipping column {column_name} (update not allowed)")
            continue

        payload = {
            "enabled": column_data["enabled"],
            "hashed": column_data.get("hashed", False)
        }

        print(f"Updating column {column_name} with payload: {payload}")

        url = f"{BASE_URL}/connections/{connection_id}/schemas/{schema_name}/tables/{table_name}/columns/{column_name}"
        response = requests.patch(url, headers=HEADERS, auth=(FIVETRAN_API_KEY, FIVETRAN_API_SECRET), json=payload)

        if response.status_code >= 400:
            error_column_messages.append({"table": table_name, "column": column_name, "error": response.text})

            
        else:
            print(f"Updated column {column_name}")


def main(connection_id, folder_path):

    print("main!")
    schema_name = get_schema_name(connection_id)
    print("schema_name")
    url = f"{BASE_URL}/connections/{connection_id}/schemas/{schema_name}"
    print(url)
    # create a dict for every table and if it supports columns config
    # check that before running update_column_config


    for filename in os.listdir(folder_path):

        if filename.endswith(".json"):
            filepath = os.path.join(folder_path, filename)
            with open(filepath, "r") as f:
                table_data = json.load(f)

            table_name = os.path.splitext(filename)[0]
           
            
            if table_data.get("enabled_patch_settings"):
                update_table_config(connection_id, schema_name, table_name, table_data)

        
            if table_data.get("columns"):
                update_column_config(connection_id, schema_name, table_name, table_data)

    # Call generate_schema_json.py with the same inputs
    subprocess.run([
    "python3", "generate_schema_json.py",
    "--connection_id", connection_id,
    "--folder_name", folder_path
    ])


if __name__ == "__main__":

    print("hello!")
    import argparse

    parser = argparse.ArgumentParser(description="Apply table configs to a Fivetran connection from JSON files.")
    parser.add_argument("--connection_id", required=True, help="Fivetran connection ID")
    parser.add_argument("--folder_path", required=True, help="Path to the folder containing JSON config files")

    args = parser.parse_args()
    main(args.connection_id, args.folder_path)


    print(f"List of failed tables: {error_table_messages}")
    print(f"List of failed columns: {error_column_messages}")

    

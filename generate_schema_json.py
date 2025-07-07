import os
import json
import requests

FIVETRAN_API_KEY = os.getenv("FIVETRAN_API_KEY")
FIVETRAN_API_SECRET = os.getenv("FIVETRAN_API_SECRET")


# This function retrieves schema information for a given Fivetran connection. 
# It constructs a request URL using the provided connection_id and authenticates the request with Fivetran API credentials loaded from environment variables
# Upon a successful HTTP response, it returns the parsed JSON data containing schema metadata.
def get_connection_schema(connection_id):
    url = f"https://api.fivetran.com/v1/connections/{connection_id}/schemas"
    response = requests.get(url, auth=(FIVETRAN_API_KEY, FIVETRAN_API_SECRET))
    response.raise_for_status()
    return response.json()

# This function fetches column-level configuration data for a specific table within a schema of a Fivetran connection.
# It builds a URL using the connection_id, schema_name, and table_name, and makes an authenticated GET request. 
# If the API responds with a 404 (indicating no column config is available), it returns None; otherwise, it returns the column details from the response.
def get_table_columns_config(connection_id, schema_name, table_name):
    url = f"https://api.fivetran.com/v1/connections/{connection_id}/schemas/{schema_name}/tables/{table_name}/columns"
    response = requests.get(url, auth=(FIVETRAN_API_KEY, FIVETRAN_API_SECRET))
    if response.status_code == 404:
        return None  # No columns info available
    response.raise_for_status()
    return response.json().get("data", {}).get("columns", {})

def reload_schema(connection_id):
    url = f"https://api.fivetran.com/v1/connections/{connection_id}/schemas/reload"
    reload_payload = {
        "exclude_mode": "PRESERVE"
    }
    response_payload= requests.post(url, auth=(FIVETRAN_API_KEY, FIVETRAN_API_SECRET), json=reload_payload)
    
# This function generates and stores table configuration JSON files for all tables in a Fivetran connection. 
# It first retrieves the full schema, then iterates over each table within each schema. 
# For each table, it constructs a dictionary containing table properties and column configurations (if supported). 
# These configurations are written as separate JSON files in a user-specified output folder. 
# If column-level configuration is supported, the function populates detailed attributes for each column.
def save_table_configs(connection_id, folder_name):
    os.makedirs(folder_name, exist_ok=True)


    reload_schema(connection_id)
    schema_data = get_connection_schema(connection_id)
    schemas = schema_data.get("data", {}).get("schemas", {})
    
    for schema_name, schema_value in schemas.items():

        schema_name = schema_value.get("name_in_destination")
        if not schema_name:
            continue

        for table_name, table_value in schema_value.get("tables", {}).items():
            output = {
                "table": table_name,
                "sync_mode": table_value.get("sync_mode"),
                "enabled": table_value.get("enabled"),
                "enabled_patch_settings": table_value.get("enabled_patch_settings", {}).get("allowed"),
                "supports_columns_config": table_value.get("supports_columns_config"),
                "columns": {}  # default if not populated
            }

            if table_value.get("supports_columns_config") is True:
                columns_config = get_table_columns_config(connection_id, schema_name, table_name)
                if columns_config:
                    output["columns"] = {
                        col_name: {
                            "name_in_destination": col_data.get("name_in_destination"),
                            "enabled": col_data.get("enabled"),
                            "hashed": col_data.get("hashed"),
                            "is_primary_key": col_data.get("is_primary_key"),
                            "enabled_patch_settings": col_data.get("enabled_patch_settings"),
                        }
                        for col_name, col_data in columns_config.items()
                    }

            file_path = os.path.join(folder_name, f"{table_name}.json")
            with open(file_path, "w") as f:
                json.dump(output, f, indent=2)
            print(f"Saved config for table: {table_name} → {file_path}")

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--connection_id", required=True, help="Fivetran connection ID")
    parser.add_argument("--folder_name", required=True, help="Name of folder to store output JSON files")

    args = parser.parse_args()
    save_table_configs(args.connection_id, args.folder_name)

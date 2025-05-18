import json
import os
import re

def clean_column_name(name: str) -> str:
    """Cleans column names for BigQuery compatibility."""
    name = name.strip()
    name = name.lower()
    name = re.sub(r"\s+", "_", name)            # Replace spaces with underscores
    name = re.sub(r"[^a-zA-Z0-9_]", "", name)    # Remove non-alphanumeric characters except underscore
    return name    

def generate_json_schemas(
    schema_config: str,
    output_folder: str
):
    os.makedirs(output_folder, exist_ok=True)

    for table in schema_config.get("tables", []):
        table_name = table["table_name"]
        schema_fields = table.get("schema", [])

        output_path = os.path.join(output_folder, f"{table_name}_schema.json")
        
        with open(output_path, "w") as f:
            json.dump(schema_fields, f, indent=2)

        print(f"✅ Schema for {table_name} saved at {output_path}")

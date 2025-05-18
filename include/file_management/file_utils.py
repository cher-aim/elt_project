import yaml
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def read_config(config_path:str):
    """Reads init_config.yaml and returns its contents as a dictionary."""
    # if not os.path.exists(f"/usr/local/airflow/include/configs/{config_path}"):
    #     print(f"⚠️ Config file not found: {config_path}")
    #     return {}  # ✅ Return an empty dictionary instead of None

    with open(config_path, "r") as file:
        try:
            return yaml.safe_load(file) or {}  # ✅ Ensure it never returns None
        except yaml.YAMLError as e:
            print(f"❌ Error reading YAML file {config_path}: {e}")
            return {}

def read_sql(file_path):
    with open(file_path, "r") as f:
        return f.read()

def convert_to_parquet(input_csv_folder_path: str, output_parquet_folder_path:str , chunk_size: int = 500_000, size_threshold_mb: int = 500):
    """
    Converts CSV files in a folder to Parquet files.
    For large CSV files (> size_threshold_mb), process in chunks to save memory.

    Args:
        input_csv_folder_path (str): Path to source CSV files
        output_parquet_folder_path (str): Destination path for Parquet files
        chunk_size (int, optional): Number of rows per chunk when splitting large files. Default = 500,000
        size_threshold_mb (int, optional): Size threshold to switch to chunked mode. Default = 500 MB
    """
    if not os.path.exists(input_csv_folder_path):
        raise FileNotFoundError(f"Input folder not found: {input_csv_folder_path}")
    
    os.makedirs(output_parquet_folder_path, exist_ok=True)

    for filename in os.listdir(input_csv_folder_path):
        if filename.endswith(".csv"):
            csv_path = os.path.join(input_csv_folder_path, filename)
            parquet_filename = filename.replace(".csv", ".parquet")
            parquet_path = os.path.join(output_parquet_folder_path, parquet_filename)

            file_size_mb = os.path.getsize(csv_path) / (1024 * 1024)
            print(f"📄 Converting {filename} ({file_size_mb:.2f} MB) -> {parquet_filename}")

            if file_size_mb < size_threshold_mb:
                df = pd.read_csv(csv_path, low_memory=False)
                df = df.astype(str)  # ✅ force all columns to string
                df.to_parquet(parquet_path, index=False)
                print(f"✅ Saved: {parquet_path}")
            else:
                print(f"⚡ File is large. Using chunked processing...")
                csv_iterator = pd.read_csv(csv_path, chunksize=chunk_size, low_memory=False)
                writer = None

                for i, chunk in enumerate(csv_iterator):
                    chunk = chunk.astype(str)  # ✅ force string per chunk
                    table = pa.Table.from_pandas(chunk)

                    if writer is None:
                        writer = pq.ParquetWriter(parquet_path, table.schema)

                    writer.write_table(table)
                    print(f"    ➡️ Wrote chunk {i + 1}")

                if writer:
                    writer.close()

                print(f"✅ Finished writing large file: {parquet_path}")
    
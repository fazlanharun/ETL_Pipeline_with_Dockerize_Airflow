import os
import pandas as pd
from utils.db import engine

def load_master_data():

    # --- Locate master_data.csv ---
    def find_file(filename, search_path):
        for root, dirs, files in os.walk(search_path):
            if filename in files:
                return os.path.join(root, filename)
        raise FileNotFoundError(f"{filename} not found in {search_path}")

    try:
        csv_path = find_file("master_data.csv",search_path="/opt/airflow/dags/db_config")
        df = pd.read_csv(csv_path)
        df.to_sql("master_data", engine, if_exists="replace", index=False)
        print("Master_Data ingested successfully.")
    except FileNotFoundError as e:
        print(e)

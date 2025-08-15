from etl.extract import extract_sales_data
from etl.transform import transform
from etl.load import aggregate_sales
from utils.file_utils import find_latest_csv
from utils.db import engine

def run_etl():
    
    sale_path = find_latest_csv("sales_*.csv", search_path="/opt/airflow/data/raw")

    extract_sales_data(engine,sale_path)
    latest_created = transform(engine) 
    if latest_created:
        aggregate_sales(engine, latest_created)
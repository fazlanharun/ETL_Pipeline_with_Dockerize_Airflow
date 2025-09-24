from etl.extract import extract_sales_data
from etl.transform import transform
from etl import load
from utils.file_utils import find_latest_csv
from utils.db import engine

def run_etl():
    
    sale_path = find_latest_csv("sales_*.csv", search_path="/opt/airflow/data/raw")

    extract_sales_data(engine,sale_path)
    latest_created = transform(engine) 
    if latest_created:
        load.aggregate_sales(engine, latest_created)
        load.create_dim_brand(engine)
        load.create_dim_category(engine)
        load.create_dim_product(engine)
        load.create_dim_time(engine)
        load.create_fact_sale(engine)
        load.create_sales_full_view(engine)
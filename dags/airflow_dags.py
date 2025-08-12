import os
import sys
import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import text
from utils.db import engine
from db_config.load_master_data_csv_to_postgres import load_master_data
from pipeline import run_etl

# Add current directory to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__)))

#configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_schema():
    """Create bronze, silver, and gold schemas in PostgreSQL"""
    logger.info("Starting schema creation...")

    try:       
        with engine.connect() as conn:
            for schema in ['bronze','silver','gold']:
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
        logger.info("All schemas created successfully")
                   
    except Exception as e:
        logger.error(f"Error in schema creation: {str(e)}")
        raise

with DAG(
    dag_id="sales_etl_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:
    
    init_schema_task = PythonOperator(
        task_id = "init_schema",
        python_callable = create_schema
    )

    load_master_task = PythonOperator(
        task_id="load_master_data",
        python_callable=load_master_data
    )

    etl_task = PythonOperator(
        task_id="run_etl_pipeline",
        python_callable=run_etl
    )

    # # Set task dependency:
    init_schema_task >> load_master_task >> etl_task

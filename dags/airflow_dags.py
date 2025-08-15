from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from db_config.load_master_data_csv_to_postgres import load_master_data
from db_config.create_table import create_schema
from db_config.create_pbi_user import ensure_powerbi_access
from pipeline import run_etl

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
    
    grant_powerbi = PythonOperator(
    task_id='grant_powerbi_access',
    python_callable=ensure_powerbi_access
    )

    # # Set task dependency:
    init_schema_task >> load_master_task >> etl_task >> grant_powerbi

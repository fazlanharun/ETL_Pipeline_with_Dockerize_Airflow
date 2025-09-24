ETL Pipeline with Dockerized Airflow

<img width="1102" height="772" alt="Data_Architecture_Sale_ETL_Pipeline drawio" src="https://github.com/user-attachments/assets/1045cc6f-7214-4117-9726-34be9de9fd91" />


A modular, audit-safe ETL pipeline built with Apache Airflow, PostgreSQL, and Docker, designed for reproducibility, operational clarity, 
and seamless integration with Power BI.

🔄 ETL Flow  
The DAG sales_etl_pipeline orchestrates four key tasks:  

Task	Description  
init_schema	Creates   :PostgreSQL tables using SQL scripts  
load_master_data      :Loads reference/master data from CSVs  
run_etl_pipeline	    :Extracts, transforms, and loads sales data  
grant_powerbi_access  :Ensures Power BI can access updated tables  

Each task is modular and reusable, with audit logging built into the ETL layer.  

🧪 ETL Logic Highlights  
Extraction: Reads raw CSVs from data/raw/, adds timestamps.  

Transformation: Check data quality, deduplicate, cleans columns, , validates rows as per business rules.  

Loading: Inserts into PostgreSQL all three stage bronze, silver, gold.  

After ETL completion, the DAG grants access to Power BI via grant_powerbi_access, enabling user with acess to data at gold layer.  

<img width="1130" height="900" alt="Connect Power Bi to Postgres" src="https://github.com/user-attachments/assets/84dc084d-d7e7-472a-b378-b01c00c55ec3" />


<img width="1836" height="850" alt="ELT and Data Warehouse Dashboard" src="https://github.com/user-attachments/assets/9708c2ad-97b6-4461-bce5-9c2079f14787" />

  
The sale data is a fake data created with python script in data/raw/generate_data.py  
  
🚀 Getting Started
bash
# Clone the repo
git clone https://github.com/fazlanharun/ETL_Pipeline_with_Dockerize_Airflow.git  
cd ETL_Pipeline_with_Dockerize_Airflow

# Start the pipeline
docker-compose up --build  

Access Airflow at localhost:8080 and pgAdmin at localhost:5050.

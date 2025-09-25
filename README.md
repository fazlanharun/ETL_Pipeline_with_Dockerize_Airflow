ETL Pipeline with Dockerized Airflow

![Data Architecture Sale ETL](https://github.com/user-attachments/assets/1198cff9-6de4-430c-bbea-f38e734bfd4a)



A modular ETL pipeline built with Apache Airflow, PostgreSQL, and Docker, designed for reproducibility, operational clarity, 
and seamless integration with Power BI.  

Business Problem : Manual processing of daily sales data using Microsoft Excel was time-consuming and inefficient, delaying actionable insights for stakeholders.  

Solution: Developed an automated ETL pipeline to streamline data cleaning and accelerate the data-to-insight cycle, enabling Sales and Marketing stakeholders to make faster, data-driven decisions.  

🔄 ETL Flow  
The DAG sales_etl_pipeline orchestrates four key tasks:  

Task	Description  
init_schema	          :Creates PostgreSQL tables using SQL scripts  
load_master_data      :Loads reference/master data from CSVs  
run_etl_pipeline	    :Extracts, transforms, and loads sales data  
grant_powerbi_access  :Ensures Power BI can access updated tables  

Each task is modular and reusable, with audit logging built into the ETL layer.  

🧪 ETL Logic Highlights  
Bronze: Reads raw sale data CSVs from data/raw/, adds timestamps created. Load to bronze schema. 

Silver: Extract latest batch of sale record from bronze.  Check data quality, deduplicate, validates rows as per business rules. Load to silver. 

Gold: Aggregates total sales from the latest batch in silver. Create fact and dimension table. Inserts into PostgreSQL and ready to use for analytic and reporting.

After ETL completion, the DAG grants access to Power BI via grant_powerbi_access, enabling user with acess to data at gold layer.  

<img width="1130" height="900" alt="Connect Power Bi to Postgres" src="https://github.com/user-attachments/assets/84dc084d-d7e7-472a-b378-b01c00c55ec3" />


<img width="1836" height="850" alt="ELT and Data Warehouse Dashboard" src="https://github.com/user-attachments/assets/9708c2ad-97b6-4461-bce5-9c2079f14787" />

  
The sale data is a fictitious data created with python script in data/raw/generate_data.py  
  
🚀 Getting Started
bash
# Clone the repo
git clone https://github.com/fazlanharun/ETL_Pipeline_with_Dockerize_Airflow.git  
cd ETL_Pipeline_with_Dockerize_Airflow

# Start the pipeline
docker-compose up --build  

Access Airflow at localhost:8080 and pgAdmin at localhost:5050.


import pandas as pd

#ingest sale data in csv file to bronze schema in postgres
def extract_sales_data(engine,sale_path):
    
    raw_df = pd.read_csv(sale_path)
    raw_df["date"] = pd.to_datetime(raw_df["date"]).dt.date
    
    # Ingestion timestamp
    created_ts = pd.Timestamp.now(tz="UTC")
    raw_df['created']=created_ts
    

    raw_df.to_sql("sales_raw",engine,schema="bronze",if_exists="append",index=False)
    print(f"Bronze : Loaded {len(raw_df)} record raw_sales to bronze.sales_raw with created = {created_ts.isoformat()}") 
    print(f"Bronze first 5 row {raw_df.head()}") 
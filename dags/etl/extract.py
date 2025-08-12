
#ingest sale data in csv file to bronze schema in postgres
def extract_sales_data(engine,sale_path):
    import pandas as pd
    raw_df = pd.read_csv(sale_path)
    raw_df.to_sql("sales_raw",engine,schema="bronze",if_exists="replace",index=False)
    print(f"Loaded {len(raw_df)} record raw_sales to bronze.sales_raw") 
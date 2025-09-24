
import pandas as pd
import os
from sqlalchemy import text

#validate data and enrich
def transform(engine):

    
    with engine.connect() as conn:
        latest_created = conn.execute(
            text("SELECT MAX(created) FROM bronze.sales_raw")
        ).scalar()
    
    if latest_created is None:
        print("No data in bronze.")
        return None
    
    #pull latest data
    sales_df = pd.read_sql(
        text("SELECT * FROM bronze.sales_raw WHERE created = :c"),
        engine,
        params={"c": latest_created}
    )

    master_df = pd.read_sql( "SELECT item_code, master_unit_price FROM bronze.master_data",engine)

    "Join sale with master data, validate unit price should be same"
    merged = sales_df.merge(master_df, on = 'item_code', how = 'left')
    
    merged['price_match'] = merged['unit_price'] == merged['master_unit_price']

    merged['revenue_sale'] = merged['unit_price'] * merged['quantity']
    merged['expected_revenue'] = merged['master_unit_price'] * merged['quantity']
    # Compare exact revenue values
    merged['revenue_match'] = merged['revenue_sale'] == merged['expected_revenue']

    #check if total revenue same
    total_from_sales = merged['revenue_sale'].sum()
    total_from_master = merged['expected_revenue'].sum()

    if total_from_sales != total_from_master:
        print(f'File rejected: total revenue mismatch, sale_file :{total_from_sales} vs master_validation: {total_from_master}.')

    #If any row has a mismatch between unit_price and master_unit_price, it enters this block.
    if not merged['price_match'].all():
        #send mismatched row to folder alerts/rejected_file
        
        #build absolute path
        reject_dir = "/opt/airflow/alerts/rejected_files"
        filename = "mismatches.csv"
        reject_path = os.path.join(reject_dir, filename)
        os.makedirs(reject_dir,exist_ok=True)
        merged[~merged['price_match']].to_csv(reject_path, index=False)

        print(f"File rejected: unit price mismatch. Saved mismatches to {reject_path}")
        return None

    #runs only if all prices match
      
    #column that define unique sale:
    business_keys = ['date','item_code','brand_name','quantity', 'unit_price' ]    
    
    existing_silver = pd.read_sql_table("sales_cleaned",engine,schema = "silver")
    
    #filter out duplicate, keep only rows from merged that not exist in silver
    #find duplicate
    
    merged['date'] = pd.to_datetime(merged['date'])

    merged['is_duplicate'] = merged.merge(
        existing_silver,
        on = business_keys,
        how = "left",
        indicator = True
        )['_merge']  == 'both'
    
    #keep only non-duplicate
    filtered = merged[~merged['is_duplicate']].copy()
    if filtered.empty:
        print(f"No new rows from bronze to append in silver.sales_cleaned.")
        return None
    
    filtered['total_sale'] = filtered['quantity'] * filtered['unit_price']

    silver_df = filtered[['date', 'item_code','brand_name', 'quantity', 'unit_price', 'total_sale','created']]
    silver_df.to_sql("sales_cleaned", engine,schema="silver", if_exists="append",index= False)
    print(f"Loaded {len(silver_df)} new record(s) to silver.sales_cleaned from batch {latest_created}")
    print(f"silver_df first 5 row",silver_df.head())
    return latest_created

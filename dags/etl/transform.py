
#validate data and enrich
def transform(engine):
    import pandas as pd
    query = "Select * FROM bronze.sales_raw"
    sales_df = pd.read_sql(query,engine)

    master_df = pd.read_sql( "SELECT item_code, master_unit_price FROM master_data",engine)

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

    if not merged['price_match'].all():
        #send mismatched row to folder alerts/reject file
        merged[~merged['price_match']].to_csv("alerts/rejected_files/mismatches.csv", index=False)
        print('File rejected:unit price mismatch.')
        return None

    merged['total_revenue'] = merged['quantity'] * merged['unit_price']
    silver_df = merged[['date', 'item_code', 'quantity', 'unit_price', 'total_revenue']]
    silver_df.to_sql("sales_cleaned", engine,schema="silver", if_exists="replace",index= False)
    print("Loaded cleaned_sales to silver.sales_cleaned")
    return silver_df

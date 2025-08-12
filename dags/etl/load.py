
def aggregate_sales(engine):
    """Load aggregated data to gold schema."""
    import pandas as pd

    query = "select * from silver.sales_cleaned"
    df = pd.read_sql(query,engine)

    summary = df.groupby("item_code").agg(total_sales=("total_revenue","sum")).reset_index()
    summary.to_sql("fact_sales_summary", engine, schema = "gold", if_exists="append", index=False)
    print(f"Loaded {len(summary)} records into gold.fact_sales_summary.")

from sqlalchemy import text
import pandas as pd

def aggregate_sales(engine, latest_created):
    delta_sql = """
    WITH delta AS (
      SELECT
        item_code,
        SUM(total_sale) AS delta_sales
      FROM silver.sales_cleaned
      WHERE created = :batch_created
      GROUP BY item_code
    )
    INSERT INTO gold.fact_sales_summary (item_code, total_sales, last_update)
    SELECT item_code, delta_sales, now()
    FROM delta
    ON CONFLICT (item_code) DO UPDATE
    SET total_sales = gold.fact_sales_summary.total_sales + EXCLUDED.total_sales,
        last_update = now();
    """  
    with engine.begin() as conn:
        conn.execute(text(delta_sql), {"batch_created": latest_created})
    
    print(f"Gold load complete for batch {latest_created}.")
    
    gold_preview = pd.read_sql(
        text("SELECT * FROM gold.fact_sales_summary ORDER BY last_update DESC LIMIT 5"),
        engine
    )
    print("Latest gold snapshot:")
    print(gold_preview.to_markdown(index=False))

        

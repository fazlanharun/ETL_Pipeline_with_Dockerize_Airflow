
from sqlalchemy import text
import pandas as pd

#aggregate sales from a batch latest_created and 
#update the gold.fact_sales_summary table accordingly — either inserting new rows or incrementing existing totals.
def aggregate_sales(engine, latest_created):
    delta_sql = """
    WITH delta AS (
      SELECT
        date,
        item_code,
        SUM(total_sale) AS delta_sales
      FROM silver.sales_cleaned
      WHERE created = :batch_created
      GROUP BY item_code, date
    )
    INSERT INTO gold.fact_sales_summary (date, item_code, total_sales, last_update)
    SELECT date, item_code, delta_sales, now()
    FROM delta
    ON CONFLICT (item_code,date) DO UPDATE
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

# create dim from bronze.master_data   
def create_dim_brand(engine):
  sql = """
  CREATE TABLE IF NOT EXISTS gold.dim_brand(
  brand_id SERIAL PRIMARY KEY,
  brand_name TEXT UNIQUE NOT NULL);
  
  INSERT INTO gold.dim_brand (brand_name)
  SELECT DISTINCT brand from bronze.master_data
  ON CONFLICT (brand_name) DO NOTHING;
  """
  with engine.begin() as conn:
    conn.execute(text(sql))
  print("dim_brand created and populated.")
  
def create_dim_category(engine):
    sql = """
    CREATE TABLE IF NOT EXISTS gold.dim_category (
        category_id SERIAL PRIMARY KEY,
        category_name TEXT UNIQUE NOT NULL
    );

    INSERT INTO gold.dim_category (category_name)
    SELECT DISTINCT category FROM bronze.master_data
    ON CONFLICT (category_name) DO NOTHING;
    """
    with engine.begin() as conn:
        conn.execute(text(sql))
    print("dim_category created and populated.")
    
def create_dim_product(engine):
    sql = """
    CREATE TABLE IF NOT EXISTS gold.dim_product (
        item_code TEXT PRIMARY KEY,
        product_name TEXT NOT NULL,
        brand_id INT REFERENCES gold.dim_brand(brand_id),
        category_id INT REFERENCES gold.dim_category(category_id),
        unit_price NUMERIC
    );

    INSERT INTO gold.dim_product (item_code, product_name, brand_id, category_id, unit_price)
    SELECT 
        m.item_code,
        m.item_name,
        b.brand_id,
        c.category_id,
        m.master_unit_price
    FROM bronze.master_data m
    JOIN gold.dim_brand b 
    ON m.brand = b.brand_name
    JOIN gold.dim_category c 
    ON m.category = c.category_name
    ON CONFLICT (item_code) DO NOTHING;
    """
    with engine.begin() as conn:
        conn.execute(text(sql))
    print("dim_product created and populated.")
    
def create_dim_time(engine):
    sql = """
    CREATE TABLE IF NOT EXISTS gold.dim_time (
        time_id SERIAL PRIMARY KEY,
        date DATE NOT NULL,
        year INT,
        month INT,
        day INT
    );

    INSERT INTO gold.dim_time (date, year, month, day)
    SELECT DISTINCT 
        date,
        EXTRACT(YEAR FROM date),
        EXTRACT(MONTH FROM date),
        EXTRACT(DAY FROM date)
    FROM gold.fact_sales_summary;
    """
    with engine.begin() as conn:
        conn.execute(text(sql))
    print("dim_time created and populated.")
    
def create_fact_sale(engine):
    sql = """
    CREATE TABLE IF NOT EXISTS gold.fact_sales (
        sale_id SERIAL PRIMARY KEY,
        item_code TEXT REFERENCES gold.dim_product(item_code),
        brand_id INT REFERENCES gold.dim_brand(brand_id),
        category_id INT REFERENCES gold.dim_category(category_id),
        time_id INT REFERENCES gold.dim_time(time_id),
        total_sales NUMERIC,
        quantity NUMERIC
    );

    INSERT INTO gold.fact_sales (item_code, brand_id, category_id, time_id, total_sales, quantity)
    SELECT 
        s.item_code,
        p.brand_id,
        p.category_id,
        t.time_id,
        s.total_sales,
        s.total_sales / p.unit_price
    FROM gold.fact_sales_summary s
    JOIN gold.dim_product p ON s.item_code = p.item_code
    JOIN gold.dim_brand b ON p.brand_id = b.brand_id
    JOIN gold.dim_category c ON p.category_id = c.category_id
    JOIN gold.dim_time t ON s.date = t.date;
    """
    with engine.begin() as conn:
        conn.execute(text(sql))
    print("fact_sales created and populated.")
    
    
def create_sales_full_view(engine):
    sql = """
    CREATE OR REPLACE VIEW gold.v_sales_full AS
    SELECT 
        f.sale_id,
        f.item_code,
        p.product_name,
        b.brand_name,   
        c.category_name,
        t.date,
        t.year,
        t.month,
        t.day,
        f.total_sales,
        round(f.quantity,0) as quantity
    FROM gold.fact_sales f
    JOIN gold.dim_product p ON f.item_code = p.item_code
    JOIN gold.dim_brand b ON f.brand_id = b.brand_id
    JOIN gold.dim_category c ON p.category_id = c.category_id
    JOIN gold.dim_time t ON f.time_id = t.time_id;
    """
    with engine.begin() as conn:
        conn.execute(text("DROP VIEW IF EXISTS gold.v_sales_full"))
        conn.execute(text(sql))
    print("gold.v_sales_full view created.")
        
    sales_full_view_preview = pd.read_sql(
        text("SELECT * FROM gold.v_sales_full LIMIT 5"),
        engine
    )
    print("sales_full_view_preview snapshot:")
    print(sales_full_view_preview.to_markdown(index=False))
        

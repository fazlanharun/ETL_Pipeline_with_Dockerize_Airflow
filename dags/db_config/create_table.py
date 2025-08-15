
def create_schema():
    
    from sqlalchemy import text
    from utils.db import engine
    import os
    import sys
    import logging
    
    sys.path.append(os.path.join(os.path.dirname(__file__)))
        
    #configure logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    """Create bronze, silver, and gold schemas in PostgreSQL"""
    logger.info("Starting schema creation...")

    try:       
        ddl = '''
        CREATE SCHEMA IF NOT EXISTS bronze;
        CREATE SCHEMA IF NOT EXISTS silver;
        CREATE SCHEMA IF NOT EXISTS gold;
    
        CREATE TABLE IF NOT EXISTS bronze.sales_raw (
        date DATE NOT NULL,
        item_code TEXT NOT NULL,
        brand_name TEXT NOT NULL,
        quantity NUMERIC NOT NULL,
        unit_price NUMERIC NOT NULL,
        executive TEXT,
        created TIMESTAMPTZ NOT NULL);
        
        CREATE TABLE IF NOT EXISTS silver.sales_cleaned (
        date DATE NOT NULL,
        item_code TEXT NOT NULL,
        brand_name TEXT NOT NULL,
        quantity NUMERIC NOT NULL,
        unit_price NUMERIC NOT NULL,
        total_sale NUMERIC NOT NULL,
        created TIMESTAMPTZ NOT NULL);
        
        CREATE TABLE IF NOT EXISTS gold.fact_sales_summary (
        item_code TEXT PRIMARY KEY,
        total_sales NUMERIC NOT NULL,
        last_update  timestamptz NOT NULL DEFAULT now()
        );

        '''
        with engine.connect() as conn:
                conn.execute(text(ddl))
        logger.info("All schemas created successfully")
                   
    except Exception as e:
        logger.error(f"Error in schema creation: {str(e)}")
        raise
import psycopg2

def ensure_powerbi_access():
    conn = psycopg2.connect(
        host="postgres",  # service name in docker-compose
        port=5432,
        dbname="airflow",
        user="airflow",
        password="airflow"
    )
    conn.autocommit = True
    cur = conn.cursor()
    
    # 1. Create role if not exists
    cur.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'powerbi_user') THEN
                CREATE ROLE powerbi_user LOGIN PASSWORD 'Gold#';
            END IF;
        END
        $$;
    """)

    # 2. Grant access to gold schema
    cur.execute("GRANT USAGE ON SCHEMA gold TO powerbi_user;")
    cur.execute("GRANT SELECT ON ALL TABLES IN SCHEMA gold TO powerbi_user;")
    cur.execute("""
        ALTER DEFAULT PRIVILEGES IN SCHEMA gold
        GRANT SELECT ON TABLES TO powerbi_user;
    """)
    
    cur.close()
    conn.close()


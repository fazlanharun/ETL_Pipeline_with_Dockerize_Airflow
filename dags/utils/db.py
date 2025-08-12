from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv("/opt/airflow/.env")

user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
db = os.getenv("DB_NAME")

engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")






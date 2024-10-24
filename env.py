from dotenv import load_dotenv
import os
import psycopg2
load_dotenv()


DATABASE_URL = os.getenv("DATABASE_URL")
BASE_URL = os.getenv("BASE_URL")

POSTGRES_JDBC_LOCATION = os.getenv("POSTGRES_JDBC_LOCATION")
CHECKPOINT_LOCATION_1 = os.getenv("CHECKPOINT_LOCATION_1")
CHECKPOINT_LOCATION_2 = os.getenv("CHECKPOINT_LOCATION_2")


def connect_db():
    conn = psycopg2.connect(DATABASE_URL)
    return conn

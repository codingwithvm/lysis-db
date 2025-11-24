import os

import pymssql
from dotenv import load_dotenv

load_dotenv()

DB_SERVER = os.getenv("DB_SERVER")
DB_PORT = int(os.getenv("DB_PORT"))
DB_NAME = os.getenv("DB_NAME")
DB_USERNAME = os.getenv("DB_USERNAME")
DB_PASSWORD = os.getenv("DB_PASSWORD")


def get_connection():
    try:
        conn = pymssql.connect(
            server=DB_SERVER,
            port=DB_PORT,
            user=DB_USERNAME,
            password=DB_PASSWORD,
            database=DB_NAME,
        )
        return conn
    except Exception as e:
        print("Error connecting to SQL Server:", e)
        raise e


def run_query(sql: str, params: tuple = None):
    conn = get_connection()
    cursor = conn.cursor(as_dict=True)

    cursor.execute(sql, params) if params else cursor.execute(sql)

    results = cursor.fetchall()

    conn.close()
    return results

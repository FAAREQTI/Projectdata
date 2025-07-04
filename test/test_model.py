import pandas as pd
import pandas as pd
import boto3
from io import BytesIO, StringIO
import pandas as pd
import psycopg2
from typing import List, Tuple
import argparse
import os
from pathlib import Path
from dotenv import load_dotenv
import importlib
import pytest 

# load csv
def load_csv(filename: str) -> pd.DataFrame:
    try:
        df = pd.read_csv(filename)
    except Exception as e:
        print("Error:", e)
    return df

# assert fixing data  

import re
def test_date_format(df):
    pattern = re.compile(r'^\d{4}-\d{2}-\d{2}$')
    for date_str in df['date_column']:
        assert pattern.match(date_str), f"Invalid date format: {date_str}"

###################


def connet_dbg2():
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(
            host="localhost",
            database='test_db',
            user="fatima",
            password="fafafa99"
        )
        cur = conn.cursor()
    except Exception as e:
        print("Error:", e)
    return conn, cur


def create_table(cur, col_type, name_of_table, conn):
    try:
        cur.execute(f"DROP TABLE IF EXISTS {name_of_table}")
        cur.execute(f"CREATE TABLE {name_of_table} ({col_type})")
        conn.commit()
    except Exception as e:
        print("Error in create_table:", e)
        conn.rollback()  
        raise  


# Schema to apply to all tables
col_type = "id SERIAL PRIMARY KEY, name TEXT"

# Table names to create
table_names = ["table_1", "table_2", "table_3"]

# Get connection
conn, cur = connet_dbg2()

# Create each table
for table_name in table_names:
    create_table(cur, col_type, table_name, conn)
    print(f"Table {table_name} created.")


import pytest
import psycopg2


# Tables you expect to be created
expected_tables = ["table_1", "table_2", "table_3"]

def test_tables_exist(connet3):
    conn, cur = connet3
    for table_name in expected_tables:
        cur.execute(f"SELECT to_regclass('public.{table_name}')")
        result = cur.fetchone()[0]
        assert result == table_name, f"❌ Table '{table_name}' does not exist in the database."


# Test function to check if 'test_db' exists
def test_database_exists(admin_conn):
    conn, cur = admin_conn
    target_db = "test_db"

    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (target_db,))
    result = cur.fetchone()
    assert result is not None, f"❌ Database '{target_db}' does not exist."


    

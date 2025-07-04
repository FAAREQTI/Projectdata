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
from utils import connet_dbg 


def test_connect_g():
    conn, cur = connet_dbg()
    assert conn is not None
    assert cur is not None 
    cur.close()
    conn.close()



def create_table(cur: str, col_type: str, name_of_table: str, conn: str):
    try:
        cur.execute(f"DROP TABLE IF EXISTS {name_of_table}")
        cur.execute(f"CREATE TABLE {name_of_table} ({col_type})")
        conn.commit()
    except Exception as e:
        print("Error:", e)

def create_table(cur, col_type, name_of_table, conn):
    try:
        cur.execute(f"DROP TABLE IF EXISTS {name_of_table}")
        cur.execute(f"CREATE TABLE {name_of_table} ({col_type})")
        conn.commit()
    except Exception as e:
        print("Error in create_table:", e)
        conn.rollback()  
        raise  


def test_create_table(connet_dbg2):
    conn, cur = connet_dbg2
    assert conn is not None and cur is not None
    col_type = "id SERIAL PRIMARY KEY, name TEXT"
    table_name = "test_table"
    create_table(cur, col_type, table_name, conn)
    cur.execute("SELECT to_regclass('public.test_table');")
    result = cur.fetchone()[0]
    assert result == "test_table"


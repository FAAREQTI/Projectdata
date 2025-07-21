import pandas as pd
from datetime import datetime
import os
import sys
from datetime import datetime
import pytest 
from src.utils import load_csv
import re
from typing import Tuple
from src.utils import connet_dbg, create_table



def test_date_column_format(df: pd.DataFrame, column: str = "date"):
    pattern = re.compile(r'^\d{4}-\d{2}-\d{2}$')
    for val in df[column]:
        assert pattern.match(val), f"Invalid date format: {val}"


# convering python to sql format
def convert_types_to_sql_format(df2: pd.DataFrame) -> Tuple[str, str]:
    """Converts pandas data types to SQL data types.
    Args:
        df (pd.DataFrame): Pandas dataframe to be converted
    Returns:
        Tuple[str, str]: tuple contaning SQL column names and values
    """
    try:
        types = []
        for i in df2.dtypes:
            if i == 'int64':
                types.append('int')
            elif i == 'object':
                types.append('VARCHAR(255)')
            elif i == 'float':
                types.append("DECIMAL(6,2)")

        col_type = list(zip(df2.columns.values, types))
        col_type = tuple([" ".join(i) for i in col_type])
        col_type = ', '.join(col_type)
        values = ', '.join(["%s" for i in range(len(df2.columns))])
    except Exception as e:
        print("Error:", e)
    return col_type, values


def test_convert_types_to_sql_format():
    df2 = pd.DataFrame({
        'name': ['Alice', 'Bob'],
        'age': [25, 30],
        'salary': [50000.5, 60000.75]
    })

    col_type, values = convert_types_to_sql_format(df2)

    assert col_type == "name VARCHAR(255), age int, salary DECIMAL(6,2)"
    assert values == "%s, %s, %s"

col_type = "id SERIAL PRIMARY KEY, name TEXT"

table_names = ["table_1", "table_2", "table_3"]

conn, cur = connet_dbg()

# Create each table
for table_name in table_names:
    create_table(cur, col_type, table_name, conn)
    print(f"Table {table_name} created.")


expected_tables = ["table_1", "table_2", "table_3"]

def test_tables_exist(connet3):
    conn, cur = connet3
    for table_name in expected_tables:
        cur.execute(f"SELECT to_regclass('public.{table_name}')")
        result = cur.fetchone()[0]
        assert result == table_name, f"Table '{table_name}' does not exist in the database."


# Test function to check if 'test_db' exists
def test_database_exists(admin_conn):
    conn, cur = admin_conn
    target_db = "test_db"

    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (target_db,))
    result = cur.fetchone()
    assert result is not None, f"Database '{target_db}' does not exist."


def test_create_table(connet_dbg2):
    conn, cur = connet_dbg2
    assert conn is not None and cur is not None
    col_type = "id SERIAL PRIMARY KEY, name TEXT"
    table_name = "test_table"
    create_table(cur, col_type, table_name, conn)
    cur.execute("SELECT to_regclass('public.test_table');")
    result = cur.fetchone()[0]
    assert result == "test_table"



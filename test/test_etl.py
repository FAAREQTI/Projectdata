import pandas as pd
from datetime import datetime
import os
import sys
from datetime import datetime
import pytest 
from src.utils import load_csv
import re
from typing import Tuple
from src.utils import create_table, connet_dbg 

## These tests validate proper data formatting and ensure correct SQL type conversion 

@pytest.mark.xfail
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
            elif 'datetime' in str(i):
                types.append("VARCHAR(255)")

        col_type = list(zip(df2.columns.values, types))
        col_type = tuple([" ".join(i) for i in col_type])
        col_type = ', '.join(col_type)
        values = ', '.join(["%s" for i in range(len(df2.columns))])
    except Exception as e:
        print("Error:", e)
    return col_type, values


def test_convert_types_to_sql_format(df2):
    col_type, values = convert_types_to_sql_format(df2)

    expected_col_type = "city VARCHAR(255), product_line VARCHAR(255), date VARCHAR(255), quantity int"
    expected_values = "%s, %s, %s, %s"

    assert col_type == expected_col_type
    assert values == expected_values


col_type = "id SERIAL PRIMARY KEY, name TEXT"

table_names = ["table_1", "table_2", "table_3"]

conn, cur = connet_dbg()

# Create each table
for table_name in table_names:
    create_table(cur, col_type, table_name, conn)
    print(f"Table {table_name} created.")


expected_tables = ["table_1", "table_2", "table_3"]

##Tests to verify PostgreSQL setup: database existence, table creation, and table presence using a shared connection fixture.

def test_tables_exist(connet_dbg):
    conn, cur = connet_dbg
    for table_name in expected_tables:
        cur.execute(f"SELECT to_regclass('public.{table_name}')")
        result = cur.fetchone()[0]
        assert result == table_name, f"Table '{table_name}' does not exist in the database."


# Test function to check if 'test_db' exists
def test_database_exists(connet_dbg):
    conn, cur = connet_dbg
    target_db = "test_db"

    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (target_db,))
    result = cur.fetchone()
    assert result is not None, f"Database '{target_db}' does not exist."


def test_create_table(connet_dbg):
    conn, cur = connet_dbg
    assert conn is not None and cur is not None
    col_type = "id SERIAL PRIMARY KEY, name TEXT"
    table_name = "test_table"
    create_table(cur, col_type, table_name, conn)
    cur.execute("SELECT to_regclass('public.test_table');")
    result = cur.fetchone()[0]
    assert result == "test_table"



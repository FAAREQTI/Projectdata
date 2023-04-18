import pandas as pd
import psycopg2
from typing import List, Tuple
import argparse


# function to connect to database
def connet_db(database: str) -> Tuple[str, str]:
    try:
        conn = psycopg2.connect(
            host="localhost",
            database=database,
            user="postgres",
            password="ARES"
        )
        cur = conn.cursor()
    except Exception as e:
        print("Error:", e)
    return conn, cur

# function to create table


def create_table(cur: str, col_type: str, name_of_table: str, conn: str):
    try:
        # drop the table if exists
        cur.execute(f"DROP TABLE IF EXISTS {name_of_table}")
        # Create a new table
        cur.execute(f"CREATE TABLE {name_of_table} ({col_type})")
        # Commit the changes
        conn.commit()
    except Exception as e:
        print("Error:", e)

# load csv


def load_csv(filename: str) -> pd.DataFrame:
    try:
        df = pd.read_csv(filename)
    except Exception as e:
        print("Error:", e)
    return df

# convert python to sql format


def convert_types_to_sql_format(df: pd.DataFrame) -> Tuple[str, str]:
    try:
        types = []
        for i in df.dtypes:
            if i == 'int64':
                types.append('int')
            elif i == 'object':
                types.append('VARCHAR(255)')
            elif i == 'float':
                types.append("DECIMAL(6,2)")

        col_type = list(zip(df.columns.values, types))
        col_type = tuple([" ".join(i) for i in col_type])
        col_type = ', '.join(col_type)
        values = ', '.join(["%s" for i in range(len(df.columns))])
    except Exception as e:
        print("Error:", e)
    return col_type, values

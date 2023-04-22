import pandas as pd
import psycopg2
from typing import List, Tuple
import argparse
import os
from pathlib import Path
from dotenv import load_dotenv

env_path = Path('.env')
load_dotenv(env_path)

# function to connect to database


def connet_db(database: str) -> Tuple[str, str]:
    """Connects to a PostgreSQL database with the given database name
    Args:
        database (str): the name of the database
    Returns:
        Tuple[str, str]: tuple containing the connection and cursor objects.
    """
    try:
        conn = psycopg2.connect(
            host="localhost",
            database=database,
            user="postgres",
            password=os.getenv("PASSWORD")
        )
        cur = conn.cursor()
    except Exception as e:
        print("Error:", e)
    return conn, cur

# function to create table


def create_table(cur: str, col_type: str, name_of_table: str, conn: str):
    """Creates a new table in a PostgreSQL database
    Args:
        cur (str): cursor object
        col_type (str): colum corresponding data types 
        name_of_table (str): name of table to be created
        conn (str): the connection object
    """
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
    """Loads a CSV file as a pandas DataFrame.
     Args:
        filename: path to csv file
    Returns:
        df: the CSV file as a pandas DataFrame
    """


def load_csv(filename: str) -> pd.DataFrame:
    try:
        df = pd.read_csv(filename)
    except Exception as e:
        print("Error:", e)
    return df

# convert python to sql format


def convert_types_to_sql_format(df: pd.DataFrame) -> Tuple[str, str]:
    """Converts pandas data types to SQL data types.
    Args:
        df (pd.DataFrame): Pandas dataframe to be converted
    Returns:
        Tuple[str, str]: tuple contaning SQL column names and values
    """
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

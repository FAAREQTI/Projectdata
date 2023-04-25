import boto3
from io import BytesIO, StringIO
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


def auth_aws(env_path):
    """Sets up authentication for AWS, returns bucket and s3 objects, and prints success message"""
    # Load environment variables
    load_dotenv(env_path)

    # Get AWS credentials and bucket name from environment variables
    bucket = os.getenv("BUCKET")
    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")

    # Authenticate AWS client
    s3 = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )

    # Print success message
    print("Successfully authenticated")

    # Return bucket and s3 objects
    return bucket, s3


def upload_file(filename, key):
    """Uploads a file to S3 using the auth_aws() function and returns file name and key"""
    # Authenticate AWS and get bucket and s3 objects
    bucket, s3 = auth_aws(Path('.env'))

    # Upload file to S3
    s3.upload_file(filename, bucket, Key=key)

    # Print success message
    print("Successfully uploaded")

    # Return file name and key
    return filename, key


def read_file_from_s3(key):
    """Reads a file from S3 using the auth_aws() function and returns a Pandas DataFrame"""
    # Authenticate AWS and get bucket and s3 objects
    bucket, s3 = auth_aws(Path('.env'))

    # Get file from S3
    res = s3.get_object(Bucket=bucket, Key=key)
    csv_data = res['Body'].read().decode('utf-8')

    # Convert file data to Pandas DataFrame
    df = pd.read_csv(StringIO(csv_data))

    # Return Pandas DataFrame
    return df

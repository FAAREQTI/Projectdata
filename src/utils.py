import boto3
from io import BytesIO, StringIO
import pandas as pd
import psycopg2
from typing import List, Tuple
import argparse
import os
from pathlib import Path
from dotenv import load_dotenv
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import gspread

# function to connect to database
def connet_db(database: str) -> Tuple[str, str]:
    """Connects to a PostgreSQL database with the given database name
    Args:
        database (str): the name of the database
    Returns:
        Tuple[str, str]: tuple containing the connection and cursor objects.
    """
    env_path = Path('.env')
    load_dotenv(env_path)
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
def load_csv(filename: str) -> pd.DataFrame:
    """Loads a CSV file as a pandas DataFrame.
     Args:
        filename: path to csv file
    Returns:
        df: the CSV file as a pandas DataFrame
    """
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


def upload_to_s3(df: pd.DataFrame, filename: str) -> bool:
    """
    Uploads a pandas DataFrame to an S3 bucket.
    Parameters:
    -----------
    df : pandas.DataFrame
        The pandas DataFrame to upload.
    filename : str
        The name to give to the uploaded file. This should not include the file extension.
    Returns:
    --------
    bool
        True if the DataFrame was successfully uploaded, False otherwise.
    """
    # Authenticate with AWS
    bucket_name, s3 = auth_aws(".env")

    # Upload the file
    try:
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        response = s3.put_object(
            ACL = 'private',
            Body=csv_buffer.getvalue(),
            Bucket = bucket_name,
            Key=filename + '.csv'
        )
    except Exception as e:
        print(e)
        return False
    return True


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

def upload_to_google_sheet(spreadsheet_id: str, df: pd.DataFrame, worksheet_name: str) -> bool:
    
    # Authenticate with Google Sheets API
    SCOPES = [
        "https://spreadsheets.google.com/feeds",
        'https://www.googleapis.com/auth/spreadsheets',
        "https://www.googleapis.com/auth/drive.file",
        "https://www.googleapis.com/auth/drive"
    ]
    
    credentials = ServiceAccountCredentials.from_json_keyfile_name("GCP.json", SCOPES)
    gc = gspread.authorize(credentials)

    # Open the worksheet
    try:
        worksheet = gc.open_by_key(spreadsheet_id).worksheet(worksheet_name)
    except gspread.exceptions.WorksheetNotFound:
        worksheet = gc.open_by_key(spreadsheet_id).add_worksheet(worksheet_name, 1, 1)

    # Clear the existing content in the worksheet
    worksheet.clear()

    # Convert Timestamp columns to string format
    df = df.astype(str)

    # Write the DataFrame to the worksheet
    cell_list = worksheet.update([df.columns.values.tolist()] + df.values.tolist())
    if cell_list:
        print("success")
    else:
        print("failed")
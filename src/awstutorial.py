import boto3
import os
from dotenv import load_dotenv
from io import BytesIO, StringIO
import pandas as pd

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

import pandas as pd
import psycopg2
from utils import load_csv, connet_db, create_table, convert_types_to_sql_format
import argparse

parser = argparse.ArgumentParser(description= "loading csv files to mysql database")
parser.add_argument('-f','--filename', type= str, help='pass the path to the csv file')
args = parser.parse_args()

df = load_csv(args.filename)
print("data loaded")

parser = argparse.ArgumentParser(description="Connecting to a PostgreSQL database")
parser.add_argument('-d', '--database', type=str, help='the name of the database to connect to')
args = parser.parse_args()

conn, cur = connet_db(database=args.database)
print("Connected to database: ", args.database)


parser = argparse.ArgumentParser(description="Convert pandas DataFrame columns to SQL data types")
parser.add_argument('-d', '--dataframe', type=str, help='path to the CSV file to load as pandas DataFrame')
args = parser.parse_args()

df = pd.read_csv(args.dataframe)
col_types, values = convert_types_to_sql_format(df=df)
print("successfully converted"
      
parser = argparse.ArgumentParser(description="Create a table in the database")
parser.add_argument('-t', '--table', type=str, help='Name of the table to create')
args = parser.parse_args()
create_table(cur=cur, col_type=col_types, name_of_table=args.table, conn=conn)
print("Created table", args.table)
create_table(cur=cur, col_type=col_types, name_of_table="supermarket", conn=conn)
print("created table")

for i,row in df.iterrows(): 
    sql = f'INSERT INTO supermarket VALUES ({values})'
    cur.execute(sql, tuple(row))                
    conn.commit()

cur.execute("SELECT * FROM supermarket;")
print(cur.fetchall())
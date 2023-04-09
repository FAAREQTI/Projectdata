import pandas as pd
import psycopg2
from utils import load_csv, connet_db, create_table, convert_types_to_sql_format
import argparse

parser = argparse.ArgumentParser(description= "upload data to postgres database")
parser.add_argument('-f','--filename', type= str, help='pass the path to the csv file')
parser.add_argument('-d', '--database', type=str, help='the name of the database to connect to')
parser.add_argument('-t', '--table', type=str, help='Name of the table to create')
args = parser.parse_args()

df = load_csv(args.filename)
print("data loaded")

conn, cur = connet_db(database=args.database)
print("Connected to database: ", args.database)

col_types, values = convert_types_to_sql_format(df=df)
print("successfully converted")
      
create_table(cur=cur, col_type=col_types, name_of_table=args.table, conn=conn)
print("Created table", args.table)

for i,row in df.iterrows(): 
    sql = f'INSERT INTO {args.table} VALUES ({values})'
    cur.execute(sql, tuple(row))                
    conn.commit()

print("successfully uploaded", cur.rowcount)


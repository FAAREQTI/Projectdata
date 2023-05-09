import pandas as pd
import yaml
import os
import psycopg2
from utils import load_csv, connet_db, create_table, convert_types_to_sql_format
import argparse
from pathlib import Path

# args --------------------------------------------------------------
parser = argparse.ArgumentParser(description= "upload data to postgres database")
parser.add_argument('-id','--task_id', type= str, help='pass the path to the csv file')
parser.add_argument('-d', '--database', type=str, help='the name of the database to connect to')
args = parser.parse_args()

# load tasks from config --------------------------------------------------------------
with open("./config/config.yaml", 'r') as f:
    config = yaml.load(f, Loader=yaml.FullLoader)

# define variables from config --------------------------------------------------------------
config_import = config[args.task_id]["import"]

# STEP 1: connect to database --------------------------------------------------------------
conn, cur = connet_db(database=args.database)
print("Connected to database: ", args.database)

# STEP 2: create database -> create data -> insert data 
for i in range(len(config_import)):
    data = Path(config_import[i]["import"]["dirpath"], 
                config_import[i]["import"]["prefix_filename"]+ '.' +
                config_import[i]["import"]["file_extension"])
    print(data)
    
    # load data to upload --------------
    df = load_csv(data)
    print("data loaded")

    # create sql format --------------
    table_name = os.path.basename(data).split('.')[0]
    col_types, values = convert_types_to_sql_format(df=df)
    print("successfully converted")
    
    # create database --------------
    create_table(cur=cur, col_type=col_types, name_of_table=table_name, conn=conn)
    print(f"Created table: {table_name}")

    # insert data to table in database --------------
    count = 0
    for i,row in df.iterrows(): 
        sql = f'INSERT INTO {table_name} VALUES ({values})'
        cur.execute(sql, tuple(row))                
        conn.commit()
        count += 1    
    print(f"Successfully uploaded {count} rows to the database")
    
cur.close()
conn.close()

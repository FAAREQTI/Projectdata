import pandas as pd
from src.utils import load_csv, connet_db, create_table, convert_types_to_sql_format


#function to read and execute code in python: 
def run_sql_script(database, script_path):
    with open(script_path, "r") as f:
        sql_script = f.read()
    conn, cur = connet_db(database="postgres")
    df= pd.read_sql(sql_script, conn)
    cur.close()
    conn.close()
    return df 


# return a dataframe from sql script
def process() -> pd.DataFrame:
    database = "postgres"
    script_path = "src/JOIN.sql"
    df = run_sql_script(database, script_path)
    return df

result= process()
print(result)
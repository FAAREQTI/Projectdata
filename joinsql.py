import pandas as pd
import sqlite3

# read SQL script from file
with open("script.sql", "r") as f:
    sql_script = f.read()

# connect to database and execute script
conn = sqlite3.connect("database.db")
df = pd.read_sql(sql_script, conn)

# print resulting dataframe
print(df)

import pandas as pd
from utils import load_csv
from datetime import datetime

path = 'data/supermarket_sales.csv'
df = load_csv(path)

# Convert date and time columns to datetime datatype
# define a lambda function to convert the date string to the desired format


def date_converter(x): return datetime.strptime(x, '%m-%d-%Y').strftime('%Y-%m-%d') \
    if '-' in x else datetime.strptime(x, '%m/%d/%Y').strftime('%Y-%m-%d')


# apply the lambda function to the 'date' column of the DataFrame
df['date'] = df['date'].apply(date_converter)
df['date'] = pd.to_datetime(df['date'])

# create table 1
table1 = df[['invoice_id', 'branch', 'city',
             'customer_type', 'gender', 'product_line', 'unit_price']]
table1.to_csv('data/table1.csv', index=False)

# create table 2
table2 = df[['invoice_id', 'quantity', 'tax_5_percent',
             'total', 'date', 'time', 'payment']]
table2.to_csv('data/table2.csv', index=False)

# create table 3
table3 = df[['invoice_id', 'cogs',
             'gross_margin_percentage', 'gross_income', 'rating']]
table3.to_csv('data/table3.csv', index=False)

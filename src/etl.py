import pandas as pd

path = '/Users/fatima-ezzahraareqti/Desktop/projectds/supermarket_sales.csv'
df = pd.read_csv('supermarket_sales.csv')

# Convert date and time columns to datetime datatype
df['date'] = pd.to_datetime(df['date'])
df['time'] = pd.to_datetime(df['time'], format='%H:%M').dt.time
# Convert numerical columns to appropriate datatypes
df['unit_price'] = df['unit_price'].astype('float')
df['quantity'] = df['quantity'].astype('int')
df['tax_5_percent'] = df['tax_5_percent'].astype('float')
df['total'] = df['total'].astype('float')
df['cogs'] = df['cogs'].astype('float')
df['gross_margin_percentage'] = df['gross_margin_percentage'].astype('float')
df['gross_income'] = df['gross_income'].astype('float')
df['rating'] = df['rating'].astype('float')


# create table 1
table1 = df[['invoice_id', 'branch', 'city',
             'customer_type', 'gender', 'product_line', 'unit_price']]
table1.to_csv('table1.csv', index=False)

# create table 2
table2 = df[['invoice_id', 'quantity', 'tax_5_percent',
             'total', 'date', 'time', 'payment']]
table2.to_csv('table2.csv', index=False)

# create table 3
table3 = df[['invoice_id', 'cogs',
             'gross_margin_percentage', 'gross_income', 'rating']]
table3.to_csv('table3.csv', index=False)

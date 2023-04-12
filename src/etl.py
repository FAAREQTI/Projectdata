import pandas as pd
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

df[['invoice_id', 'branch', 'city', 'customer_type',
    'gender', 'product_line', 'unit_price']]

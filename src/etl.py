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
# Create a dictionary with column names and SQL data types
col_type = {
    'invoice_id': 'varchar(225)',
    'branch': 'varchar(225)',
    'city': 'varchar(225)',
    'customer_type': 'varchar(225)',
    'gender': 'varchar(225)',
    'product_line': 'varchar(225)',
    'unit_price': 'numeric(10,2)',
    'quantity': 'int',
    'tax_5_percent': 'numeric(10,2)',
    'total': 'numeric(10,2)',
    'date': 'date',
    'time': 'time',
    'payment': 'varchar(225)',
    'cogs': 'numeric(10,2)',
    'gross_margin_percentage': 'numeric(10,2)',
    'gross_income': 'numeric(10,2)',
    'rating': 'numeric(10,2)'
}
# STEP 1: CONVERT DATA TYPES to SQL FORMAT

df.dtypes  # int, float, object -> int, decimal(6,2), varchar(255)
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


def convert_types_to_sql_format(df):
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

    return col_type, values


cursor.execute("CREATE TABLE table1 (invoice_id INT, branch VARCHAR(255), city VARCHAR(255), customer_type VARCHAR(255), gender VARCHAR(255), product_line VARCHAR(255), unit_price FLOAT)")

cursor.execute("CREATE TABLE table2 (invoice_id INT, quantity INT, tax_5_percent FLOAT, total FLOAT, date DATE, time TIME, payment VARCHAR(255))")

cursor.execute(
    "CREATE TABLE table3 (invoice_id INT, cogs FLOAT, gross_margin_percentage FLOAT, gross_income FLOAT, rating FLOAT)")

import pandas as pd
from datetime import datetime
import os
import sys
from datetime import datetime
import pytest 
from utils import load_csv2
import re

path = 'data/supermarket_sales.csv'
df = load_csv2(path)

if __name__ == "__main__":
    path = 'data/supermarket_sales.csv'
    df = load_csv2(path)  
    print("Original date values:")
    print(df['date'].head())


# Converting string date
def date_converter(date_str):
    return datetime.strptime(date_str, "%m/%d/%Y").strftime("%Y-%m-%d")

def convert_date_column(df: pd.DataFrame, column: str = "date") -> pd.DataFrame:
    df[column] = df[column].apply(date_converter)
    return df



def test_date_column_format(df: pd.DataFrame, column: str = "date"):
    pattern = re.compile(r'^\d{4}-\d{2}-\d{2}$')
    for val in df[column]:
        assert pattern.match(val), f"❌ Invalid date format: {val}"

@pytest.fixture
def df():
    path = 'data/supermarket_sales.csv'
    return load_csv2(path)

def test_date_column_format2(df):
    pattern = re.compile(r'^\d{4}-\d{2}-\d{2}$')
    for val in df['date']:
        assert pattern.match(val), f"❌ Invalid date format: {val}"

from typing import Tuple

# convering python to sql format
def convert_types_to_sql_format(df2: pd.DataFrame) -> Tuple[str, str]:
    """Converts pandas data types to SQL data types.
    Args:
        df (pd.DataFrame): Pandas dataframe to be converted
    Returns:
        Tuple[str, str]: tuple contaning SQL column names and values
    """
    try:
        types = []
        for i in df2.dtypes:
            if i == 'int64':
                types.append('int')
            elif i == 'object':
                types.append('VARCHAR(255)')
            elif i == 'float':
                types.append("DECIMAL(6,2)")

        col_type = list(zip(df2.columns.values, types))
        col_type = tuple([" ".join(i) for i in col_type])
        col_type = ', '.join(col_type)
        values = ', '.join(["%s" for i in range(len(df2.columns))])
    except Exception as e:
        print("Error:", e)
    return col_type, values


def test_convert_types_to_sql_format():
    df2 = pd.DataFrame({
        'name': ['Alice', 'Bob'],
        'age': [25, 30],
        'salary': [50000.5, 60000.75]
    })

    col_type, values = convert_types_to_sql_format(df2)

    assert col_type == "name VARCHAR(255), age int, salary DECIMAL(6,2)"
    assert values == "%s, %s, %s"
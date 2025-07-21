import pandas as pd
import pytest
from src.utils import load_csv
import warnings 
import psycopg2
warnings.filterwarnings("ignore")



@pytest.fixture
def data_df():
    df = load_csv('data/supermarket_sales.csv')
    return df

@pytest.fixture
def data_table1():
    table1 = load_csv('data/table1.csv')
    return table1

@pytest.fixture
def data_table2():
    table2 = load_csv('data/table2.csv')
    return table2

@pytest.fixture
def data_table3():
    table3 = load_csv('data/table3.csv')
    return table3


@pytest.fixture(scope="session")
def df2():
    """Fixture that returns a test dataframe"""
    return pd.DataFrame({
        'city': ['CityA'] * 10 + ['CityB'] * 10,
        'product_line': ['Electronics'] * 5 + ['Clothing'] * 5 +
                        ['Electronics'] * 5 + ['Clothing'] * 5,
        'date': pd.date_range(start='2023-01-01', periods=10).tolist() * 2,
        'quantity': [10, 12, 11, 13, 12, 20, 22, 21, 23, 22,
                     14, 15, 13, 17, 16, 18, 20, 19, 21, 22]
    })


@pytest.fixture
def df():
    path = 'data/supermarket_sales.csv'
    return load_csv(path)

@pytest.fixture
def connet_dbg():
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(
            host="localhost",
            database='test_db',
            user="fatima",
            password="fafafa99"
        )
        cur = conn.cursor()
    except Exception as e:
        print("Error:", e)
    return conn, cur

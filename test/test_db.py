import pandas as pd
from datetime import datetime
import pytest
from src.utils import load_csv
    
###This pytest functions check for date_converter functions and the existence of correct columns in extracted tables 

def date_converter(x): 
    return datetime.strptime(x, '%m-%d-%Y').strftime('%Y-%m-%d') \
    if '-' in x else datetime.strptime(x, '%m/%d/%Y').strftime('%Y-%m-%d')

####
@pytest.mark.date
def test_date_converter1():
    assert date_converter('06/29/2023') == '2023-06-29'

@pytest.mark.date
def test_date_converter2():
    assert date_converter('06/28/2023') == '2023/06/28'

@pytest.mark.parametrize("first_date, output_date", [("06/29/2023", "2023-06-29"), ("06/28/2023","2023/06/28" )])
def test_date_converter(first_date, output_date):
    assert date_converter(first_date) == output_date

####

@pytest.mark.data
def test_customer_creation(data_table1):
    expected_columns1 = [
        'invoice_id', 'branch', 'city',
        'customer_type', 'gender', 'product_line', 'unit_price'
    ]
    assert list(data_table1.columns) == expected_columns1
    

####
@pytest.mark.data
def test_sales_creation(data_table2):
    expected_columns2 = [
        'invoice_id', 'quantity', 'tax_5_percent',
             'total', 'date', 'time', 'payment'
    ]
    assert list(data_table2.columns) == expected_columns2


####
@pytest.mark.data
@pytest.mark.xfail
def test_kpi_creation(data_table3):
    expected_columns3 = [
        'invoice_id', 'cogs',
             'gross_margin_percentage', 'gross_incom', 'rating'
    ]
    assert list(data_table3.columns) == expected_columns3



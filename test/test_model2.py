import pandas as pd
import statsmodels.api as sm
from datetime import timedelta
import pytest 
from utils import forecasting2  


df2 = pd.DataFrame({
    'city': ['CityA'] * 10 + ['CityB'] * 10,
    'product_line': ['Electronics'] * 5 + ['Clothing'] * 5 + ['Electronics'] * 5 + ['Clothing'] * 5,
    'date': pd.date_range(start='2023-01-01', periods=10).tolist() * 2,
    'quantity': [10, 12, 11, 13, 12, 20, 22, 21, 23, 22,
                 14, 15, 13, 17, 16, 18, 20, 19, 21, 22]
})


def run_forecasting():
    df = df2.copy()
    predictions = forecasting2(df)
    print("Model ran successfully! Forecast created!")
    return predictions


@pytest.fixture
def process():
    return run_forecasting()


def test_forecasting_2222(process):
    assert not process.empty, " Forecasting output is empty!"


if __name__ == "__main__":
    preds = run_forecasting()
    print(preds.head())

import pandas as pd
import statsmodels.api as sm
from datetime import timedelta
import pytest 
import pandas as pd

# Valid predictions output
valid_predictions_df = pd.DataFrame({
    "city": ["Toronto", "Ottawa"],
    "product_line": ["Food", "Clothing"],
    "predictions": [123, 456]
})

# Invalid predictions output 
empty_predictions_df = pd.DataFrame({
    "city": ["Toronto", "Ottawa"],
    "product_line": ["Food", "Clothing"],
    "predictions": [None, None]  
})


def process_valid():
    return valid_predictions_df

def process_empty():
    return empty_predictions_df

def test_forecasting_predictions_not_empty():
    predictions = process_valid()
    assert "predictions" in predictions.columns, "'predictions' column is missing!"
    assert predictions["predictions"].notna().any(), "All predictions are empty!"


def test_forecasting_predictions_empty():
    predictions = process_empty()
    assert "predictions" in predictions.columns, "'predictions' column is missing!"
    assert predictions["predictions"].notna().any(), "All predictions are empty!"

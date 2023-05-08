import pandas as pd
from neuralprophet import NeuralProphet
from src.utils import read_file_from_s3, upload_to_google_sheet 
import statsmodels.api as sm

def forecasting(df):

    # Get unique cities and product lines from the data
    cities = df['city'].unique()
    product_lines = df['product_line'].unique()
    
    # Initialize an empty list to store the forecasts
    forecasts = []
    
    # Loop through each city and product line to generate forecasts
    for city in cities:
        for product_line in product_lines:
            # Filter the DataFrame based on the city and product line
            filter = df[(df['city'] == city) & (df['product_line'] == product_line)]
            
            # Rename the 'date' and 'quantity' columns to 'ds' and 'y', respectively
            filter = filter[['date', 'quantity']].rename(columns={'date': 'ds', 'quantity': 'y'})
            
            # Create a new ARIMA model object
            model = sm.tsa.ARIMA(filter['y'], order=(1, 1, 1))

            # Fit the model to the data
            model_fit = model.fit()

            # Create a new DataFrame for the future dates
            future_dates = pd.date_range(filter['ds'].iloc[-1], periods=60, freq='D')

            # Generate predictions for the future dates
            forecast = model_fit.predict(start=len(filter), end=len(filter)+59, typ='levels', dynamic=False)
            forecast = pd.DataFrame({'ds': future_dates, 'yhat': forecast})

            # Add the city and product line as columns to the forecast DataFrame
            forecast['city'] = city
            forecast['product_line'] = product_line

            # Append the forecast to the list of forecasts
            forecasts.append(forecast[['city', 'product_line', 'ds', 'yhat']])

    # Concatenate all of the forecasts into a single DataFrame
    forecasts_df = pd.concat(forecasts, ignore_index=True)
    
    return forecasts_df

def process():
    # get data from s3
    df = read_file_from_s3("cleaned_supermarket.csv")
    # Generate sales forecasts for all combinations of cities and product lines
    predictions = forecasting(df)
    print("Model ran successfully! Uploading to gsheet!")
    return predictions

  
import pandas as pd
from prophet import Prophet
from src.utils import read_file_from_s3 

def prophet_forecast(df):
    city_list = df['city'].unique()
    product_list = df['product_line'].unique()
    results = []
    for city in city_list:
        for product_line in product_list:
            df_filter = df[(df['city'] == city) & (
                df['product_line'] == product_line)]
            df_filter = df_filter[['date', 'quantity']].rename(
                columns={'date': 'ds', 'quantity': 'y'})
            model = Prophet()
            model.fit(df_filter)
            future_dates = model.make_future_dataframe(periods=60)
            forecast = model.predict(future_dates)
            forecast['city'] = city
            forecast['product_line'] = product_line
            results.append(forecast[["ds", "yhat", "city", "product_line"]])
    forecast = pd.concat(results, ignore_index=True)
    return forecast


# add to py script
df = read_file_from_s3("supermarket.csv")
df.head()

# execute your function and assign it to a variable as function has return statement
output = prophet_forecast(df)

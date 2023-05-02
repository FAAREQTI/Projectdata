import pandas as pd
from neuralprophet import NeuralProphet
from utils import read_file_from_s3, upload_to_google_sheet 

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
            model = NeuralProphet()
            df_filter=df_filter.drop_duplicates(["ds"], keep="first")
            model.fit(df_filter, freq="D")
            future_dates = model.make_future_dataframe(df_filter, n_historic_predictions=True, periods=60)
            forecast = model.predict(future_dates)
            forecast['city'] = city
            forecast['product_line'] = product_line
            results.append(forecast[["ds", "yhat1", "city", "product_line"]])
    forecast = pd.concat(results, ignore_index=True)
    return forecast


# add to py script
df = read_file_from_s3("supermarket.csv")

# execute your function and assign it to a variable as function has return statement
output = prophet_forecast(df)

spreadsheet_id = "1xjPE5wUM7dd5JuZaZXoEIKMgeOJpNcZ5i_uw7EXwQIk"
worksheet_name = "supermarket"

upload_to_google_sheet(spreadsheet_id, output, worksheet_name)    
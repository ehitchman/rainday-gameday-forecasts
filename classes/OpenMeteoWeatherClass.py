import openmeteo_requests

import requests_cache
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from retry_requests import retry

from classes.ConfigManagerClass import ConfigManager
from classes.GCS import GCSManager

class WeatherHistoryRetriever:
    def __init__(self):
        # Setup the Open-Meteo API client with cache and retry on error
        cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
        retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
        self.openmeteo = openmeteo_requests.Client(session=retry_session)

        # Setup Config manager and GCS client
        self.config = ConfigManager(yaml_filepath='config', yaml_filename='config.yaml')
        self.gcs_manager = GCSManager()

    def fetch_and_process(self, latitude, longitude, start_date, end_date, name):
        url = "https://archive-api.open-meteo.com/v1/archive"
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "start_date": start_date,
            "end_date": end_date,
            "hourly": ["temperature_2m", "relative_humidity_2m", "apparent_temperature", "precipitation", "rain", "snowfall", "wind_speed_10m"]
        }
        responses = self.openmeteo.weather_api(url, params=params)

        # Process the response
        response = responses[0]
        hourly = response.Hourly()
        hourly_data = self._process_hourly_data(hourly, name)

        return pd.DataFrame(data=hourly_data)

    def _process_hourly_data(self, hourly, name):
        # Original columns from the function
        variable_names = ["temperature_2m", "relative_humidity_2m", "apparent_temperature", "precipitation", "rain", "snowfall", "wind_speed_10m"]
        variables = [hourly.Variables(i).ValuesAsNumpy() for i in range(len(variable_names))]
        
        # Create a dictionary for the data with new column names aligned with forecast_schema
        hourly_data = {
            "forecast_datetime": pd.date_range(
                start=pd.to_datetime(hourly.Time(), unit="s"),
                end=pd.to_datetime(hourly.TimeEnd(), unit="s"),
                freq=pd.Timedelta(seconds=hourly.Interval()),
                inclusive="left"
            ),
            "temp": variables[0],  # Assuming temperature_2m corresponds to temp
            "temp_humidity": variables[1],  # Assuming relative_humidity_2m corresponds to temp_humidity
        }

        # Filter out rows where the hour is not divisible by 3
        df = pd.DataFrame(data=hourly_data)
        df['name'] = name
        df = df[df['forecast_datetime'].dt.hour % 3 == 0]

        return df

    def send_df_to_bq(self, df_union):
        print("Do something...")
        self.gcs_manager.write_df_to_gcs(
            df=df_union, 
            bucket_name = self.config.bucket_name, 
            gcs_bucket_filepath = self.config.wthr_historic_unioned_csvpath
            )
        message = "Looks like the write_df_to_gcs() has completed successfully"
        return message
    
# Example usage
if __name__ == "__main__":
    config = ConfigManager(yaml_filepath='config', yaml_filename='config.yaml')
    wthr_history_retriever = WeatherHistoryRetriever()

    users_details = config.users_details

    dfs = []

    six_days_ago = (datetime.now() - timedelta(days=6)).strftime('%Y-%m-%d')
    start_date = six_days_ago
    end_date = six_days_ago

    for user in users_details:
        name = user['name']
        lat = user['lat']
        lon = user['lon']
        print(f"Fetching data for {name}...")
        df = wthr_history_retriever.fetch_and_process(lat, lon, start_date, end_date, name)
        dfs.append(df)
    
    df_unioned = pd.concat(dfs, ignore_index=True)

    wthr_history_retriever.send_df_to_bq(df_union=df_unioned)
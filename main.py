import functions_framework
import pandas as pd
import os
from datetime import datetime, timedelta

from classes.ConfigManagerClass import ConfigManager
from classes.LoggingClass import LoggingManager
from classes.GCS import GCSManager
from classes.OpenWeatherMap import WeatherForecastRetriever
from classes.OpenMeteoWeatherClass import WeatherHistoryRetriever


#os.environ['RAINDAY_IN_CLOUD_ENVIRONMENT'] = 'yes'
runtime_logger_level = 'DEBUG'

class WeatherForecaster():
    def __init__(self):
        self.config = ConfigManager(yaml_filename = 'config.yaml', yaml_filepath = 'config')
        self.forecast_manager = WeatherForecastRetriever()
        self.gcs_manager = GCSManager()
        self.logging_manager = LoggingManager() 
        self.logger = self.logging_manager.create_logger(
            logger_name='log_main',
            debug_level=runtime_logger_level,
            stream_logs=True,
            mode='a'
        )

    @functions_framework.http
    def get_weather_forecast_and_write_to_gcs(
        self
        ):
        """
        Returns a combined list of dataframes with each users forecast from config.yml

        """
        #GCS related variables
        bucket_name = self.config.bucket_name
        gcs_folder_path = self.config.forecast_csvpath
        bcs_file_name_wout_date = self.config.individual_forecast_csvpath
        todays_date = datetime.today().strftime('%Y-%m-%d')
        gcs_file_path = gcs_folder_path+'/'+f'5-day forecast_{todays_date}'+'.csv'

        #set/declare variables/objects
        list_of_all_forecast_details_dfs = []

        # config_users is iterated over to grab a forecast for each user
        config_users = self.config.users_details
        for user in config_users:

            #grab user details for use in weather forecasting
            user_name = user['name'] 
            user_lat = user['lat'] 
            user_lon = user['lon'] 
            user_cityprovince = user['city-province'] 
            self.logger.info(f"{user_name}, {user_cityprovince} ({user_lat}, {user_lon})")

            #Takes users details from the yaml and gets a forecast 
            json_forecast = self.forecast_manager.getWeatherForecast(
                user_name = user_name,
                lon = user_lon,
                lat = user_lat,
                write_to_directory=False
                )
            
            #Transforms the forecast from forecast_manager.getWeatherForecast() to a usable format for
            # caompring dates/times/weather conditions
            json_forecast_details = self.forecast_manager.transformJsonForecast( 
                json_forecast
                )
            
            #upload INDIVIDUAL forecast to GCS
            self.gcs_manager.write_df_to_gcs(df=json_forecast_details, 
                            bucket_name=bucket_name, 
                            gcs_bucket_filepath=bcs_file_name_wout_date+'/'+'5-day forecast_'+user_name+'.csv', 
                            is_testing_run=False)
            
            #append df to new all forecasts list item and users name list item
            list_of_all_forecast_details_dfs.append(json_forecast_details)

        #concatenate the list of DFs to a single DF for use in analysis/viz layer
        forecasts_details_concat = pd.concat(list_of_all_forecast_details_dfs)

        self.logger.info("-----------------------")
        self.logger.info("-----------------------")
        self.logger.info("THIS IS THE forecasts_details_concat:")            
        self.logger.info(forecasts_details_concat)
        self.logger.info("-----------------------")
        self.logger.info("-----------------------")

        #upload to GCS
        self.gcs_manager.write_df_to_gcs(df=forecasts_details_concat, 
                        bucket_name=bucket_name, 
                        gcs_bucket_filepath=gcs_file_path, 
                        is_testing_run=False)

        return(f"FINISHED: The list of forecasts has been saved to GCS bucket: {bucket_name} in location: {gcs_file_path}")


    @functions_framework.http
    def union_and_write_gcs_blob_forecasts_to_gcs(
        self, 
        request=None
        ):
        """
        A Cloud Function that unions multiple CSV files from a Google Cloud Storage (GCS) bucket
        and writes the combined result to another GCS blob.

        Args:
            request (flask.Request, optional): The HTTP request object. Defaults to None.
            is_testing_run (bool, optional): Specifies if the function is being run in a testing environment.
                Defaults to False.

        Returns:
            str: A message indicating the successful completion of the function.

        Raises:
            N/A

        """
        # Define the GCS bucket and file information
        bucket_name = self.config.bucket_name

        # Get all historic daily CSV files from the bucket and union them together
        #  - directory should contain multiple files
        csvs_to_union_folder_location = self.config.forecast_csvpath    
        blobs_list = self.gcs_manager.list_gcs_blobs(bucket_name=bucket_name)
        unioned_forecasts = self.gcs_manager.union_gcs_csv_blobs(
            blobs_list=blobs_list,
            csvs_to_union_folder_location=csvs_to_union_folder_location
            )

        # Write the unioned forecasts to GCS. File will contain a row for every 
        # forecast_date_capture, forecast_time, user   
        gcs_file_name = 'all_historic_forecasts.csv' 
        gcs_forecasthistory_bucket_directory = self.config.forecast_unioned_csvpath
        gcs_forecasthistory_filepath = os.path.join(
            gcs_forecasthistory_bucket_directory, 
            gcs_file_name
            ).replace('\\', '/')
        self.gcs_manager.write_df_to_gcs(
            df=unioned_forecasts,
            bucket_name=bucket_name,
            gcs_bucket_filepath=gcs_forecasthistory_filepath,
            is_testing_run=False
            )
        return print(f"FINISHED: The combined/unioned forecasts have been saved to GCS bucket: {bucket_name} in location: {gcs_forecasthistory_filepath}")
     
#entry point for rainday-gameday_get-union-and-store-forecasts
@functions_framework.http
def main(request=None): 
    forecaster = WeatherForecaster()
    forecaster.get_weather_forecast_and_write_to_gcs()
    forecaster.union_and_write_gcs_blob_forecasts_to_gcs()
    message = "FINAL: Looks like the main() function ran without issue!"
    return message

#entry point for rainday-gameday_get-historic-openmeteo-weather
@functions_framework.http
def get_historic_weather():
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

    result =wthr_history_retriever.send_df_to_bq(df_union=df_unioned)
 
    return result

if __name__ == '__main__':
    print("Tests included in main.py, however only run this command if you're certain you'd like to overwrite the forecast that may have been scheduled for first thing this morning")
    #config = ConfigManager(yaml_filename='config.yaml', yaml_filepath='config')
    #return_message_get_weather_forecast = main()
    #print(return_message_get_weather_forecast)
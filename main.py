import functions_framework
import pandas as pd
import os
from datetime import datetime, timedelta
import base64
import json

from classes.ConfigManagerClass import ConfigManager
from classes.LoggingClass import LoggingManager
from classes.GCS import GCSManager
from classes.OpenWeatherMap import WeatherForecastRetriever
from classes.OpenMeteoWeatherClass import WeatherHistoryRetriever
from classes.PubSub import PubSubManager
from classes.BigQueryManager import BigQueryManager

#os.environ['RAINDAY_IN_CLOUD_ENVIRONMENT'] = 'yes'
runtime_logger_level = 'DEBUG'

logging_manager = LoggingManager() 
logger = logging_manager.create_logger(
    logger_name='log_main',
    debug_level=runtime_logger_level,
    stream_logs=True,
    mode='a'
)

class WeatherForecaster():
    def __init__(self):
        self.config = ConfigManager(yaml_filename = 'config.yaml', yaml_filepath = 'config')
        self.forecast_manager = WeatherForecastRetriever()
        self.gcs_manager = GCSManager()
        self.bq_manager = BigQueryManager()

        self.logging_manager = LoggingManager() 
        self.logger = self.logging_manager.create_logger(
            logger_name='log_WeatherForecaster',
            debug_level=runtime_logger_level,
            stream_logs=True,
            mode='a'
        )

    @functions_framework.http
    def get_weather_forecast_and_write_to_gcs(self):
        """
        Returns a combined list of dataframes with each users forecast from config.yml

        """
        #GCS related variables
        bucket_name = self.config.bucket_name
        gcs_folder_path = self.config.wthr_forecast_csvpath
        bcs_file_name_wout_date = self.config.wthr_forecast_individual_csvpath
        todays_date = datetime.today().strftime('%Y-%m-%d')

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
            gcs_filepath=bcs_file_name_wout_date+'/'+'5-day forecast_'+user_name+'.csv'
            self.gcs_manager.write_df_to_gcs(df=json_forecast_details, 
                            bucket_name=bucket_name, 
                            gcs_bucket_filepath=gcs_filepath, 
                            is_testing_run=False)
            
            #append df to new all forecasts list item and users name list item
            list_of_all_forecast_details_dfs.append(json_forecast_details)

        #concatenate the list of DFs to a single DF for use in analysis/viz layer
        forecasts_details_concat = pd.concat(list_of_all_forecast_details_dfs)

        self.logger.debug("This is the forecasts_details_concat:")            
        self.logger.debug(forecasts_details_concat)

        #upload to GCS
        gcs_filepath = gcs_folder_path+'/'+f'5-day forecast_{todays_date}'+'.csv'
        self.gcs_manager.write_df_to_gcs(df=forecasts_details_concat, 
                        bucket_name=bucket_name, 
                        gcs_bucket_filepath=gcs_filepath, 
                        is_testing_run=False)

        return(f"FINISHED: The list of forecasts has been saved to GCS bucket: {bucket_name} in location: {gcs_filepath}")


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
        blobs_list = self.gcs_manager.list_gcs_blobs(bucket_name=bucket_name)

        unioned_forecasts = self.gcs_manager.union_gcs_csv_blobs(
            blobs_list=blobs_list,
            csvs_to_union_folder_location=self.config.wthr_forecast_folderpath
            )

        # Write the unioned forecasts to GCS. File will contain a row for every 
        # forecast_date_capture, forecast_time, user   
        gcs_file_name = self.config.wthr_forecast_unioned_filename
        wthr_forecast_unioned_folderpath = self.config.wthr_forecast_unioned_folderpath

        gcs_filepath = os.path.join(
            wthr_forecast_unioned_folderpath, 
            gcs_file_name
            ).replace('\\', '/')
  
        # Coerce to pandas dattime object:
        unioned_forecasts['forecast_datetime'] = pd.to_datetime(unioned_forecasts['forecast_datetime'])

        # Write to GCS
        self.gcs_manager.write_df_to_gcs(
            df=unioned_forecasts,
            bucket_name=bucket_name,
            gcs_bucket_filepath=gcs_filepath,
            is_testing_run=False
            )
        return print(f"FINISHED: The combined/unioned forecasts have been saved to GCS bucket: {bucket_name} in location: {gcs_filepath}")
     
#entry point for rainday-gameday_get-union-and-store-forecasts
@functions_framework.http
def main(request=None): 
    logger.info("starting main:")
    forecaster = WeatherForecaster()
    forecaster.get_weather_forecast_and_write_to_gcs()
    forecaster.union_and_write_gcs_blob_forecasts_to_gcs()
    message = "FINAL: Looks like the main() function ran without issue!"
    return message

#entry point for rainday-gameday_get-historic-openmeteo-weather
@functions_framework.http
def get_historic_weather(request=None):
    dfs = []
    gcs_manager = GCSManager()
    config = ConfigManager(yaml_filepath='config', yaml_filename='config.yaml')
    wthr_history_retriever = WeatherHistoryRetriever()

    logger.info("starting get_historic_weather:")
    logger.info(f"The writepath, config.wthr_historic_csvpath: {config.wthr_historic_csvpath}")

    # Get daily historic wthr from openmeteo
    six_days_ago = (datetime.now() - timedelta(days=6)).strftime('%Y-%m-%d')
    start_date = six_days_ago
    end_date = six_days_ago
    users_details = config.users_details

    try:
        for user in users_details:
            name = user['name']
            lat = user['lat']
            lon = user['lon']
            print(f"Fetching data for {name}...")
            df = wthr_history_retriever.fetch_and_process(lat, lon, start_date, end_date, name)
            dfs.append(df)
        df_unioned = pd.concat(dfs, ignore_index=True)

        # Write daily historic wthr to GCS 
        gcs_filepath = config.wthr_historic_csvpath+f'_{six_days_ago}.csv'
        result = gcs_manager.write_df_to_gcs(
            df=df_unioned,
            bucket_name=config.bucket_name,
            gcs_bucket_filepath=gcs_filepath
        )
        outcome='complete'
    except:
        outcome='failed'

    # Create Pubsub message, create publisher, publsih topic data
    data_json = {
            'status': outcome, 
            'timestamp': datetime.now().strftime("%d-%m-%Y, %H:%M:%S")
        }
    data_bytestr = json.dumps(data_json).encode("utf-8")
    publisher = PubSubManager(config.pubsub_project_id, topic_id='get_historic_weather')
    publisher.publish_topic_data(data_bytestr=data_bytestr)

    return result

@functions_framework.cloud_event
def transform_historic_weather(
    self,
    cloud_event=None
    ):
    # Initialize configuration and Google Cloud Storage Manager
    # config = ConfigManager(yaml_filepath='config', yaml_filename='config.yaml')
    # gcs_manager = GCSManager()

    # Logging the paths for reference
    logger.info("Starting transform_historic_weather:")
    logger.info(f"The read path for unioning, config.wthr_historic_csvpath: {self.config.wthr_historic_csvpath}")
    logger.info(f"The write path, config.wthr_historic_unioned_csvpath: {self.config.wthr_historic_unioned_csvpath}")

    # Simulating a Pub/Sub message for local testing
    if cloud_event is None:
        logger.info("Running locally, simulating cloud_event for testing.")
        test_data_json = {
            'status': 'test',
            'timestamp': datetime.now().strftime("%d-%m-%Y, %H:%M:%S")
        }
        test_message_data = json.dumps(test_data_json).encode("utf-8")
        test_message_data_base64 = base64.b64encode(test_message_data).decode('utf-8')
        cloud_event = type('test', (object,), {})()  # Creating a mock object
        cloud_event.data = {'message': {'data': test_message_data_base64}}

    # Check if cloud_event has data and a message
    if cloud_event.data and "message" in cloud_event.data:
        message_data = base64.b64decode(cloud_event.data["message"]["data"]).decode("utf-8")
        logger.info(f"Received message data: {message_data}")

        try:
            message_json = json.loads(message_data)
            completion_status = message_json.get('status')
        except json.JSONDecodeError:
            logger.error("Error decoding message data as JSON.")
            return "Error in processing - message data not in JSON format"

        if completion_status == 'complete':
            # Get daily historic weather data from GCS and union them
            blobs_list = self.gcs_manager.list_gcs_blobs(bucket_name=self.config.bucket_name)
            whtr_historic_unioned = self.gcs_manager.union_gcs_csv_blobs(
                blobs_list=blobs_list,
                csvs_to_union_folder_location=self.config.wthr_historic_csvpath
            )

            # Write unioned daily historic weather data to GCS
            gcs_filepath = self.config.wthr_historic_unioned_csvpath + '.csv'
            message_result = self.gcs_manager.write_df_to_gcs(
                df=whtr_historic_unioned,
                bucket_name=self.config.bucket_name,
                gcs_bucket_filepath=gcs_filepath
            )

            # Return a success message or result
            return f"Processed successfully: {message_result}"
        else:
            # Log and return if the completion status is not 'complete'
            logger.error("The completion_status was not 'complete'. Function aborted.")
            return "Aborted: The completion_status was not 'complete'"
    else:
        # Log and return an error if no message data is found
        logger.error("No message data found in the cloud event.")
        return "Error: No message data found in the cloud event."

@functions_framework.cloud_event
def bq_create_or_replace_historic_weather_unioned(
    cloud_event=None
    ):
    bq_manager = BigQueryManager()
    config = ConfigManager(yaml_filename='config.yaml', yaml_filepath='config')
    
    # Historic Weather
    bq_manager.create_or_replace_bq_table_from_gcs(
        project_name=config.gcp_project_name,
        source_bucket_name=config.bucket_name,
        source_dir_path=config.wthr_historic_unioned_folderpath,
        source_file_name=config.wthr_historic_unioned_filename,
        target_dataset_name=config.bq_dataset_name,
        target_table_name=config.bq_historic_table_name,
        schema=config.bq_schemas_historic_weather
        )

@functions_framework.cloud_event
def bq_create_or_replace_historic_forecasts_unioned(
    cloud_event=None
    ):
    bq_manager = BigQueryManager()
    config = ConfigManager(yaml_filename='config.yaml', yaml_filepath='config')
    
    # Historic Forecasts
    bq_manager.create_or_replace_bq_table_from_gcs(
        project_name=config.gcp_project_name,
        source_bucket_name=config.bucket_name,
        source_dir_path=config.wthr_forecast_unioned_folderpath,
        source_file_name=config.wthr_forecast_unioned_filename,
        target_dataset_name=config.bq_dataset_name,
        target_table_name=config.bq_forecast_table_name,
        schema=config.bq_schemas_historic_forecast
        )

if __name__ == '__main__':
    config = ConfigManager(yaml_filename='config.yaml', yaml_filepath='config')
    weather_forecaster = WeatherForecaster()
    print("Tests included in main.py, however only run these tests if you're certain you'd like to overwrite the forecast that may have been scheduled for first thing this morning")
    print(f"This is the project name: {config.gcp_project_name}")
    # main()
    # get_historic_weather()
    # transform_historic_weather()
    #bq_create_or_replace_historic_weather_unioned()
    #weather_forecaster.union_and_write_gcs_blob_forecasts_to_gcs()
    #bq_create_or_replace_historic_forecasts_unioned()
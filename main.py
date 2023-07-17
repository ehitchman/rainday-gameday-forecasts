#%%

import functions_framework

@functions_framework.http
def main(request=None, is_testing_run=False):
    """
    Returns a combined list of dataframes with each users forecast from config.yml

    """

    #import modules
    from modules import getWeatherForecast, transformJsonForecast_table, load_yaml, write_df_to_gcs, list_gcs_blobs
    import pandas as pd
    from datetime import datetime
    from google.cloud import storage

    #load environment variables and config.yml
    yaml_data = load_yaml()

    #keys, etc.
    openweathermap_api_key = yaml_data['OPENWEATHERMAP_API_KEY']

    #GCS related variables
    bucket_name = yaml_data['bucket_name']
    todays_date = datetime.today().strftime('%Y-%m-%d')
    gcs_folder_path = yaml_data['5dayforecast_csvdir']
    gcs_file_path = gcs_folder_path+'/'+f'5-day forecast_{todays_date}'+'.csv'
    bcs_file_name_wout_date = yaml_data['5dayforecast_individual_csvpath']

    #set/declare variables/objects
    log_response_file_name = yaml_data['response_file_name']
    log_responses_directory = yaml_data['log_responses_directory']
    list_of_all_forecast_details_dfs = []
    list_of_all_users_names = []

    #Get config_users from either yaml or local if is_testing_run=True.
    # config_users is iterated over to grab forecasts
    #is_testing_run=True
    if is_testing_run==True:
        config_users = [{'name': 'testuser', 'lat': 100, 'lon': 100, 'city-province': 'Toronto, Ontario'}]
    else:
        config_users = yaml_data['users_details']

    #iterate over each entry in config_users
    for user in config_users:

        #grab user details for use in weather forecasting
        user_name = user['name'] 
        user_lat = user['lat'] 
        user_lon = user['lon'] 
        user_cityprovince = user['city-province'] 
        print(f"{user_name}, {user_cityprovince} ({user_lat}, {user_lon})")

        #Takes users details from the yaml and gets a forecast 
        json_forecast = getWeatherForecast(openweathermap_api_key=openweathermap_api_key,
                                           user_name = user_name,
                                           lon = user_lon,
                                           lat = user_lat,
                                           is_testing_run=False, #is testing_run==True makes default 
                                            # test user items that do not align with the 'user_name'
                                            # used in main.py.  Potential solution would be to move
                                            # the is_testing_run to outside of the fucntion and
                                            # into main.py, or to include a level of testing for 
                                            # both the function and main.py
                                           write_to_directory=False,
                                           log_response_file_name = log_response_file_name,
                                           log_responses_directory = log_responses_directory)

        #Transforms the forecast from getWeatherForecast() to a usable format for
        # caompring dates/times/weather conditions
        json_forecast_details = transformJsonForecast_table(json_forecast, 
                                                            is_testing_run=False)
        
        #upload INDIVIDUAL forecast to GCS
        write_df_to_gcs(df=json_forecast_details, 
                        bucket_name=bucket_name, 
                        gcs_bucket_filepath=bcs_file_name_wout_date+'/'+'5-day forecast_'+user_name+'.csv', 
                        is_testing_run=False)
        
        #append df to new all forecasts list item and users name list item
        list_of_all_forecast_details_dfs.append(json_forecast_details)
        list_of_all_users_names.append(user_name)

    #concatenate the list of DFs to a single DF for use in analysis/viz layer
    forecasts_details_concat = pd.concat(list_of_all_forecast_details_dfs)

    #upload to GCS
    write_df_to_gcs(df=forecasts_details_concat, 
                    bucket_name=bucket_name, 
                    gcs_bucket_filepath=gcs_file_path, 
                    is_testing_run=False)

    #list GCS blobs
    #list_gcs_blobs(bucket_name=bucket_name)
     
    return(f"FINISHED: The list of forecasts has been saved to GCS bucket: {bucket_name} in location: {gcs_file_path}")


@functions_framework.http
def union_and_write_gcs_blob_forecasts_to_gcs(request=None, is_testing_run=False):
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

    # Import necessary libraries and modules
    import pandas as pd
    from modules import load_yaml, list_gcs_blobs, union_gcs_csv_blobs, write_df_to_gcs, write_to_csv_and_xlsx
    import os

    # Load the YAML configuration data
    yaml_data = load_yaml()

    # Define the GCS bucket and file information
    bucket_name = yaml_data['bucket_name']
    
    #This is the scripts output, the final unioned forecast.  The file will
    # contain a row for every forecast_date_capture, forecast_time, user   
    gcs_file_name = 'all_historic_forecasts.csv' 
    gcs_forecasthistory_bucket_directory = yaml_data['5dayforecast_historic_forecast_csvdir']
    gcs_forecasthistory_filepath = os.path.join(gcs_forecasthistory_bucket_directory, 
                                                gcs_file_name).replace('\\', '/')
    
    # Get all historic daily CSV files from the bucket and union them together
    #  - directory should contain multiple files, each one containing a single
    # forecast containing multiple forecast times and users
    csvs_to_union_folder_location = yaml_data['5dayforecast_csvdir']
    blobs_list = list_gcs_blobs(bucket_name=bucket_name)
    unioned_forecasts = union_gcs_csv_blobs(blobs_list=blobs_list,
                                            csvs_to_union_folder_location=csvs_to_union_folder_location)

    # TEST: Write unioned_Forecasts to file for invest.
    #write_to_csv_and_xlsx(df=unioned_forecasts, filename='unioned_forecasts')

    # Write the unioned forecasts to GCS
    write_df_to_gcs(df=unioned_forecasts,
                    bucket_name=bucket_name,
                    gcs_bucket_filepath=gcs_forecasthistory_filepath,
                    is_testing_run=False)

    return print(f"FINISHED: The combined/unioned forecasts have been saved to GCS bucket: {bucket_name} in location: {gcs_forecasthistory_filepath}")

if __name__ == '__main__':
    main()
    union_and_write_gcs_blob_forecasts_to_gcs()

#%%

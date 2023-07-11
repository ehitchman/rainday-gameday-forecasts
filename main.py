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
    gcs_file_path = yaml_data['5dayforecast_csvpath']+f'_{todays_date}'+'.csv'
    bcs_file_path_wout_date = yaml_data['5dayforecast_individual_csvpath']

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
                        gcs_bucket_filepath=bcs_file_path_wout_date+"_"+user_name+'.csv', 
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
    list_gcs_blobs(bucket_name=bucket_name)
     
    return(f"The list of forecasts has been saved to GCS bucket: {bucket_name} in location: {gcs_file_path}")

if __name__ == '__main__':
    main()
#%%

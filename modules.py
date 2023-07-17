#------------------------------------------------------------------------------#
#Notes/Questions


#------------------------------------------------------------------------------#

#load environment/yaml
def  load_yaml(filepathfromwd='config.yaml'):
    import yaml
    import os 

    ########################################
    #Load YAML FILE FIRST (ensures that envfile_path_string is loaded to capture environment)
    fullfilepath = os.path.join(os.getcwd(),filepathfromwd)
    with open(fullfilepath, "r") as file:
        yaml_data = yaml.safe_load(file)
        file.close
    print(f"Yaml file loaded from path:{fullfilepath}")

    return yaml_data
    #EOF


#load environment/yaml
def load_environment(yaml_data):
    import os
    from dotenv import load_dotenv

    #########################################
    #NOTE: should this be moved to main.py?
    #NOTE: Load environment file via path object sourced from yaml and get the environment
    # variables for __________
    envfile_path_string = os.path.join(os.getcwd(),yaml_data['envfile_name'])
    print("Environment file loaded: ",load_dotenv(envfile_path_string))


########################################
#write dataframe to file
def writeDfToCsv(df,
                 filename=f'file.xlsx',
                 folderpath='',
                 is_testing_run=False):
    import os
    
    #testing
    if is_testing_run == True:
        import pandas as pd
        df = pd.DataFrame(data=[{1,2,3},{4,5,6}],columns=['a','b','c'])
    else: next       
    
    #final file path
    path_and_file = os.path.join(os.getcwd(),folderpath, filename)

    #readout for user
    print(f"Response (type: {type(df)} saved to {folderpath}'/'{filename}.")
    
    #create/overwrite df to file
    with open(path_and_file,"w") as file:
        df.to_csv(path_and_file, sep=',', encoding='utf-8', index=False)
        file.close
    #EOF


##########################################
#Writes dataframe to specified bucket/path
def write_df_to_gcs(df, 
                    bucket_name = 'your_bucket_name', 
                    gcs_bucket_filepath = 'your/buckjet/filepath.csv', 
                    is_testing_run=False):
    
    import pandas as pd
    from google.cloud import storage
    import io
    from datetime import datetime

    if is_testing_run == True:
        #simple dataframe
        df = pd.DataFrame(data=[[1,2,3],[4,5,6]],columns=['a','b','c'])

    # get the bucket that the file will be uploaded to.
    storage_client = storage.Client()
    bucket_object = storage_client.get_bucket(bucket_name)

    # Create a new blob and upload the file's content.
    fileblob_object = bucket_object.blob(gcs_bucket_filepath)

    # create/open and then write df to file object
    file_object = io.StringIO()
    df.to_csv(file_object, index=False)
    file_object.seek(0) #move to beginning for using read() below

    # upload from string and close file
    fileblob_object.upload_from_string(file_object.read(), content_type="text/csv")
    file_object.close()

    #error checking
    if 'a' != 'a':
        message = ''
    else:
        message = f"Wrote to {bucket_name} at location {gcs_bucket_filepath}" 
    
    return(message)
    #EOF


########################################
#Reads GCS blob and writes to local file
def get_config_from_gcs_xlsx(bucket_name = 'your_bucket_name', 
                             gcs_bucket_blobpath = 'your/bucket/blobpath.xlsx', 
                             is_testing_run=False):
    
    from google.cloud import storage
    import io
    import pandas as pd

    # get the bucket that the file will be uploaded to.
    storage_client = storage.Client()
    bucket_object = storage_client.get_bucket(bucket_name)

    # Create a new local blob and download file to local directory
    fileblob_object = bucket_object.blob(gcs_bucket_blobpath)
    fileblob_object.download_to_filename('config_user-details.xlsx')
    
    #read xlsx for return
    df = pd.read_excel('config_user-details.xlsx')
    
    if 'a' != 'a':
        message = ''
    else:
        message = f"Retrieved {bucket_name} at location {gcs_bucket_blobpath} to dataframe" 
    
    print(message)

    return(df)
    #EOF


########################################
#TODO: upload updated config located in root directory (move to different location?)
def upload_config_to_gcs_xlsx(bucket_name='your_bucket_name',
                              gcs_bucket_blobpath='your/bucket/blobpath.xlsx',
                              is_testing_run=False):
    from google.cloud import storage 
    storage_client = storage.Client()
    bucket_object = storage_client.get_bucket(bucket_name)
    print('TODO: incomplete, currently uploading manually')
    return()
    #EoF


########################################
# list all files/directories/blobs
def list_gcs_blobs(bucket_name = 'rainday-gameday-bucket'):
    
    from google.cloud import storage
    
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name)
    blobs_list = list(blobs)
    
    #List blobs in bucket
    # for blob in blobs_list:
    #     print(blob.name)
    print('func list_gcs_blobs: finished')
    return blobs_list


#######################################
# Union blobs 
def union_gcs_csv_blobs(blobs_list=None, 
                        csvs_to_union_folder_location=''):

    import pandas as pd
    from google.cloud import storage
    import io
    dfs = []
    csv_content = io.StringIO()
    
    for blob in blobs_list:
        if blob.name.startswith(csvs_to_union_folder_location) and blob.name.endswith('.csv') and not blob.name.endswith('/'):

            #write blob string content to StringIO object 
            csv_content.write(blob.download_as_text())
            csv_content.seek(0)

            #Get it into a df
            df = pd.read_csv(csv_content)

            #append to df list
            dfs.append(df)

    # Union all DataFrames
    unioned_dfs = pd.concat(dfs)

    print('func union_gcs_csv_blobs: finished')
    return unioned_dfs


#########################################
#Function to get the forecast from openweatherAPI
def getWeatherForecast(openweathermap_api_key=None,
                       user_name=None, lat=None, lon=None,
                       is_testing_run=False,
                       write_to_directory=False,
                       log_responses_directory='log/responses',
                       log_response_file_name='response'):
    """Takes n parameters and outputs a weather forec,
    ast"""
    import os 
    import requests
    import json
    
    #Get the key and set test values if is_Testing_run is set to True
    #is_testing_run=True
    if is_testing_run == True:
        OPENWEATHERMAP_API_KEY = 'sldkfj'
        lat = float('42.98')
        lon = float('-81.24')
        user_name = 'testuser'
        write_to_directory=False
        log_responses_directory = 'log/responses'
        log_response_file_name = 'response'
        print('LOG getWeatherForecast(): is testing run:', is_testing_run, '.  Using testing API KEY and lon/lat values')
    else: 
        # Get open weather api key
        OPENWEATHERMAP_API_KEY = openweathermap_api_key
        print('LOG getWeatherForecast(): is testing run:', is_testing_run,".  Getting lon/lat/user values")

    #get request
    getrequest = requests.get(f'https://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&units=metric&appid={OPENWEATHERMAP_API_KEY}')
    print(f'LOG: GOT REQUEST WITH\n --RESPONSE CODE: {getrequest.status_code}')

    #save response to json file `OR retieve a historic response for use
    if getrequest.status_code == 200:
        
        #add request to variable
        json_user_forecast = getrequest.json()

        if write_to_directory == True:
            # Save the getrequest content to a local file
            with open(os.path.join(os.getcwd(),log_responses_directory,log_response_file_name+'_'+str(user_name)+'.json'),
                    "w") as file:
                json.dump(json_user_forecast, file)
                file.close
                
            print(f"--Response (type: {type(json_user_forecast)} saved to {log_response_file_name}_{str(user_name)}")
    else:
        #get the most recent (or whatever) response file in log_responses_directory
        print(f"--LOG: Request failed with status code: {getrequest.status_code}, " \
                "Loading instead a sample json response from internals")

        #iterate over log responses directory
        for file in os.listdir(log_responses_directory):
            filename = os.fsdecode(file)
            if filename.endswith(".json"): 
                #open json file
                with open(os.path.join(log_responses_directory,filename),
                            "r") as file:
                    json_user_forecast = json.load(file)
                    print(type(json_user_forecast))
                    file.close
                break
            else: 
                #add request to variable
                json_user_forecast = {} 
                continue 
    
    #add usersname to json response data in local object
    json_user_forecast['user_name'] = user_name

    print(f"--return object for {user_name} is of type: {type(json_user_forecast)}")

    #return json_user_forecast in the form of a dictionary
    return json_user_forecast
    #EOF


#########################################
#Function to take a request object's JSON data in the form of a dictionary
def transformJsonForecast(json_user_forecast,
                          is_testing_run=True):
    """
    returns df, a dataframe. Takes a openweathermap request object's JSON data in the form of a 
    dictionary and moulds the data into a dataframe for comparing/working 
    with contained forecast data
    """
    from datetime import date, datetime
    import pandas as pd
    
    #set/declare variables/objects
    forecast_schema = ['forecast_capture_date', 'forecast_dateunix', 
                       'forecast_datestring', 'name', 'rain_category', 
                       'rain_category_value',  ]
    forecast_weather_username = []
    forecast_wthr_capture_date  = []
    forecast_datestrings = []
    forecast_datesunix = []
    forecast_wthr_categories = [] #will determien =rain or !=rain
    forecast_wthr_categories_values = []

    # Access specific data from the JSON
    json_user_forecast_list = json_user_forecast['list']

    #json_user_forecast_list now contains multiple forecasts. Review each and capture the
    # date/rain status (=rain or !=rain) in lists for building a dataframe
    for i, singleuserssingleforecast in enumerate(json_user_forecast_list):
        
        #get date fields
        forecast_dateunix = json_user_forecast_list[i]['dt']
        forecast_datestring = datetime.utcfromtimestamp(forecast_dateunix).strftime('%Y-%m-%d %H:%M:%S')
        forecast_wthr_capture_date.append(datetime.utcfromtimestamp(forecast_dateunix).strftime('%Y-%m-%d'))
        forecast_datestrings.append(forecast_datestring)
        forecast_datesunix.append(forecast_dateunix)

        #get name
        user_name = json_user_forecast['user_name']
        forecast_weather_username.append(user_name)
        
        #get weather category and category value (1 = rain, 0 = no rain)
        if 'rain' in json_user_forecast_list[i]["weather"][0]['main'].lower():
            forecast_wthr_categories.append('rain')
            forecast_wthr_categories_values.append('1')
        else:
            forecast_wthr_categories_values.append('0')
            forecast_wthr_categories.append('no rain')

    # TODO this section seems outside of normal practice
    # Add columns/arrays/series' to df.  TODO this will eventually be looped over 
    # each of the usernames contained in the yaml config file
    #https://stackoverflow.com/questions/30522724/take-multiple-lists-into-dataframe
    zipped_lists = zip(forecast_wthr_capture_date, forecast_datesunix, 
                       forecast_datestrings, forecast_weather_username, 
                       forecast_wthr_categories, forecast_wthr_categories_values)
    list_of_zipped_lists = list(zipped_lists)
    
    forecast_df = pd.DataFrame(list_of_zipped_lists, columns = forecast_schema)
    # removed 12:05pm 2023-06-12 #list_of_forecast_dfs.append(forecast_df)

    ####################################################################
    if is_testing_run == True:
        #NOTE/TESTING THIS GENERATES A SECOND DF TO THE LIST AND IS FOR TESTING ONLY
        df2 = forecast_df.replace({'eric':'john'}, inplace=False)
        forecast_df = [forecast_df,df2]
        print('LOG transformJsonForecast(): is testing run:', is_testing_run)
    else: 
        print('LOG transformJsonForecast(): is testing run:', is_testing_run, '. Returning list_of_forecast_dfs')
    ####################################################################

    print(f"--return object for {user_name} is of type: {type(forecast_df)}")

    return forecast_df
    #EOF


#########################################
#Function to take a request object's JSON data in the form of a dictionary
def transformJsonForecast_table(json_user_forecast,
                                is_testing_run=True):
    """
    returns df, a dataframe. Takes a openweathermap request object's JSON data in the form of a 
    dictionary and moulds the data into a dataframe for comparing/working 
    with contained forecast data
    """
    from datetime import date, datetime
    import pandas as pd
    
    forecast_schema = ['forecast_capture_date', 'forecast_dateunix', 'forecast_datestring', 'name', 
                        'rain_category', 'rain_category_value', 'temp', 
                        'temp_min', 'temp_max', 'temp_humidity', 
                        'weather class', 'weather description' ]
    forecast_capture_date = []
    forecast_weather_username = []
    forecast_datestrings = []
    forecast_datesunix = []
    forecast_wthr_categories = [] #will determien =rain or !=rain
    forecast_wthr_categories_values = []
    forecast_temp = []
    forecast_temp_min = []
    forecast_temp_max = []
    forecast_temp_humidity = []
    forecast_wthr_main = []
    forecast_wthr_description = []

    # Access specific data from the JSON
    json_user_forecast_list = json_user_forecast['list']

    #json_user_forecast_list now contains multiple forecasts. Review each and capture the
    # date/rain status (=rain or !=rain) in lists for building a dataframe
    for i, singleuserssingleforecast in enumerate(json_user_forecast_list):
        
        #get date fields
        forecast_capture_date.append(datetime.today().strftime('%Y-%m-%d'))
        forecast_dateunix = json_user_forecast_list[i]['dt']
        forecast_datestring = datetime.utcfromtimestamp(forecast_dateunix).strftime('%Y-%m-%d %H:%M:%S')
        forecast_datestrings.append(forecast_datestring)
        forecast_datesunix.append(forecast_dateunix)

        #get name
        user_name = json_user_forecast['user_name']
        forecast_weather_username.append(user_name)
        
        #get other weather details
        forecast_temp.append(json_user_forecast_list[i]['main']['temp'])
        forecast_temp_min.append(json_user_forecast_list[i]['main']['temp_min'])
        forecast_temp_max.append(json_user_forecast_list[i]['main']['temp_max'])
        forecast_temp_humidity.append(json_user_forecast_list[i]['main']['humidity'])
        forecast_wthr_main.append(json_user_forecast_list[i]["weather"][0]['main'].lower())
        forecast_wthr_description.append(json_user_forecast_list[i]["weather"][0]['description'].lower())

        #get weather category and category value (1 = rain, 0 = no rain)
        if 'rain' in json_user_forecast_list[i]["weather"][0]['main'].lower():
            forecast_wthr_categories.append('rain')
            forecast_wthr_categories_values.append('1')
        else:
            forecast_wthr_categories_values.append('0')
            forecast_wthr_categories.append('no rain')

    # TODO this section seems outside of normal practice
    # Add columns/arrays/series' to df.  TODO this will eventually be looped over 
    # each of the usernames contained in the yaml config file
    #https://stackoverflow.com/questions/30522724/take-multiple-lists-into-dataframe
    zipped_lists = zip(forecast_capture_date,
                       forecast_datesunix, 
                       forecast_datestrings, 
                       forecast_weather_username, 
                       forecast_wthr_categories, 
                       forecast_wthr_categories_values,
                       forecast_temp,
                       forecast_temp_min,
                       forecast_temp_max,
                       forecast_temp_humidity,
                       forecast_wthr_main,
                       forecast_wthr_description)
    
    list_of_zipped_lists = list(zipped_lists)
    
    forecast_df = pd.DataFrame(list_of_zipped_lists, columns = forecast_schema)
    # removed 12:05pm 2023-06-12 #list_of_forecast_dfs.append(forecast_df)

    #NOTE: Error CHecking
    if is_testing_run == True:
        #NOTE/TESTING THIS GENERATES A SECOND DF TO THE LIST AND IS FOR TESTING ONLY
        df2 = forecast_df.replace({'eric':'john'}, inplace=False)
        forecast_df = [forecast_df,df2]
        print('LOG transformJsonForecast(): is testing run:', is_testing_run)
    else: 
        print('LOG transformJsonForecast(): is testing run:', is_testing_run, '. Returning list_of_forecast_dfs')

    print(f"--return object for {user_name} is of type: {type(forecast_df)}")

    return forecast_df
    #EOF


#########################################
#2. Use pandas to group by date and name and location and then take the
# max of "forecast_weather_category_values" by DAY
def compareWeatherResults(list_of_all_forecast_dfs, 
                          is_testing_run=True):
    
    import pandas as pd

    #NOTE/QUESTION How best to loop through a list of dfs and merge on each 
    for i, forecast_df in enumerate(list_of_all_forecast_dfs):

        ################
        if is_testing_run == True:
            i=0 #NOTE/TESTING THIS IS FOR TESTING ONLY
            print('LOG compareWeatherResults(): is testing run:', is_testing_run)
        else:
            print('LOG compareWeatherResults(): is testing run:', is_testing_run)
        ################

        #Slices he list of dataframes to get just the nth dataframe and then:
        # LOOP repeat until done list of dataframes captured from openweathermap
        #  based on the yaml config files' lon/lat coordinates  
        # - if 1st run, creates a dataframe from the 1st in the list of dfs the function is called with 
        # - if nth run, merges the nth dataframe from the list with the previous iterations version 
        #    of forecastsandweathercat_df on rain_category, forecast_datestring columns
        # - filters result to only the rain_category, forecast_datestring columns
        # - the resulting dataframe contains only forecast_datestrings where there is rain for all parties
        if i == 0:
            forecastsandweathercat_df = pd.DataFrame()
            forecastsandweathercat_df = list_of_all_forecast_dfs[i]
            forecastsandweathercat_df = forecastsandweathercat_df.loc[: ,['rain_category','forecast_datestring']]
        else:
            forecast_df=list_of_all_forecast_dfs[i]
            forecast_df = forecast_df.loc[: ,['rain_category','forecast_datestring']]
            forecastsandweathercat_df = pd.merge(forecastsandweathercat_df, forecast_df,
                                                 on = ['rain_category', 'forecast_datestring'])
            #TODO add a step which filters the erge to 
            forecastsandweathercat_df =  forecastsandweathercat_df.loc[(forecastsandweathercat_df['rain_category'] == 'rain'),:]

    ###################
    #ALTERNATIVE METHOD
    #TODO concatenate all dfs from df list into a single table for use with pandas
    #df = pd.concat(list_of_all_forecast_dfs)
    #print(df)
    
    #TODO Pivot data to make easy for comparison (finding all 1s)
    #3. Compare values for each day to see if there is overlap
    ###################

    #TODO/FEATURE output should look like a  list of days/times/users that can meet
    #   -time    -[users] -reason
    #   2023-06-09 00:00    [eric, crube, nano] 'rain'
    #   2023-06-09 04:00    [crube, oath, prag] 'cuz its saturday'
    
    return forecastsandweathercat_df
    #EOF

############## UTILITY FUNCS ##############

def write_to_csv_and_xlsx(df, filename='df'):
    import pandas as pd
    import os

    df.to_csv(f"{filename}.csv", index=False)
    df.to_excel(f"{filename}.xlsx", index=False)
    
    return print(f"function write_to_csv_and_xlsx: finsihed\n  -Wrote to: {os.getcwd()}")
    #EoF
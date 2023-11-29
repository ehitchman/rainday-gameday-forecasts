import os
import requests
import json
from datetime import datetime
import pandas as pd

from classes.ConfigManagerClass import ConfigManager
from classes.LoggingClass import LoggingManager

runtime_logger_level = 'DEBUG'

class OpenWeatherMapForecasts():

    def __init__(self):

        #load environment variables and config.yml
        self.config = ConfigManager(yaml_filepath='config', yaml_filename='config.yaml')

        #logger
        self.logging_manager = LoggingManager() 
        self.logger = self.logging_manager.create_logger(
            logger_name='log_OpenWeatherMap',
            debug_level=runtime_logger_level,
            stream_logs=True,
            mode='a'
        )

    #Function to get the forecast from openweatherAPI
    def getWeatherForecast(
            self,
            user_name=None, 
            lat=None, 
            lon=None,
            write_to_directory=False
            ):
        """Takes n parameters and outputs a weather forecast"""
        self.logger.info(f'getWeatherForecast() is running')

        # Get open weather api key
        getrequest = requests.get(f'https://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&units=metric&appid={self.config.openweathermap_api_key}')
        
        message = ' - Using .env API KEY, getting lon/lat/user values'
        self.logger.info((f'{message}\n'
            f' Got response (code: {getrequest.status_code})'))

        #save response to json file `OR retieve a historic response for use
        if getrequest.status_code == 200:
            
            #add request to variable
            json_user_forecast = getrequest.json()

            if write_to_directory == True:
                # Save the getrequest content to a local file
                with open(os.path.join(
                    self.config.log_responses_directory,
                    self.config.response_file_name+'_'+str(user_name)+'.json'),
                    "w"
                    ) as file:
                    json.dump(json_user_forecast, file)
                    file.close              
                self.logger.info(f" -Response type: {type(json_user_forecast)} (saved to {self.config.response_file_name}_{str(user_name)})")
        else:
            #get the most recent (or whatever) response file in self.config.log_responses_directory
            self.logger.error((f" - Request failed, loading a sample json response from internals"))

            #iterate over log responses directory
            for file in os.listdir(self.config.log_responses_directory):
                filename = os.fsdecode(file)
                if filename.endswith(".json"): 
                    with open(os.path.join(self.config.log_responses_directory, filename),
                                "r") as file:
                        json_user_forecast = json.load(file)
                        self.logger.info(type(json_user_forecast))
                        file.close
                    break
                else: 
                    #add request to variable
                    json_user_forecast = {} 
                    continue 
        
        #add usersname to json response data in local object
        json_user_forecast['user_name'] = user_name

        self.logger.info(f" -getWeatherForecast() finished, return object for {user_name} is of type: {type(json_user_forecast)}")

        #return json_user_forecast in the form of a dictionary
        return json_user_forecast
        #EOF

    #Function to take a request object's JSON data in the form of a dictionary
    def transformJsonForecast(self,
                              json_user_forecast):
        """
        returns df, a dataframe. Takes a openweathermap request object's JSON data in the form of a 
        dictionary and moulds the data into a dataframe for comparing/working 
        with contained forecast data
        """

        forecast_schema = [
            'capture_date', 
            'forecast_dateunix', 
            'forecast_datetime',
            'name', 
            'rain_category', 
            'rain_category_value', 
            'temp', 
            'temp_min', 
            'temp_max', 
            'temp_humidity', 
            'weather_description'
           ]
        
        capture_dates  = []
        forecast_datesunix = []
        forecast_datestime = []
        forecast_usernames = []
        forecast_wthr_rain_categories = []
        forecast_wthr_rain_category_values = []
        forecast_wthr_temps = []
        forecast_wthr_temps_min = []
        forecast_wthr_temps_max = []
        forecast_wthr_temps_humidity = []
        forecast_wthr_weather_descriptions = []

        self.logger.info("-------------------------")
        self.logger.info("-------------------------")
        self.logger.info("-------------------------")
        self.logger.info("")
        # Access specific data from the JSON
        json_user_forecast_list = json_user_forecast['list']

        #json_user_forecast_list now contains multiple forecasts. Review each and capture the
        # date/rain status (=rain or !=rain) in lists for building a dataframe
        self.logger.info(f'getWeatherForecast() is running')
        for i, singleuserssingleforecast in enumerate(json_user_forecast_list):

            #get date fields
            todays_date = datetime.today().strftime('%Y-%m-%d')
            capture_date= todays_date
            capture_dates.append(capture_date)

            forecast_dateunix = json_user_forecast_list[i]['dt']
            forecast_datesunix.append(forecast_dateunix)

            forecast_datetime = datetime.utcfromtimestamp(forecast_dateunix).strftime('%Y-%m-%d %H:%M:%S')
            forecast_datestime.append(forecast_datetime)

            forecast_username = json_user_forecast['user_name']
            forecast_usernames.append(forecast_username)

            #get weather category and category value (1 = rain, 0 = no rain)
            if 'rain' in json_user_forecast_list[i]["weather"][0]['main'].lower():
                forecast_wthr_rain_categories.append('rain')
                forecast_wthr_rain_category_values.append('1')
            else:
                forecast_wthr_rain_categories.append('no rain')
                forecast_wthr_rain_category_values.append('0')

            # Extract main weather details
            main_weather = singleuserssingleforecast['main']
            forecast_wthr_temps.append(main_weather['temp'])
            forecast_wthr_temps_min.append(main_weather['temp_min'])
            forecast_wthr_temps_max.append(main_weather['temp_max'])
            forecast_wthr_temps_humidity.append(main_weather['humidity'])

            # Extract weather description (assuming only one weather condition per entry)
            weather_description = singleuserssingleforecast['weather'][0]['description']
            forecast_wthr_weather_descriptions.append(weather_description)

        # TODO this section seems outside of normal practice
        # Add columns/arrays/series' to df.  TODO this will eventually be looped over 
        # each of the usernames contained in the yaml config file
        #https://stackoverflow.com/questions/30522724/take-multiple-lists-into-dataframe
        self.logger.info("ZIPPING LISTS NOW..................")
        zipped_lists = zip(
            capture_dates, 
            forecast_datesunix, 
            forecast_datestime, 
            forecast_usernames, 
            forecast_wthr_rain_categories, 
            forecast_wthr_rain_category_values,
            forecast_wthr_temps,
            forecast_wthr_temps_min,
            forecast_wthr_temps_max,
            forecast_wthr_temps_humidity,
            forecast_wthr_weather_descriptions
            )
        
        list_of_zipped_lists = list(zipped_lists)

        forecast_df = pd.DataFrame(list_of_zipped_lists, columns=forecast_schema)

        return forecast_df

    # #Use pandas to group by date and name and location and then take the max of 
    # # "forecast_weather_category_values" by DAY
    # def compareWeatherResults(
    #         self,
    #         list_of_all_forecast_dfs, 
    #         is_testing_run=True
    #         ):
        
    #     import pandas as pd

    #     #NOTE/QUESTION How best to loop through a list of dfs and merge on each 
    #     for i, forecast_df in enumerate(list_of_all_forecast_dfs):

    #         ################
    #         if is_testing_run == True:
    #             i=0 #NOTE/TESTING THIS IS FOR TESTING ONLY
    #             self.logger.info('compareWeatherResults() is running, test run:', is_testing_run)
    #         else:
    #             self.logger.info('compareWeatherResults() is running, test run:', is_testing_run)
    #         ################

    #         #Slices he list of dataframes to get just the nth dataframe and then:
    #         # LOOP repeat until done list of dataframes captured from openweathermap
    #         #  based on the yaml config files' lon/lat coordinates  
    #         # - if 1st run, creates a dataframe from the 1st in the list of dfs the function is called with 
    #         # - if nth run, merges the nth dataframe from the list with the previous iterations version 
    #         #    of forecastsandweathercat_df on rain_category, forecast_datestring columns
    #         # - filters result to only the rain_category, forecast_datestring columns
    #         # - the resulting dataframe contains only forecast_datestrings where there is rain for all parties
    #         if i == 0:
    #             forecastsandweathercat_df = pd.DataFrame()
    #             forecastsandweathercat_df = list_of_all_forecast_dfs[i]
    #             forecastsandweathercat_df = forecastsandweathercat_df.loc[: ,['rain_category','forecast_datestring']]
    #         else:
    #             forecast_df=list_of_all_forecast_dfs[i]
    #             forecast_df = forecast_df.loc[: ,['rain_category','forecast_datestring']]
    #             forecastsandweathercat_df = pd.merge(forecastsandweathercat_df, forecast_df,
    #                                                 on = ['rain_category', 'forecast_datestring'])
    #             #TODO add a step which filters the erge to 
    #             forecastsandweathercat_df =  forecastsandweathercat_df.loc[(forecastsandweathercat_df['rain_category'] == 'rain'),:]

    #     ###################
    #     #ALTERNATIVE METHOD
    #     #TODO concatenate all dfs from df list into a single table for use with pandas
    #     #df = pd.concat(list_of_all_forecast_dfs)
    #     #self.logger.info(df)
        
    #     #TODO Pivot data to make easy for comparison (finding all 1s)
    #     #3. Compare values for each day to see if there is overlap
    #     ###################

    #     #TODO/FEATURE output should look like a  list of days/times/users that can meet
    #     #   -time    -[users] -reason
    #     #   2023-06-09 00:00    [eric, crube, nano] 'rain'
    #     #   2023-06-09 04:00    [crube, oath, prag] 'cuz its saturday'
        
    #     self.logger.info(f' - compareWeatherResults() finished, return object is of type: {type(forecastsandweathercat_df)}"')
    #     return forecastsandweathercat_df
    #     #EOF

    # #Function gathers a ditionary for use in bot.py.  Uses getWeatherForecast, 
    # # transformJsonForecast, transformJsonForecast_table, compareWeatherResults 
    # def getRaindayGamedayOptions(self):

    #     ############################################################
    #     #TODO change envfile_path_string in config.yml
    #     # - Update the uri for the env path.  Create a template env file, etc.
    #     # - End goal is to make script easily usable by others
        

    #     list_of_forecast_dfs = []
    #     list_of_all_forecast_dfs = []
    #     list_of_all_users_names = []
    #     dict_returned_objects = {}

    #     for i, user in enumerate(self.config.users_details):
            
    #         #get users_detail list and details
    #         user_details = self.config.users_details[i]
    #         user_name = user_details['name']
    #         user_lon = user_details['lon']
    #         user_lat = user_details['lat']
    #         self.logger.info("Getting forecast for", user_name)

    #         #NOTE does it make sense to be calling a function directly inside of a 
    #         # function?  Is this a sign of poor design?
    #         json_forecast = self.getWeatherForecast(
    #             user_name = user_name,
    #             lon = user_lon,
    #             lat = user_lat,
    #             write_to_directory=True,
    #             i = i)
            
    #         self.logger.info(f'Finished call to getWeatherForecast for {user_name}')

    #         #Run transformJsonForecast
    #         list_of_forecast_dfs = self.transformJsonForecast(json_user_forecast = json_forecast)
    #         #self.logger.info("list_of_forecast_dfs is of type:", type(list_of_forecast_dfs))
    #         self.logger.info(f'Finished call to transformJsonForecast for {user_name}')

    #         #append df to new all forecasts list item and users name list item
    #         list_of_all_forecast_dfs.append(list_of_forecast_dfs)
    #         list_of_all_users_names.append(user_name)

    #     #self.logger.info logs
    #     self.logger.info("returned list_of_all_forecast_dfs of type", type(list_of_all_forecast_dfs), 'and length', len(list_of_all_forecast_dfs))

    #     #TODO concatenate all dfs from df list into a single table for use with pandas
    #     raindaygameday = self.compareWeatherResults(list_of_all_forecast_dfs = list_of_all_forecast_dfs,
    #                                         is_testing_run=False)
    #     self.logger.info(f'Finished call to compareWeatherResults for {user_name}')
    #     raindaygameday = raindaygameday['forecast_datestring']

    #     #add objects to return dictionary
    #     dict_returned_objects['dates_and_times_df'] = raindaygameday
    #     dict_returned_objects['users_names_list'] = list_of_all_users_names

    #     #return dictionary
    #     self.logger.info('EoF: return object is of type:',type(dict_returned_objects))
    #     return dict_returned_objects

if __name__ =="__main__":
    owm = OpenWeatherMapForecasts()
    config = ConfigManager(yaml_filepath='config', yaml_filename='config.yaml')
    os.path.join(config.log_responses_directory, config.response_file_name+'_.json')
    print(f"Testing retrieval of ConfigClass variable. config.env_filename:{owm.config.env_filename}")
    output = owm.getWeatherForecast(
        lat=43.65,
        lon=-79.38,
        write_to_directory=True
    )
    print(output)
    df = owm.transformJsonForecast(output)
    print(df)

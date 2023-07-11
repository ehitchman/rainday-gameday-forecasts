# rainday-gameday-forecasts
Attached to the cloud function rainday-gameday_getuserforecasts, this is similar to rainday-gameday but stores a daily 5-day forecast for each user in the config file to a separate gcs blob

## Adding your keys: 
### NOTE: You must update the config file with placeholders indicated in CAPS_CASE

### Open Weather Map
#### 1. create an account and generate your own API key from openWeatherMap 
#### 2. store the key in the respective variable in the env file
#### 3. SAVE the env.template.py file as env.py in the root directory

## Adding the GPS coordinates to the config file
### NOTE: GPS cooridnates must be set in the config file.  
#### 1. Open config.yaml and add any number of entries to the `users_details` item
##### EXAMPLE: see config.yaml

##### TODO: Add a more user-friendly GPS lookup to fetch forecasts.  The below API comes recommended from Open Weather Map
###### https://openweathermap.org/api/geocoding-api#reverse
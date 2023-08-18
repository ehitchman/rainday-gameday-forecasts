# Rainday Gameday!
## Adding your keys: 
### NOTE: You must create your own environment file from the env.template.py template included

### Open Weather Map
#### 1. create an account and generate your own API key from openWeatherMap 
#### 2. store the key in the respective variable in the env file
#### 3. SAVE the env.template.py file as env.py in the root directory

### NOTE: You need to create your own bot and capture the respective appid/botid (and do some minor settings configuration) 
#### 1. A tutorial like this should be all that's needed: https://pythoninoffice.com/building-a-simple-python-discord-bot-with-discordpy-in-2022-2023
#### 2. store the key in the respective variable in the env file
#### 3. if you did not already do this after adding the Open Weather API key to the env file, SAVE the env.template.py file as env.py in the root directory

## Adding the GPS coordinates to the config file
### NOTE: GPS cooridnates must be set in the config file.  
#### 1. Open config.yaml and add any number of entries to the `users_details` item
##### EXAMPLE: see config.yaml

##### TODO: Add arguments to the bot function call !raindaygameday so that it can accept a list of GPS cooridnates
###### - This will have to end up taking the form of a dictionary similar to the structure of config.yaml

##### TODO: Add a more user-friendly GPS lookup to fetch forecasts.  The below API comes recommended from Open Weather Map
###### https://openweathermap.org/api/geocoding-api#reverse

## Notes for how to get the VM setup
### Note: Key is to add get the key generated and added to yoru github account (if private repo.)  This will allow you to clone the repo to your VM once set up
1. Create google VM instance
    - default values seemed to work fine

2. Open VMs SSH-in-browser
    a. create an ssh key
        $~ssh-keygen
    b. commit the ssh key and change permissions
        $~eval "$(ssh-agent -s)"
            - ssh-agent is called and -s captures commands required to _______
            - eval is a keyword used to execute what is inside of "#()"
        $~ssh-add ~/.ssh/id_rsa
            -  ssh-add used to 'commit' the id_rsa private key
        $~chmod 700 ~/.ssh
        $~chmod 600 ~/.ssh/id_rsa
            - Unsure exactly why two of these are needed.  Need to investigate the permission codes deeper

    result: Should echo that permissions have been added

3. Add id_rsa.pub (you can view using linux $cat from the vm) to github account ssh key settings

4. Run requirements.txt in vm
    - NOTE: Ran into some issues with this as 'pip freeze > requirements.txt' didn't seem to capture all packages (notably missing like yaml, dotenv, numpy, pandas)               

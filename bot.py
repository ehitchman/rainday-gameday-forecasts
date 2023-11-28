#URL for bot sharing.  Includes bot scope and read messages  permissions
#https://discord.com/api/oauth2/authorize?client_id=1117856119625371839&permissions=67584&scope=bot

#external imports
import os

#bot specific imports
import discord
from discord.ext import commands

from classes.ConfigManagerClass import ConfigManager

config = ConfigManager(yaml_filepath='config', yaml_filename='config.yaml')

#load_yaml() 
from modules import load_yaml, load_env

#load_envrionment()
load_env(env_filename=config.env_filename, 
         env_dirname=config.env_filedir)
DISCORD_BOT_TOKEN=os.getenv('DISCORD_BOT_TOKEN')

# Create the Discord client and add message content intents
# NOTE: sometimes tutorials use intents "all" instead of "default"
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix='!', intents=intents)

@bot.command()
async def raindaygameday(ctx):

    # #import and main python module and functions
    from modules import getRaindayGamedayOptions

    # returns dictionary of n objects
    #  1. 'dates_and_times_df -- data frame of available times for gaming
    #  2. 'users_names_list' -- list of users included in the forecast comparison (from config.yml['users_details'] entry)
    finaldictionary = getRaindayGamedayOptions()

    #Get dates for use in message from bot in printer friendly concatenation of dates/times
    list_of_dates_and_times = finaldictionary['dates_and_times_df'] #df object in top level of dictionary
    type(list_of_dates_and_times)
    string_of_dates_and_times = ', '.join(list_of_dates_and_times)
    type(string_of_dates_and_times)

    #Get names for use in message from bot in printer friendly concatenation of names
    finallistofnamesforgaming = finaldictionary['users_names_list'] #list object in top level of dictionary
    type(finallistofnamesforgaming) 
    string_of_names = ', '.join(finallistofnamesforgaming)
    type(string_of_names)

    #build and send message to discord
    #TODO: Add GPS coordinates or corresponding locale name in message
    message = f'Lets make the next rainday a gameday!  Based on the rain forecast for {string_of_names}, here are our next raindaygameday options:\n{string_of_dates_and_times}'
    
    await ctx.send(message)

#run the client
bot.run(DISCORD_BOT_TOKEN)
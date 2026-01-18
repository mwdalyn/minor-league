'''
Created on: July 16, 2023 
Updated: July 22, 2025 (oops)
Goal: 
Collect data and logos for all minor league baseball teams documented at the following link
https://en.wikipedia.org/wiki/List_of_Minor_League_Baseball_leagues_and_teams
Store the data in a convenient way and retrieve. 
'''

# Imports
import datetime as dt
import os
from src.utils.html import cook_soup, set_user_agent
# Constants
SLEEP_TIME = 6 # seconds for sleep
TEAMS_LINK = "https://en.wikipedia.org/wiki/List_of_Minor_League_Baseball_leagues_and_teams"
USER_AGENT = set_user_agent(header_file=os.path.abspath(os.path.join(".","user-agent.txt")))

# Functions
def cook_teams_soup(header = USER_AGENT, output_file_path = os.path.join("data","html","milb")):
	# # Get team soup and parse (custom function, NOT universal)
	cook_soup(TEAMS_LINK, user_agent = header, html_file_path = output_file_path)

def cook_city_soup(city, state, header = USER_AGENT, output_file_path = os.path.join("data","html","city")):
	url = "https://en.wikipedia.org/wiki/" + city.replace(" ","_") + ",_" + state.replace(" ","_") 
	cook_soup(url, user_agent = header , html_file_path = output_file_path)



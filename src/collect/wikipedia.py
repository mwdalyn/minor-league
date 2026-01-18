'''
Created on: July 16, 2023 
Updated: July 22, 2025 (oops)
Goal: 
Collect data and logos for all minor league baseball teams documented at the following link
https://en.wikipedia.org/wiki/List_of_Minor_League_Baseball_leagues_and_teams
Store the data in a convenient way and retrieve. 
'''

# Imports
import requests 
import time
from bs4 import BeautifulSoup
import datetime as dt
import os

# Constants
SLEEP_TIME = 6 # seconds for sleep
TEAMS_LINK = "https://en.wikipedia.org/wiki/List_of_Minor_League_Baseball_leagues_and_teams"

# Functions
def set_ua_headers(ua_file_path='user-agent.txt'):
	wiki_user_headers = {}
	with open(ua_file_path, 'r') as f:
		for line in f:
			try:
				# Parsing the txt
				key, value = line.strip().split(':', 1)
				key, value = key.replace("'","").strip(), value.replace("'","").strip()
				wiki_user_headers[key] = value
			except ValueError:
				print(f"Skip: {line.strip()}")

def cook_soup(url, headers, output_html_path = None, parser = 'html.parser'):
	## Cook Soup
	try:
		response = requests.get(
		url = url,
		headers = headers)
		timestamp = dt.datetime.now().strftime(format="%Y%m%d_%H%M%S")
		response.raise_for_status()
		# Parse soup
		soup = BeautifulSoup(response.content, parser)
		# Archive as an html file with date name
		if output_html_path:
			url_page = "_".join(url.split("/")[-1].split("_")[0:4]).lower() # Grab first four phrases in the final segment of wiki url as unique ID
			with open(os.path.join(output_html_path, f"wiki_{url_page}_{str(timestamp)}.html"), "w", encoding='utf-8-sig') as file: 
				file.write(str(soup))
	except Exception as e:
		print(f'Error: {e}')
		soup = f'Error: {e}'
	return soup


# # if __name__ == "__main__":
# # Establish user agent
# wiki_user_headers = set_ua_headers()

# # Get team soup and parse (custom function, NOT universal)
# team_soup = cook_soup(TEAMS_LINK, wiki_user_headers, output_html_path=os.path.join("data","html","milb"))

# # Find City information and stats
# team_df["City"], team_df["State"] = team_df["City"].strip(), team_df["State"].strip()
# city_dfs, city_state_list = [], []
# for index, row in team_df.iterrows():
# 	city, state, city_state = row["City"], row["State"], row["City"] + ", " + row["State"]
# 	print(city+", "+state)
# 	if city_state not in city_state_list:
# 		url = "https://en.wikipedia.org/wiki/" + city.replace(" ","_") + ",_" + state.replace(" ","_") # NOTE: Need to make these more robust, sometimes (Franklin, WI) county is in the middle
# 		city_soup = cook_soup(url, wiki_user_headers, output_html_path=os.path.join("data","html","city"))
# 		time.sleep(SLEEP_TIME)
# 		df = read_city_soup(city_soup)
# 		if not isinstance(df, pd.DataFrame):
# 			print("\nContinue: Not a dataframe\n")
# 			continue
# 		df["city_name"] = city 
# 		city_dfs.append(df)
# 		city_state_list.append(city_state)
# 	else:
# 		print("Continue: Already downloaded.")
# 		continue

# # Concat
# main_df = pd.concat(city_dfs, ignore_index=True)
# # Export raw
# main_df.to_csv("main_city_infobox_df.csv") # NOTE: Can use other cols ('first settled', etc.) to supplement "founded"



